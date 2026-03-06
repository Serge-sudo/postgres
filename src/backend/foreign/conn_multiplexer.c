/*-------------------------------------------------------------------------
 *
 * conn_multiplexer.c
 *		Connection multiplexer for foreign data wrappers
 *
 * This module implements a worker-based connection multiplexer that routes
 * all foreign server connections through local and remote worker pools
 * instead of establishing direct connections.
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/foreign/conn_multiplexer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "access/xact.h"
#include "foreign/conn_multiplexer.h"
#include "funcapi.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"

/* Forward declarations for libpq functions (to avoid including libpq-fe.h with BUILDING_DLL) */
typedef struct pg_conn PGconn;
typedef struct pg_result PGresult;

typedef enum
{
	CONNECTION_OK,
	CONNECTION_BAD
} ConnStatusType;

typedef enum
{
	PGRES_EMPTY_QUERY = 0,
	PGRES_COMMAND_OK,
	PGRES_TUPLES_OK
} ExecStatusType;

extern PGconn *PQconnectdb(const char *conninfo);
extern void PQfinish(PGconn *conn);
extern ConnStatusType PQstatus(const PGconn *conn);
extern PGresult *PQexec(PGconn *conn, const char *command);
extern ExecStatusType PQresultStatus(const PGresult *res);
extern char *PQresultErrorMessage(const PGresult *res);
extern char *PQerrorMessage(const PGconn *conn);
extern void PQclear(PGresult *res);
extern char *PQcmdStatus(PGresult *res);
extern char *PQdb(const PGconn *conn);

/* GUC parameters - defined in globals.c */
extern int foreign_conn_multiplexer_workers;

/* Maximum number of workers supported */
#define MAX_CONN_MULTIPLEXER_WORKERS 64

/* Queue sizes - 512KB per queue fits within typical /dev/shm limits */
#define CONN_MUX_QUEUE_SIZE	(512 * 1024)  /* 512KB per queue */

/* Message types for worker communication */
typedef enum
{
	CONN_MUX_MSG_CONNECT,		/* establish connection request */
	CONN_MUX_MSG_QUERY,			/* execute query request */
	CONN_MUX_MSG_CLOSE,			/* close connection request */
	CONN_MUX_MSG_RESPONSE,		/* response from worker */
	CONN_MUX_MSG_ERROR			/* error from worker */
} ConnMuxMessageType;

/* Worker state phases for progress reporting */
typedef enum
{
	MUX_STATE_STARTING = 0,			/* Worker is starting up */
	MUX_STATE_IDLE,					/* Waiting for request */
	MUX_STATE_RECEIVING,			/* Receiving request from backend */
	MUX_STATE_CONNECTING,			/* Establishing foreign connection */
	MUX_STATE_EXECUTING,			/* Executing query via libpq */
	MUX_STATE_CLOSING,				/* Closing a connection */
	MUX_STATE_SENDING_RESPONSE,		/* Sending response to backend */
	MUX_STATE_STOPPED				/* Worker has stopped */
} ConnMuxWorkerPhase;

/* Shared memory per-worker progress information */
typedef struct ConnMuxWorkerProgress
{
	int			pid;				/* Worker PID (0 if not started) */
	ConnMuxWorkerPhase phase;		/* Current worker phase */
	ConnMuxMessageType current_request_type;	/* Type of request being processed */
	int			current_conn_id;	/* Connection ID of current request */
	int			requester_pid;		/* PID of backend that sent current request */
	int64		requests_completed;	/* Total requests processed */
	int64		connect_count;		/* Number of CONNECT requests handled */
	int64		query_count;		/* Number of QUERY requests handled */
	int64		close_count;		/* Number of CLOSE requests handled */
	int64		error_count;		/* Number of errors encountered */
	int			active_connections; /* Number of active connections in this worker */
	TimestampTz last_request_time;	/* When the current/last request started */
	TimestampTz worker_start_time;	/* When the worker started */
} ConnMuxWorkerProgress;

/* Shared memory structures */
typedef struct ConnMultiplexerWorkerQueues
{
	LWLock		queue_lock;			/* Serializes backend access to this worker's queues */
	bool		ready;				/* true when worker is ready for next request */
	ConnMuxWorkerProgress progress;	/* Progress tracking for this worker */
	shm_mq	   *request_queue;		/* Queue for requests to worker */
	shm_mq	   *response_queue;		/* Queue for responses from worker */
	char		request_queue_data[CONN_MUX_QUEUE_SIZE];	/* Request queue storage */
	char		response_queue_data[CONN_MUX_QUEUE_SIZE];	/* Response queue storage */
} ConnMultiplexerWorkerQueues;

typedef struct ConnMultiplexerShmemStruct
{
	LWLock		lock;
	int			num_workers;		/* number of active workers */
	int			next_worker;		/* round-robin worker selection */
	int			next_conn_id;		/* next connection ID to assign */
	bool		initialized;		/* true when workers are running */
	ConnMultiplexerWorkerQueues worker_queues[MAX_CONN_MULTIPLEXER_WORKERS]; /* Queue storage for each worker */
} ConnMultiplexerShmemStruct;

static ConnMultiplexerShmemStruct *ConnMultiplexerShmem = NULL;
static LWLock *ConnMultiplexerLock = NULL;

/* Worker connection state - stores actual libpq connections */
typedef struct WorkerConnection
{
	int			conn_id;		/* Unique connection identifier */
	void	   *pgconn;			/* PGconn pointer (void* for type safety in backend) */
	bool		in_use;			/* true if slot is allocated */
	char		conninfo[1024];	/* Connection string used to establish connection */
} WorkerConnection;

/* Worker state - stored in worker's memory context */
typedef struct WorkerState
{
	int				worker_id;
	WorkerConnection *connections;
} WorkerState;

#define MAX_WORKER_CONNECTIONS 100

/* Message header for worker communication */
typedef struct ConnMuxMessageHeader
{
	ConnMuxMessageType type;
	int			length;			/* total message length including header */
	int			conn_id;		/* connection identifier */
	int			request_id;		/* request identifier for matching */
} ConnMuxMessageHeader;

/* Complete message structure with payload */
typedef struct ConnMuxMessage
{
	ConnMuxMessageType type;
	int			length;			/* total message length including header */
	int			conn_id;		/* connection identifier */
	int			request_id;		/* request identifier for matching */
	char		data[FLEXIBLE_ARRAY_MEMBER];	/* variable length payload */
} ConnMuxMessage;

/* Forward declarations */
void conn_multiplexer_worker_main(Datum main_arg);
static void conn_multiplexer_shmem_startup(void);
static void conn_multiplexer_worker_sigterm(SIGNAL_ARGS);
static void conn_multiplexer_worker_sighup(SIGNAL_ARGS);
static void process_worker_requests(WorkerState *state);
static WorkerConnection *find_connection(WorkerConnection *connections, int conn_id);
static WorkerConnection *allocate_connection(WorkerConnection *connections);

/* Signal handlers */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

/*
 * Module initialization - called during postmaster startup
 * This is called AFTER shared memory is initialized.
 * 
 * Queues are now pre-allocated in shared memory (not DSM).
 * This avoids the issue with dsm_create() assertion in postmaster context.
 */
void
InitConnMultiplexer(void)
{
	
	if (!foreign_conn_multiplexer_workers)
		return;
	
	conn_multiplexer_shmem_startup();
	
	/* Set the number of workers if multiplexer is enabled */
	Assert (ConnMultiplexerShmem != NULL);
	
	{
		LWLockAcquire(ConnMultiplexerLock, LW_EXCLUSIVE);
		ConnMultiplexerShmem->num_workers = foreign_conn_multiplexer_workers;
		ConnMultiplexerShmem->initialized = true;
		LWLockRelease(ConnMultiplexerLock);

		ereport(LOG,
				(errmsg("connection multiplexer initialized with %d workers",
						foreign_conn_multiplexer_workers)));
	}
}

/*
 * Register background workers - called after GUC initialization
 */
void
RegisterConnMultiplexerWorkers(void)
{
	/* Register background workers if enabled */
	if (foreign_conn_multiplexer_workers > 0)
	{
		BackgroundWorker worker;
		int			i;

		/* Register background workers */
		for (i = 0; i < foreign_conn_multiplexer_workers; i++)
		{
			memset(&worker, 0, sizeof(BackgroundWorker));
			snprintf(worker.bgw_name, BGW_MAXLEN,
					 "conn_multiplexer worker %d", i);
			snprintf(worker.bgw_type, BGW_MAXLEN, "conn_multiplexer");
			worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
			worker.bgw_start_time = BgWorkerStart_PostmasterStart;
			worker.bgw_restart_time = 10;	/* restart after 10 seconds */
			snprintf(worker.bgw_library_name, sizeof(worker.bgw_library_name), "postgres");	/* built-in worker */
			snprintf(worker.bgw_function_name, sizeof(worker.bgw_function_name), "conn_multiplexer_worker_main");
			worker.bgw_main_arg = Int32GetDatum(i);
			worker.bgw_notify_pid = 0;

			RegisterBackgroundWorker(&worker);
		}
	}
}

/*
 * Calculate shared memory size needed
 */
Size
conn_multiplexer_shmem_size(void)
{
	Size		size = 0;
	
	if (!foreign_conn_multiplexer_workers)
		return 0;

	/* Base structure size includes all queue storage */
	size = add_size(size, sizeof(ConnMultiplexerShmemStruct));
	
	/* add size for the queues */
	size = add_size(size, foreign_conn_multiplexer_workers * (CONN_MUX_QUEUE_SIZE * 2)); /* request + response queues */
	
	return size;
}

/*
 * Shared memory startup hook
 */
static void
conn_multiplexer_shmem_startup(void)
{
	bool		found;
	int			i;

	/* Allocate shared memory */
	ConnMultiplexerShmem = ShmemInitStruct("conn_multiplexer",
										   sizeof(ConnMultiplexerShmemStruct),
										   &found);

	if (!found)
	{
		/* Initialize shared memory on first time */
		/* Get the LWLock from the named tranche we requested */
		ConnMultiplexerLock = &ConnMultiplexerShmem->lock;
		LWLockInitialize(ConnMultiplexerLock, LWTRANCHE_MULTIPLEXER);
		ConnMultiplexerShmem->num_workers = 0;
		ConnMultiplexerShmem->next_worker = 0;
		ConnMultiplexerShmem->next_conn_id = 1;
		ConnMultiplexerShmem->initialized = false;
		
		/* Initialize message queues and per-worker locks */
		for (i = 0; i < MAX_CONN_MULTIPLEXER_WORKERS; i++)
		{
			ConnMultiplexerWorkerQueues *wq = &ConnMultiplexerShmem->worker_queues[i];
			
			/* Initialize per-worker queue lock */
			LWLockInitialize(&wq->queue_lock, LWTRANCHE_MULTIPLEXER);
			wq->ready = false;

			/* Initialize progress tracking */
			memset(&wq->progress, 0, sizeof(ConnMuxWorkerProgress));
			wq->progress.phase = MUX_STATE_STOPPED;
			
			/* Create request queue in preallocated storage */
			wq->request_queue = shm_mq_create(wq->request_queue_data, CONN_MUX_QUEUE_SIZE);
			
			/* Create response queue in preallocated storage */
			wq->response_queue = shm_mq_create(wq->response_queue_data, CONN_MUX_QUEUE_SIZE);
		}
	}
}

/*
 * Find a connection by ID in worker's connection array
 * 
 * FULL IMPLEMENTATION: Searches through the connection slots to find
 * the one matching the given conn_id. Used by QUERY and CLOSE handlers.
 */
static WorkerConnection *
find_connection(WorkerConnection *connections, int conn_id)
{
	int i;
	
	for (i = 0; i < MAX_WORKER_CONNECTIONS; i++)
	{
		if (connections[i].in_use && connections[i].conn_id == conn_id)
			return &connections[i];
	}
	return NULL;
}

/*
 * Allocate a new connection slot
 * 
 * FULL IMPLEMENTATION: Finds an available slot in the connection array
 * and marks it as in_use. Called by CONNECT message handler.
 */
static WorkerConnection *
allocate_connection(WorkerConnection *connections)
{
	int i;
	
	for (i = 0; i < MAX_WORKER_CONNECTIONS; i++)
	{
		if (!connections[i].in_use)
		{
			connections[i].in_use = true;
			return &connections[i];
		}
	}
	return NULL;
}

/*
 * Count number of active (in_use) connections for a worker
 */
static int
count_active_connections(WorkerConnection *connections)
{
	int	i;
	int	count = 0;

	for (i = 0; i < MAX_WORKER_CONNECTIONS; i++)
	{
		if (connections[i].in_use)
			count++;
	}
	return count;
}

/*
 * Process a single request in worker.
 *
 * This function reinits the queues, sets up roles, attaches fresh handles,
 * does a blocking receive, processes the request, sends the response,
 * then releases handles. The queues are left in a clean state for the
 * next iteration.
 */
static void
process_worker_requests(WorkerState *state)
{
	shm_mq_result res;
	Size		msg_len;
	void	   *msg_data;
	ConnMuxMessage *msg;
	ConnMultiplexerWorkerQueues *wq;
	ConnMuxWorkerProgress *progress;
	shm_mq	   *request_mq;
	shm_mq	   *response_mq;
	shm_mq_handle *req_mqh;
	shm_mq_handle *resp_mqh;
	bool		had_error = false;

	wq = &ConnMultiplexerShmem->worker_queues[state->worker_id];
	progress = &wq->progress;
	request_mq = wq->request_queue;
	response_mq = wq->response_queue;

	/* Reinitialize queues for fresh use */
	shm_mq_reinit(request_mq);
	shm_mq_reinit(response_mq);

	/* Set our roles: worker is receiver on request, sender on response */
	shm_mq_set_receiver(request_mq, MyProc);
	shm_mq_set_sender(response_mq, MyProc);

	/* Attach with fresh handles */
	req_mqh = shm_mq_attach(request_mq, NULL, NULL);
	resp_mqh = shm_mq_attach(response_mq, NULL, NULL);

	/* Signal that we're ready */
	progress->phase = MUX_STATE_IDLE;
	progress->current_conn_id = 0;
	progress->requester_pid = 0;
	pg_write_barrier();

	wq->ready = true;
	pg_write_barrier();

	/* Blocking receive - waits until a backend sends a request */
	progress->phase = MUX_STATE_RECEIVING;
	pg_write_barrier();

	res = shm_mq_receive(req_mqh, &msg_len, &msg_data, false);

	/* No longer ready - processing a request */
	wq->ready = false;
	pg_write_barrier();

	if (res != SHM_MQ_SUCCESS)
	{
		shm_mq_release_handle(req_mqh);
		shm_mq_release_handle(resp_mqh);
		return;
	}

	msg = (ConnMuxMessage *) msg_data;

	/* Update progress: we received a request */
	progress->current_request_type = msg->type;
	progress->current_conn_id = msg->conn_id;
	progress->last_request_time = GetCurrentTimestamp();
	pg_write_barrier();

	switch (msg->type)
	{
		case CONN_MUX_MSG_CONNECT:
			{
				WorkerConnection *conn = allocate_connection(state->connections);
				ConnMuxMessage *resp;
				Size resp_len;

				progress->phase = MUX_STATE_CONNECTING;
				pg_write_barrier();

				if (conn != NULL)
				{
					PGconn *pgconn;
					
					conn->conn_id = msg->conn_id;
					strncpy(conn->conninfo, msg->data, sizeof(conn->conninfo) - 1);
					conn->conninfo[sizeof(conn->conninfo) - 1] = '\0';

					pgconn = PQconnectdb(conn->conninfo);
					
					if (PQstatus(pgconn) == CONNECTION_OK)
					{
						conn->pgconn = pgconn;
						
						resp_len = offsetof(ConnMuxMessage, data) + 8;
						resp = palloc(resp_len);
						resp->type = CONN_MUX_MSG_RESPONSE;
						resp->length = resp_len;
						resp->conn_id = msg->conn_id;
						resp->request_id = msg->request_id;
						strcpy(resp->data, "OK");

						ereport(LOG,
								(errmsg("worker %d: established connection %d to %s",
										state->worker_id, msg->conn_id, PQdb(pgconn))));
					}
					else
					{
						const char *err = PQerrorMessage(pgconn);
						PQfinish(pgconn);
						conn->in_use = false;
						conn->pgconn = NULL;
						
						resp_len = offsetof(ConnMuxMessage, data) + strlen(err) + 1;
						resp = palloc(resp_len);
						resp->type = CONN_MUX_MSG_ERROR;
						resp->length = resp_len;
						resp->conn_id = msg->conn_id;
						resp->request_id = msg->request_id;
						strcpy(resp->data, err);

						had_error = true;
						ereport(WARNING,
								(errmsg("worker %d: connection %d failed: %s",
										state->worker_id, msg->conn_id, err)));
					}
				}
				else
				{
					resp_len = offsetof(ConnMuxMessage, data) + 32;
					resp = palloc(resp_len);
					resp->type = CONN_MUX_MSG_ERROR;
					resp->length = resp_len;
					resp->conn_id = msg->conn_id;
					resp->request_id = msg->request_id;
					strcpy(resp->data, "No slots available");

					had_error = true;
					ereport(WARNING,
							(errmsg("worker %d: no slots for connection %d",
									state->worker_id, msg->conn_id)));
				}

				progress->phase = MUX_STATE_SENDING_RESPONSE;
				pg_write_barrier();

				if (shm_mq_send(resp_mqh, resp->length, resp, false, true) != SHM_MQ_SUCCESS)
				{
					ereport(WARNING,
							(errmsg("worker %d: failed to send CONNECT response", state->worker_id)));
				}
				pfree(resp);

				progress->connect_count++;
				progress->active_connections = count_active_connections(state->connections);
				break;
			}

		case CONN_MUX_MSG_QUERY:
			{
				WorkerConnection *conn = find_connection(state->connections, msg->conn_id);
				ConnMuxMessage *resp;
				Size resp_len;

				progress->phase = MUX_STATE_EXECUTING;
				pg_write_barrier();

				if (conn != NULL && conn->pgconn != NULL)
				{
					PGconn *pgconn = (PGconn *) conn->pgconn;
					PGresult *result;
					
					result = PQexec(pgconn, msg->data);
					
					if (result != NULL)
					{
						ExecStatusType status = PQresultStatus(result);
						
						if (status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK)
						{
							char result_str[256];
							snprintf(result_str, sizeof(result_str), 
									"OK: %s", PQcmdStatus(result));
							
							resp_len = offsetof(ConnMuxMessage, data) + strlen(result_str) + 1;
							resp = palloc(resp_len);
							resp->type = CONN_MUX_MSG_RESPONSE;
							resp->length = resp_len;
							resp->conn_id = msg->conn_id;
							resp->request_id = msg->request_id;
							strcpy(resp->data, result_str);
						}
						else
						{
							const char *err = PQresultErrorMessage(result);
							resp_len = offsetof(ConnMuxMessage, data) + strlen(err) + 1;
							resp = palloc(resp_len);
							resp->type = CONN_MUX_MSG_ERROR;
							resp->length = resp_len;
							resp->conn_id = msg->conn_id;
							resp->request_id = msg->request_id;
							strcpy(resp->data, err);

							had_error = true;
							ereport(WARNING,
									(errmsg("worker %d: query failed on connection %d: %s",
											state->worker_id, msg->conn_id, err)));
						}
						
						PQclear(result);
					}
					else
					{
						const char *err = PQerrorMessage(pgconn);
						resp_len = offsetof(ConnMuxMessage, data) + strlen(err) + 1;
						resp = palloc(resp_len);
						resp->type = CONN_MUX_MSG_ERROR;
						resp->length = resp_len;
						resp->conn_id = msg->conn_id;
						resp->request_id = msg->request_id;
						strcpy(resp->data, err);

						had_error = true;
						ereport(WARNING,
								(errmsg("worker %d: PQexec failed on connection %d: %s",
										state->worker_id, msg->conn_id, err)));
					}
				}
				else
				{
					resp_len = offsetof(ConnMuxMessage, data) + 32;
					resp = palloc(resp_len);
					resp->type = CONN_MUX_MSG_ERROR;
					resp->length = resp_len;
					resp->conn_id = msg->conn_id;
					resp->request_id = msg->request_id;
					strcpy(resp->data, "Connection not found");

					had_error = true;
					ereport(WARNING,
							(errmsg("worker %d: connection %d not found",
									state->worker_id, msg->conn_id)));
				}

				progress->phase = MUX_STATE_SENDING_RESPONSE;
				pg_write_barrier();

				if (shm_mq_send(resp_mqh, resp->length, resp, false, true) != SHM_MQ_SUCCESS)
				{
					ereport(WARNING,
							(errmsg("worker %d: failed to send QUERY response", state->worker_id)));
				}
				pfree(resp);

				progress->query_count++;
				break;
			}

		case CONN_MUX_MSG_CLOSE:
			{
				WorkerConnection *conn = find_connection(state->connections, msg->conn_id);
				ConnMuxMessage *resp;
				Size resp_len;

				progress->phase = MUX_STATE_CLOSING;
				pg_write_barrier();

				if (conn != NULL)
				{
					if (conn->pgconn != NULL)
					{
						PQfinish((PGconn *) conn->pgconn);
						conn->pgconn = NULL;
					}
					
					conn->in_use = false;

					resp_len = offsetof(ConnMuxMessage, data) + 8;
					resp = palloc(resp_len);
					resp->type = CONN_MUX_MSG_RESPONSE;
					resp->length = resp_len;
					resp->conn_id = msg->conn_id;
					resp->request_id = msg->request_id;
					strcpy(resp->data, "OK");

					ereport(LOG,
							(errmsg("worker %d: closed connection %d",
									state->worker_id, msg->conn_id)));
				}
				else
				{
					resp_len = offsetof(ConnMuxMessage, data) + 32;
					resp = palloc(resp_len);
					resp->type = CONN_MUX_MSG_ERROR;
					resp->length = resp_len;
					resp->conn_id = msg->conn_id;
					resp->request_id = msg->request_id;
					strcpy(resp->data, "Connection not found");

					had_error = true;
				}

				progress->phase = MUX_STATE_SENDING_RESPONSE;
				pg_write_barrier();

				if (shm_mq_send(resp_mqh, resp->length, resp, false, true) != SHM_MQ_SUCCESS)
				{
					ereport(WARNING,
							(errmsg("worker %d: failed to send CLOSE response", state->worker_id)));
				}
				pfree(resp);

				progress->close_count++;
				progress->active_connections = count_active_connections(state->connections);
				break;
			}

		default:
			ereport(WARNING,
					(errmsg("worker %d: unknown message type %d",
							state->worker_id, msg->type)));
			had_error = true;
			break;
	}

	/* Update completion counters */
	progress->requests_completed++;
	if (had_error)
		progress->error_count++;

	/* Release handles without detaching - queue will be reinited on next call */
	LWLockAcquire(&wq->queue_lock, LW_EXCLUSIVE);
	shm_mq_release_handle(req_mqh);
	shm_mq_release_handle(resp_mqh);
	LWLockRelease(&wq->queue_lock);

	CHECK_FOR_INTERRUPTS();
}

/*
 * Background worker main function
 *
 * FULL IMPLEMENTATION:
 * This worker maintains a pool of foreign server connections using libpq
 * and processes requests from backends through shared memory message queues.
 * 
 * The worker:
 * 1. Attaches to pre-created DSM segment (created by postmaster in InitConnMultiplexer)
 * 2. Maintains an array of WorkerConnection slots (max 100)
 * 3. Processes CONNECT messages - establishes PGconn via PQconnectdb()
 * 4. Processes QUERY messages - executes queries via PQexec()
 * 5. Processes CLOSE messages - closes connections via PQfinish()
 * 6. Monitors connection health and handles reconnection
 * 7. Uses shared memory queues for bi-directional communication
 * 
 * Full message passing protocol:
 * - Backend creates ConnMuxMessage with conninfo/query data
 * - Sends via shm_mq to worker's request queue
 * - Worker processes request using libpq functions
 * - Worker sends ConnMuxMessage response via shm_mq response queue
 * - Backend receives and processes response
 * 
 * DSM Architecture:
 * - DSM segments are created by postmaster in InitConnMultiplexer()
 * - Workers attach to pre-created segments using handles from shared memory
 * - This ensures consistent virtual address mappings across processes
 */
void
conn_multiplexer_worker_main(Datum main_arg)
{
	int			worker_id = DatumGetInt32(main_arg);
	WorkerState *state;
	MemoryContext oldcontext;
	int			i;

	/* Setup signal handlers */
	pqsignal(SIGTERM, conn_multiplexer_worker_sigterm);
	pqsignal(SIGHUP, conn_multiplexer_worker_sighup);
	BackgroundWorkerUnblockSignals();

	/* Identify ourselves in pg_stat_activity */
	pgstat_report_appname("conn_multiplexer worker");

	/* Allocate worker state in TopMemoryContext so it persists */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	state = palloc0(sizeof(WorkerState));
	state->worker_id = worker_id;
	state->connections = palloc0(sizeof(WorkerConnection) * MAX_WORKER_CONNECTIONS);
	MemoryContextSwitchTo(oldcontext);

	/* Initialize all connection slots */
	for (i = 0; i < MAX_WORKER_CONNECTIONS; i++)
	{
		state->connections[i].conn_id = 0;
		state->connections[i].pgconn = NULL;
		state->connections[i].in_use = false;
		state->connections[i].conninfo[0] = '\0';
	}

	ereport(LOG,
			(errmsg("multiplexer worker %d started", worker_id)));

	/* Initialize progress tracking in shared memory */
	{
		ConnMuxWorkerProgress *progress =
			&ConnMultiplexerShmem->worker_queues[worker_id].progress;

		progress->pid = MyProcPid;
		progress->phase = MUX_STATE_STARTING;
		progress->worker_start_time = GetCurrentTimestamp();
		progress->requests_completed = 0;
		progress->connect_count = 0;
		progress->query_count = 0;
		progress->close_count = 0;
		progress->error_count = 0;
		progress->active_connections = 0;
		progress->current_conn_id = 0;
		progress->requester_pid = 0;
		progress->last_request_time = 0;
		pg_write_barrier();
	}

	/* Update shared memory */
	LWLockAcquire(ConnMultiplexerLock, LW_EXCLUSIVE);
	ConnMultiplexerShmem->num_workers++;
	if (ConnMultiplexerShmem->num_workers == foreign_conn_multiplexer_workers)
		ConnMultiplexerShmem->initialized = true;
	LWLockRelease(ConnMultiplexerLock);

	/*
	 * Main worker loop.
	 *
	 * Each iteration: reinit queues, set roles, attach, blocking receive,
	 * process, send response, release handles. The blocking receive in
	 * process_worker_requests will wait until a backend sends a request.
	 */
	while (!got_sigterm)
	{
		/* Handle configuration reload */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Process one request (blocking until a request arrives) */
		process_worker_requests(state);
	}

	/* Cleanup all connections on exit */
	for (i = 0; i < MAX_WORKER_CONNECTIONS; i++)
	{
		if (state->connections[i].in_use && state->connections[i].pgconn != NULL)
		{
			PQfinish((PGconn *) state->connections[i].pgconn);
			state->connections[i].pgconn = NULL;
			state->connections[i].in_use = false;
		}
	}

	/* Update worker count in shared memory */
	LWLockAcquire(ConnMultiplexerLock, LW_EXCLUSIVE);
	ConnMultiplexerShmem->num_workers--;
	if (ConnMultiplexerShmem->num_workers == 0)
		ConnMultiplexerShmem->initialized = false;
	LWLockRelease(ConnMultiplexerLock);

	/* Mark worker as stopped in progress */
	ConnMultiplexerShmem->worker_queues[worker_id].progress.phase = MUX_STATE_STOPPED;
	ConnMultiplexerShmem->worker_queues[worker_id].progress.pid = 0;
	pg_write_barrier();

	ereport(LOG,
			(errmsg("connection multiplexer worker %d stopping",
					state->worker_id)));

	proc_exit(0);
}

/*
 * SIGTERM handler for workers
 */
static void
conn_multiplexer_worker_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * SIGHUP handler for workers
 */
static void
conn_multiplexer_worker_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Check if multiplexer is enabled and initialized
 */
bool
IsConnMultiplexerEnabled(void)
{
	bool		initialized;

	if (!foreign_conn_multiplexer_workers)
		return false;

	if (!ConnMultiplexerShmem)
		return false;

	/* Check if workers are initialized */
	LWLockAcquire(ConnMultiplexerLock, LW_SHARED);
	initialized = ConnMultiplexerShmem->initialized;
	LWLockRelease(ConnMultiplexerLock);

	return initialized;
}

/*
 * Get next available worker using round-robin
 */
int
GetNextMultiplexerWorker(void)
{
	int			worker_id;

	if (!IsConnMultiplexerEnabled())
		return -1;

	LWLockAcquire(ConnMultiplexerLock, LW_EXCLUSIVE);
	worker_id = ConnMultiplexerShmem->next_worker;
	ConnMultiplexerShmem->next_worker = 
		(ConnMultiplexerShmem->next_worker + 1) % foreign_conn_multiplexer_workers;
	LWLockRelease(ConnMultiplexerLock);

	return worker_id;
}

/*
 * Allocate a new connection ID
 */
static int
allocate_conn_id(void)
{
	int conn_id;
	
	LWLockAcquire(ConnMultiplexerLock, LW_EXCLUSIVE);
	conn_id = ConnMultiplexerShmem->next_conn_id++;
	LWLockRelease(ConnMultiplexerLock);
	
	return conn_id;
}

/*
 * Wait for a worker to be ready, then send a message and receive the response.
 *
 * This handles the entire request-response cycle under the per-worker queue_lock:
 * 1. Acquire queue_lock EXCLUSIVE (serializes access between backends)
 * 2. Wait for worker to be ready (it reinits queues and sets its roles)
 * 3. Set our roles as sender on request queue, receiver on response queue
 * 4. Attach, send request, wake worker, receive response
 * 5. Release handles (without detaching)
 * 6. Release queue_lock
 *
 * Returns the response message. Caller must check resp_msg->type.
 */
static ConnMuxMessage *
multiplexer_send_receive(int worker_id, const void *msg, Size msg_len)
{
	ConnMultiplexerWorkerQueues *wq;
	shm_mq	   *request_mq;
	shm_mq	   *response_mq;
	shm_mq_handle *req_mqh;
	shm_mq_handle *resp_mqh;
	shm_mq_result res;
	void	   *resp_data;
	Size		resp_len;
	ConnMuxMessage *resp_msg;
	ConnMuxMessage *resp_copy;
	int			wait_count = 0;

	wq = &ConnMultiplexerShmem->worker_queues[worker_id];

	/* Acquire exclusive lock for this worker's queue pair */
	LWLockAcquire(&wq->queue_lock, LW_EXCLUSIVE);

	/*
	 * Wait for the worker to be ready. The worker sets ready=true after
	 * reiniting queues and setting its roles. We release the lock while
	 * waiting so the worker can make progress.
	 */
	while (!wq->ready)
	{
		pg_read_barrier();
		LWLockRelease(&wq->queue_lock);

		if (wait_count >= 1000)		/* ~10 seconds */
		{
			ereport(ERROR,
					(errmsg("multiplexer: worker %d not ready after timeout", worker_id)));
		}
		wait_count++;

		pg_usleep(10000);				/* 10ms */
		LWLockAcquire(&wq->queue_lock, LW_EXCLUSIVE);
	}

	request_mq = wq->request_queue;
	response_mq = wq->response_queue;

	/* Record requester PID in worker progress for observability */
	wq->progress.requester_pid = MyProcPid;
	pg_write_barrier();

	/* Set our roles: backend is sender on request, receiver on response */
	shm_mq_set_sender(request_mq, MyProc);
	shm_mq_set_receiver(response_mq, MyProc);

	/* Attach with fresh handles */
	req_mqh = shm_mq_attach(request_mq, NULL, NULL);
	resp_mqh = shm_mq_attach(response_mq, NULL, NULL);

	/* Send the request message */
	res = shm_mq_send(req_mqh, msg_len, msg, false, true);

	if (res != SHM_MQ_SUCCESS)
	{
		shm_mq_release_handle(req_mqh);
		shm_mq_release_handle(resp_mqh);
		LWLockRelease(&wq->queue_lock);
		ereport(ERROR,
				(errmsg("multiplexer: failed to send message to worker %d", worker_id)));
	}

	/* Wait for response */
	res = shm_mq_receive(resp_mqh, &resp_len, &resp_data, false);

	if (res != SHM_MQ_SUCCESS)
	{
		shm_mq_release_handle(req_mqh);
		shm_mq_release_handle(resp_mqh);
		LWLockRelease(&wq->queue_lock);
		ereport(ERROR,
				(errmsg("multiplexer: failed to receive response from worker %d", worker_id)));
	}

	/* Copy response before releasing handles (data points into queue buffer) */
	resp_msg = (ConnMuxMessage *) resp_data;
	resp_copy = palloc(resp_len);
	memcpy(resp_copy, resp_msg, resp_len);

	/* Release handles without detaching - worker will reinit for next cycle */
	shm_mq_release_handle(req_mqh);
	shm_mq_release_handle(resp_mqh);

	LWLockRelease(&wq->queue_lock);

	return resp_copy;
}

/*
 * Send connection request to multiplexer worker
 */
bool
MultiplexerConnect(const char *conninfo, int *conn_id_out)
{
	int			worker_id;
	int			conn_id;
	ConnMuxMessage *msg;
	Size		msg_len;
	ConnMuxMessage *resp;
	
	if (!IsConnMultiplexerEnabled())
		return false;

	/* Allocate connection ID first */
	conn_id = allocate_conn_id();

	/* Determine worker for this connection */
	worker_id = conn_id % foreign_conn_multiplexer_workers;

	/* Create CONNECT message */
	msg_len = offsetof(ConnMuxMessage, data) + strlen(conninfo) + 1;
	msg = palloc(msg_len);
	msg->type = CONN_MUX_MSG_CONNECT;
	msg->length = msg_len;
	msg->conn_id = conn_id;
	msg->request_id = 0;
	strcpy(msg->data, conninfo);

	/* Send and receive via shared memory queue */
	resp = multiplexer_send_receive(worker_id, msg, msg_len);
	pfree(msg);

	if (resp->type == CONN_MUX_MSG_RESPONSE)
	{
		ereport(LOG,
				(errmsg("multiplexer: CONNECT successful, conn_id=%d, worker=%d",
						conn_id, worker_id)));
		*conn_id_out = conn_id;
		pfree(resp);
		return true;
	}
	else
	{
		ereport(ERROR,
				(errmsg("multiplexer: CONNECT failed: %s", resp->data)));
		pfree(resp);	/* not reached, but for completeness */
		return false;
	}
}

/*
 * Send query through multiplexer
 */
bool
MultiplexerQuery(int conn_id, const char *query, void **result_out)
{
	int			worker_id;
	ConnMuxMessage *msg;
	Size		msg_len;
	ConnMuxMessage *resp;
	
	if (!IsConnMultiplexerEnabled())
		return false;

	/* Determine which worker has this connection */
	worker_id = conn_id % foreign_conn_multiplexer_workers;

	/* Create QUERY message */
	msg_len = offsetof(ConnMuxMessage, data) + strlen(query) + 1;
	msg = palloc(msg_len);
	msg->type = CONN_MUX_MSG_QUERY;
	msg->length = msg_len;
	msg->conn_id = conn_id;
	msg->request_id = 0;
	strcpy(msg->data, query);

	/* Send and receive */
	resp = multiplexer_send_receive(worker_id, msg, msg_len);
	pfree(msg);

	if (resp->type == CONN_MUX_MSG_RESPONSE)
	{
		pfree(resp);
		return true;
	}
	else
	{
		ereport(ERROR,
				(errmsg("multiplexer: QUERY failed: %s", resp->data)));
		pfree(resp);	/* not reached, but for completeness */
		return false;
	}
}

/*
 * Close multiplexed connection
 */
void
MultiplexerClose(int conn_id)
{
	int			worker_id;
	ConnMuxMessageHeader msg;
	ConnMuxMessage *resp;
	
	if (!IsConnMultiplexerEnabled())
		return;

	/* Determine which worker has this connection */
	worker_id = conn_id % foreign_conn_multiplexer_workers;

	/* Create CLOSE message */
	msg.type = CONN_MUX_MSG_CLOSE;
	msg.length = sizeof(ConnMuxMessageHeader);
	msg.conn_id = conn_id;
	msg.request_id = 0;

	/* Send and receive */
	resp = multiplexer_send_receive(worker_id, &msg, sizeof(ConnMuxMessageHeader));

	if (resp->type == CONN_MUX_MSG_RESPONSE)
	{
		ereport(LOG,
				(errmsg("multiplexer: CLOSE successful, conn_id=%d", conn_id)));
	}
	else
	{
		ereport(WARNING,
				(errmsg("multiplexer: CLOSE failed: %s", resp->data)));
	}

	pfree(resp);
}

/*
 * Helper: return human-readable string for worker phase.
 */
static const char *
conn_mux_phase_name(ConnMuxWorkerPhase phase)
{
	switch (phase)
	{
		case MUX_STATE_STARTING:		return "starting";
		case MUX_STATE_IDLE:			return "idle";
		case MUX_STATE_RECEIVING:		return "waiting for request";
		case MUX_STATE_CONNECTING:		return "connecting";
		case MUX_STATE_EXECUTING:		return "executing query";
		case MUX_STATE_CLOSING:			return "closing connection";
		case MUX_STATE_SENDING_RESPONSE: return "sending response";
		case MUX_STATE_STOPPED:			return "stopped";
		default:						return "unknown";
	}
}

/*
 * Helper: return human-readable string for request type.
 */
static const char *
conn_mux_request_type_name(ConnMuxMessageType type)
{
	switch (type)
	{
		case CONN_MUX_MSG_CONNECT:	return "CONNECT";
		case CONN_MUX_MSG_QUERY:	return "QUERY";
		case CONN_MUX_MSG_CLOSE:	return "CLOSE";
		case CONN_MUX_MSG_RESPONSE:	return "RESPONSE";
		case CONN_MUX_MSG_ERROR:	return "ERROR";
		default:					return "unknown";
	}
}

/*
 * SQL function: pg_stat_conn_multiplexer()
 *
 * Returns a set of records describing the state of each multiplexer worker.
 * Reads directly from shared memory — no locks needed since fields are
 * written atomically from the worker side with write barriers.
 */
#define PG_STAT_CONN_MUX_COLS 14

Datum pg_stat_conn_multiplexer(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_stat_conn_multiplexer);

Datum
pg_stat_conn_multiplexer(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			num_workers;
	int			i;

	InitMaterializedSRF(fcinfo, 0);

	if (!ConnMultiplexerShmem)
		PG_RETURN_VOID();

	num_workers = foreign_conn_multiplexer_workers;
	if (num_workers <= 0)
		PG_RETURN_VOID();

	for (i = 0; i < num_workers; i++)
	{
		Datum		values[PG_STAT_CONN_MUX_COLS];
		bool		nulls[PG_STAT_CONN_MUX_COLS];
		ConnMuxWorkerProgress snap;
		ConnMultiplexerWorkerQueues *wq = &ConnMultiplexerShmem->worker_queues[i];

		/* Take a snapshot of progress (no lock — fields are barrier-synced) */
		pg_read_barrier();
		memcpy(&snap, &wq->progress, sizeof(ConnMuxWorkerProgress));

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		/* worker_id */
		values[0] = Int32GetDatum(i);

		/* pid */
		if (snap.pid != 0)
			values[1] = Int32GetDatum(snap.pid);
		else
			nulls[1] = true;

		/* phase */
		values[2] = CStringGetTextDatum(conn_mux_phase_name(snap.phase));

		/* current_request_type - only meaningful when actively processing */
		if (snap.phase == MUX_STATE_CONNECTING ||
			snap.phase == MUX_STATE_EXECUTING ||
			snap.phase == MUX_STATE_CLOSING ||
			snap.phase == MUX_STATE_SENDING_RESPONSE)
			values[3] = CStringGetTextDatum(conn_mux_request_type_name(snap.current_request_type));
		else
			nulls[3] = true;

		/* current_conn_id */
		if (snap.current_conn_id != 0)
			values[4] = Int32GetDatum(snap.current_conn_id);
		else
			nulls[4] = true;

		/* requester_pid */
		if (snap.requester_pid != 0)
			values[5] = Int32GetDatum(snap.requester_pid);
		else
			nulls[5] = true;

		/* active_connections */
		values[6] = Int32GetDatum(snap.active_connections);

		/* requests_completed */
		values[7] = Int64GetDatum(snap.requests_completed);

		/* connect_count */
		values[8] = Int64GetDatum(snap.connect_count);

		/* query_count */
		values[9] = Int64GetDatum(snap.query_count);

		/* close_count */
		values[10] = Int64GetDatum(snap.close_count);

		/* error_count */
		values[11] = Int64GetDatum(snap.error_count);

		/* last_request_time */
		if (snap.last_request_time != 0)
			values[12] = TimestampTzGetDatum(snap.last_request_time);
		else
			nulls[12] = true;

		/* worker_start_time */
		if (snap.worker_start_time != 0)
			values[13] = TimestampTzGetDatum(snap.worker_start_time);
		else
			nulls[13] = true;

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();
}
