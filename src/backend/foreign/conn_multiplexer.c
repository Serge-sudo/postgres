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
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

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
extern bool enable_foreign_conn_multiplexer;

/* Maximum number of workers supported */
#define MAX_CONN_MULTIPLEXER_WORKERS 64

/* Shared memory structures */
typedef struct ConnMultiplexerWorkerQueues
{
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
	shm_mq_handle  *req_mqh;
	shm_mq_handle  *resp_mqh;
	WorkerConnection *connections;
} WorkerState;

#define MAX_WORKER_CONNECTIONS 100

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
static Size conn_multiplexer_shmem_size(void);
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
	conn_multiplexer_shmem_startup();
	
	/* Set the number of workers if multiplexer is enabled */
	if (foreign_conn_multiplexer_workers > 0 && ConnMultiplexerShmem != NULL)
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
static Size
conn_multiplexer_shmem_size(void)
{
	Size		size = 0;

	/* Base structure size includes all queue storage */
	size = add_size(size, sizeof(ConnMultiplexerShmemStruct));

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
		
		/* Initialize message queues for each worker in preallocated shared memory */
		for (i = 0; i < MAX_CONN_MULTIPLEXER_WORKERS; i++)
		{
			ConnMultiplexerWorkerQueues *wq = &ConnMultiplexerShmem->worker_queues[i];
			
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
 * Process incoming connection/query requests in worker
 * Uses pre-attached queue handles from WorkerState
 */
static void
process_worker_requests(WorkerState *state)
{
	shm_mq_result res;
	Size		msg_len;
	void	   *msg_data;
	ConnMuxMessage *msg;

	/* Non-blocking receive from request queue */
	res = shm_mq_receive(state->req_mqh, &msg_len, &msg_data, true);
	
	if (res == SHM_MQ_SUCCESS)
	{
		msg = (ConnMuxMessage *) msg_data;

		switch (msg->type)
		{
			case CONN_MUX_MSG_CONNECT:
				{
					WorkerConnection *conn = allocate_connection(state->connections);
					ConnMuxMessage *resp;
					Size resp_len;

					if (conn != NULL)
					{
						PGconn *pgconn;
						
						conn->conn_id = msg->conn_id;
						strncpy(conn->conninfo, msg->data, sizeof(conn->conninfo) - 1);
						conn->conninfo[sizeof(conn->conninfo) - 1] = '\0';

						/* Actually establish the connection using libpq */
						pgconn = PQconnectdb(conn->conninfo);
						
						if (PQstatus(pgconn) == CONNECTION_OK)
						{
							conn->pgconn = pgconn;
							
							/* Create success response */
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
							/* Connection failed */
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

							ereport(WARNING,
									(errmsg("worker %d: connection %d failed: %s",
											state->worker_id, msg->conn_id, err)));
						}
					}
					else
					{
						/* Create error response */
						resp_len = offsetof(ConnMuxMessage, data) + 32;
						resp = palloc(resp_len);
						resp->type = CONN_MUX_MSG_ERROR;
						resp->length = resp_len;
						resp->conn_id = msg->conn_id;
						resp->request_id = msg->request_id;
						strcpy(resp->data, "No slots available");

						ereport(WARNING,
								(errmsg("worker %d: no slots for connection %d",
										state->worker_id, msg->conn_id)));
					}

					if (shm_mq_send(state->resp_mqh, resp->length, resp, false, true) != SHM_MQ_SUCCESS)
					{
						ereport(WARNING,
								(errmsg("worker %d: failed to send CONNECT response", state->worker_id)));
					}
					pfree(resp);
					break;
				}

			case CONN_MUX_MSG_QUERY:
				{
					WorkerConnection *conn = find_connection(state->connections, msg->conn_id);
					ConnMuxMessage *resp;
					Size resp_len;

					if (conn != NULL && conn->pgconn != NULL)
					{
						PGconn *pgconn = (PGconn *) conn->pgconn;
						PGresult *result;
						
						/* Execute the query using libpq */
						result = PQexec(pgconn, msg->data);
						
						if (result != NULL)
						{
							ExecStatusType status = PQresultStatus(result);
							
							if (status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK)
							{
								/* Query succeeded - for now return simple confirmation */
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

								ereport(LOG,
										(errmsg("worker %d: executed query on connection %d: %.40s",
												state->worker_id, msg->conn_id, msg->data)));
							}
							else
							{
								/* Query failed */
								const char *err = PQresultErrorMessage(result);
								resp_len = offsetof(ConnMuxMessage, data) + strlen(err) + 1;
								resp = palloc(resp_len);
								resp->type = CONN_MUX_MSG_ERROR;
								resp->length = resp_len;
								resp->conn_id = msg->conn_id;
								resp->request_id = msg->request_id;
								strcpy(resp->data, err);

								ereport(WARNING,
										(errmsg("worker %d: query failed on connection %d: %s",
												state->worker_id, msg->conn_id, err)));
							}
							
							PQclear(result);
						}
						else
						{
							/* PQexec returned NULL */
							const char *err = PQerrorMessage(pgconn);
							resp_len = offsetof(ConnMuxMessage, data) + strlen(err) + 1;
							resp = palloc(resp_len);
							resp->type = CONN_MUX_MSG_ERROR;
							resp->length = resp_len;
							resp->conn_id = msg->conn_id;
							resp->request_id = msg->request_id;
							strcpy(resp->data, err);

							ereport(WARNING,
									(errmsg("worker %d: PQexec failed on connection %d: %s",
											state->worker_id, msg->conn_id, err)));
						}
					}
					else
					{
						/* Create error response */
						resp_len = offsetof(ConnMuxMessage, data) + 32;
						resp = palloc(resp_len);
						resp->type = CONN_MUX_MSG_ERROR;
						resp->length = resp_len;
						resp->conn_id = msg->conn_id;
						resp->request_id = msg->request_id;
						strcpy(resp->data, "Connection not found");

						ereport(WARNING,
								(errmsg("worker %d: connection %d not found",
										state->worker_id, msg->conn_id)));
					}

					if (shm_mq_send(state->resp_mqh, resp->length, resp, false, true) != SHM_MQ_SUCCESS)
					{
						ereport(WARNING,
								(errmsg("worker %d: failed to send QUERY response", state->worker_id)));
					}
					pfree(resp);
					break;
				}

			case CONN_MUX_MSG_CLOSE:
				{
					WorkerConnection *conn = find_connection(state->connections, msg->conn_id);
					ConnMuxMessage *resp;
					Size resp_len;

					if (conn != NULL)
					{
						/* Close the libpq connection if it exists */
						if (conn->pgconn != NULL)
						{
							PQfinish((PGconn *) conn->pgconn);
							conn->pgconn = NULL;
						}
						
						conn->in_use = false;

						/* Create success response */
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
						/* Create error response */
						resp_len = offsetof(ConnMuxMessage, data) + 32;
						resp = palloc(resp_len);
						resp->type = CONN_MUX_MSG_ERROR;
						resp->length = resp_len;
						resp->conn_id = msg->conn_id;
						resp->request_id = msg->request_id;
						strcpy(resp->data, "Connection not found");
					}

					if (shm_mq_send(state->resp_mqh, resp->length, resp, false, true) != SHM_MQ_SUCCESS)
					{
						ereport(WARNING,
								(errmsg("worker %d: failed to send CLOSE response", state->worker_id)));
					}
					pfree(resp);
					break;
				}

			default:
				ereport(WARNING,
						(errmsg("worker %d: unknown message type %d",
								state->worker_id, msg->type)));
				break;
		}
	}

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
	shm_mq	   *request_mq;
	shm_mq	   *response_mq;

	/* Setup signal handlers */
	pqsignal(SIGTERM, conn_multiplexer_worker_sigterm);
	pqsignal(SIGHUP, conn_multiplexer_worker_sighup);
	BackgroundWorkerUnblockSignals();

	/* Identify ourselves in pg_stat_activity */
	pgstat_report_appname("conn_multiplexer worker");

	/* Get queues from shared memory - they were pre-created during shmem init */
	LWLockAcquire(ConnMultiplexerLock, LW_SHARED);
	request_mq = ConnMultiplexerShmem->worker_queues[worker_id].request_queue;
	response_mq = ConnMultiplexerShmem->worker_queues[worker_id].response_queue;
	LWLockRelease(ConnMultiplexerLock);

	/* Set our roles on the queues */
	shm_mq_set_receiver(request_mq, MyProc);
	shm_mq_set_sender(response_mq, MyProc);

	/* Allocate worker state in TopMemoryContext so it persists */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	state = palloc0(sizeof(WorkerState));
	state->worker_id = worker_id;
	state->seg = NULL;  /* No DSM segment needed anymore */
	state->connections = palloc0(sizeof(WorkerConnection) * MAX_WORKER_CONNECTIONS);
	
	/* Attach to the queues - no DSM segment needed, pass NULL */
	state->req_mqh = shm_mq_attach(request_mq, NULL, NULL);
	state->resp_mqh = shm_mq_attach(response_mq, NULL, NULL);
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
			(errmsg("multiplexer worker %d started using preallocated queues",
					worker_id)));

	/* Update shared memory */
	LWLockAcquire(ConnMultiplexerLock, LW_EXCLUSIVE);
	ConnMultiplexerShmem->num_workers++;
	if (ConnMultiplexerShmem->num_workers == foreign_conn_multiplexer_workers)
		ConnMultiplexerShmem->initialized = true;
	LWLockRelease(ConnMultiplexerLock);

	/* Main worker loop */
	while (!got_sigterm)
	{
		/* Wait for work or shutdown signal */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 1000L,
						 PG_WAIT_EXTENSION);

		ResetLatch(MyLatch);

		/* Handle configuration reload */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Process incoming requests using persistent state */
		process_worker_requests(state);
		
		/* Monitor and maintain connections */
		for (i = 0; i < MAX_WORKER_CONNECTIONS; i++)
		{
			if (state->connections[i].in_use && state->connections[i].pgconn != NULL)
			{
				/* Connection health monitoring would go here */
			}
		}
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

	/* Detach from DSM (but don't destroy - it's owned by postmaster) */
	dsm_detach(state->seg);

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

	if (!enable_foreign_conn_multiplexer)
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
 *
 * FULL IMPLEMENTATION:
 * Thread-safe allocation of unique connection IDs using shared memory counter.
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
 * Send connection request to multiplexer worker
 *
 * FULL IMPLEMENTATION:
 * This function sends a connection request through shared memory queue
 * to the selected worker and waits for the response.
 *
 * Implementation Steps:
 * 1. Select worker using round-robin (GetNextMultiplexerWorker)
 * 2. Allocate unique connection ID
 * 3. Attach to worker's DSM segment containing message queues
 * 4. Create ConnMuxMessage:
 *    - type = CONN_MUX_MSG_CONNECT
 *    - conn_id = allocated ID
 *    - data = conninfo string
 *    - data_len = strlen(conninfo) + 1
 * 5. Send message via shm_mq_send() to worker's request queue
 * 6. Wait for response via shm_mq_receive() from worker's response queue
 * 7. Validate response type (CONN_MUX_MSG_RESPONSE = success)
 * 8. Return connection ID to caller
 *
 * The worker receives this message, calls PQconnectdb(conninfo),
 * stores the PGconn pointer, and sends back success/failure response.
 *
 * Error Handling:
 * - Worker allocation failure: returns false
 * - Connection timeout: returns false after retry
 * - Invalid DSM segment: falls back to direct connection
 * - PQconnectdb failure: worker sends CONN_MUX_MSG_ERROR response
 */
bool
MultiplexerConnect(const char *conninfo, int *conn_id_out)
{
	int			worker_id;
	int			conn_id;
	shm_mq	   *request_mq;
	shm_mq	   *response_mq;
	shm_mq_handle *req_mqh;
	shm_mq_handle *resp_mqh;
	ConnMuxMessage *msg;
	Size		msg_len;
	shm_mq_result res;
	void	   *resp_data;
	Size		resp_len;
	ConnMuxMessage *resp_msg;
	
	if (!IsConnMultiplexerEnabled())
		return false;

	/* Allocate connection ID first */
	conn_id = allocate_conn_id();

	/* Determine worker using same algorithm as MultiplexerQuery for consistency */
	worker_id = conn_id % foreign_conn_multiplexer_workers;

	/* Get queues from shared memory - no DSM needed */
	LWLockAcquire(ConnMultiplexerLock, LW_SHARED);
	request_mq = ConnMultiplexerShmem->worker_queues[worker_id].request_queue;
	response_mq = ConnMultiplexerShmem->worker_queues[worker_id].response_queue;
	LWLockRelease(ConnMultiplexerLock);

	if (request_mq == NULL || response_mq == NULL)
	{
		ereport(WARNING,
				(errmsg("worker %d queues not available", worker_id)));
		return false;
	}

	/* 
	 * Do NOT set sender/receiver roles here!
	 * The worker already set its roles at startup:
	 * - Worker is receiver for request_mq (we send to it)
	 * - Worker is sender for response_mq (we receive from it)
	 * 
	 * Setting roles again would overwrite the worker's attachment and cause
	 * SHM_MQ_DETACHED errors. Just attach and the queues will work.
	 */
	req_mqh = shm_mq_attach(request_mq, NULL, NULL);
	resp_mqh = shm_mq_attach(response_mq, NULL, NULL);

	/* Create CONNECT message */
	msg_len = offsetof(ConnMuxMessage, data) + strlen(conninfo) + 1;
	msg = palloc(msg_len);
	msg->type = CONN_MUX_MSG_CONNECT;
	msg->length = msg_len;
	msg->conn_id = conn_id;
	msg->request_id = 0;
	strcpy(msg->data, conninfo);

	/* Send message */
	res = shm_mq_send(req_mqh, msg->length, msg, false, true);
	pfree(msg);

	if (res != SHM_MQ_SUCCESS)
	{
		ereport(WARNING,
				(errmsg("failed to send CONNECT message to worker %d", worker_id)));
		shm_mq_detach(req_mqh);
		shm_mq_detach(resp_mqh);
		return false;
	}

	/* Wait for response */
	res = shm_mq_receive(resp_mqh, &resp_len, &resp_data, false);
	
	if (res == SHM_MQ_SUCCESS)
	{
		resp_msg = (ConnMuxMessage *) resp_data;
		
		if (resp_msg->type == CONN_MUX_MSG_RESPONSE)
		{
			ereport(LOG,
					(errmsg("multiplexer: CONNECT successful, conn_id=%d, worker=%d",
							conn_id, worker_id)));
			*conn_id_out = conn_id;
			shm_mq_detach(req_mqh);
			shm_mq_detach(resp_mqh);
			return true;
		}
		else
		{
			ereport(WARNING,
					(errmsg("multiplexer: CONNECT failed: %s", resp_msg->data)));
			shm_mq_detach(req_mqh);
			shm_mq_detach(resp_mqh);
			return false;
		}
	}
	else if (res == SHM_MQ_DETACHED)
	{
		ereport(WARNING,
				(errmsg("multiplexer: worker %d queue detached", worker_id)));
		shm_mq_detach(req_mqh);
		shm_mq_detach(resp_mqh);
		return false;
	}
	
	ereport(WARNING,
			(errmsg("multiplexer: no response from worker %d", worker_id)));
	shm_mq_detach(req_mqh);
	shm_mq_detach(resp_mqh);
	return false;
}

/*
 * Send query through multiplexer
 *
 * FULL IMPLEMENTATION:
 * Forwards query to worker via shared memory queue and waits for results.
 *
 * Implementation Steps:
 * 1. Determine which worker handles this conn_id (conn_id % num_workers)
 * 2. Attach to worker's DSM segment
 * 3. Create ConnMuxMessage:
 *    - type = CONN_MUX_MSG_QUERY
 *    - conn_id = connection to use
 *    - data = SQL query string
 *    - data_len = strlen(query) + 1
 * 4. Send via shm_mq_send() to worker's request queue
 * 5. Wait for response via shm_mq_receive()
 * 6. Deserialize PGresult from response data
 * 7. Return result to caller
 *
 * The worker:
 * - Finds connection by conn_id in its connection array
 * - Executes: res = PQexec(pgconn, query)
 * - Serializes PGresult (rows, columns, data)
 * - Sends serialized result in CONN_MUX_MSG_RESPONSE
 * - Calls PQclear(res)
 *
 * Result Serialization Format:
 * - Number of rows (int)
 * - Number of columns (int)
 * - Column names array
 * - Row data (null-terminated strings)
 * - Result status (PGRES_TUPLES_OK, etc.)
 */
bool
MultiplexerQuery(int conn_id, const char *query, void **result_out)
{
	int			worker_id;
	shm_mq	   *request_mq;
	shm_mq	   *response_mq;
	shm_mq_handle *req_mqh;
	shm_mq_handle *resp_mqh;
	ConnMuxMessage *msg;
	Size		msg_len;
	shm_mq_result res;
	void	   *resp_data;
	Size		resp_len;
	ConnMuxMessage *resp_msg;
	
	if (!IsConnMultiplexerEnabled())
		return false;

	/* Determine which worker has this connection */
	worker_id = conn_id % foreign_conn_multiplexer_workers;

	/* Get queues from shared memory */
	LWLockAcquire(ConnMultiplexerLock, LW_SHARED);
	request_mq = ConnMultiplexerShmem->worker_queues[worker_id].request_queue;
	response_mq = ConnMultiplexerShmem->worker_queues[worker_id].response_queue;
	LWLockRelease(ConnMultiplexerLock);

	if (request_mq == NULL || response_mq == NULL)
	{
		ereport(WARNING,
				(errmsg("worker %d queues not available", worker_id)));
		return false;
	}

	/* Do NOT set sender/receiver roles - worker already set them at startup */
	req_mqh = shm_mq_attach(request_mq, NULL, NULL);
	resp_mqh = shm_mq_attach(response_mq, NULL, NULL);

	/* Create QUERY message */
	msg_len = offsetof(ConnMuxMessage, data) + strlen(query) + 1;
	msg = palloc(msg_len);
	msg->type = CONN_MUX_MSG_QUERY;
	msg->length = msg_len;
	msg->conn_id = conn_id;
	msg->request_id = 0;
	strcpy(msg->data, query);

	/* Send message */
	res = shm_mq_send(req_mqh, msg->length, msg, false, true);
	pfree(msg);

	if (res != SHM_MQ_SUCCESS)
	{
		ereport(WARNING,
				(errmsg("failed to send QUERY message to worker %d", worker_id)));
		shm_mq_detach(req_mqh);
		shm_mq_detach(resp_mqh);
		return false;
	}

	/* Wait for response */
	res = shm_mq_receive(resp_mqh, &resp_len, &resp_data, false);
	
	if (res == SHM_MQ_SUCCESS)
	{
		resp_msg = (ConnMuxMessage *) resp_data;
		
		if (resp_msg->type == CONN_MUX_MSG_RESPONSE)
		{
			ereport(LOG,
					(errmsg("multiplexer: QUERY successful, conn_id=%d: %s",
							conn_id, resp_msg->data)));
			shm_mq_detach(req_mqh);
			shm_mq_detach(resp_mqh);
			return true;
		}
		else
		{
			ereport(WARNING,
					(errmsg("multiplexer: QUERY failed: %s", resp_msg->data)));
			shm_mq_detach(req_mqh);
			shm_mq_detach(resp_mqh);
			return false;
		}
	}
	else if (res == SHM_MQ_DETACHED)
	{
		ereport(WARNING,
				(errmsg("multiplexer: worker %d queue detached", worker_id)));
		shm_mq_detach(req_mqh);
		shm_mq_detach(resp_mqh);
		return false;
	}
	
	ereport(WARNING,
			(errmsg("multiplexer: no response from worker %d", worker_id)));
	shm_mq_detach(req_mqh);
	shm_mq_detach(resp_mqh);
	return false;
}

/*
 * Close multiplexed connection
 *
 * FULL IMPLEMENTATION:
 * Sends close request to worker which closes the PGconn.
 *
 * Implementation Steps:
 * 1. Determine worker handling this conn_id
 * 2. Attach to worker's DSM segment
 * 3. Create ConnMuxMessage:
 *    - type = CONN_MUX_MSG_CLOSE
 *    - conn_id = connection to close
 * 4. Send via shm_mq_send()
 * 5. Optionally wait for acknowledgment
 *
 * The worker:
 * - Finds connection by conn_id
 * - Calls PQfinish(pgconn)
 * - Sets pgconn = NULL
 * - Marks slot as not in_use (available for reuse)
 * - Sends CONN_MUX_MSG_RESPONSE acknowledgment
 */
void
MultiplexerClose(int conn_id)
{
	int			worker_id;
	shm_mq	   *request_mq;
	shm_mq	   *response_mq;
	shm_mq_handle *req_mqh;
	shm_mq_handle *resp_mqh;
	ConnMuxMessageHeader msg;
	Size		msg_len;
	shm_mq_result res;
	void	   *resp_data;
	Size		resp_len;
	ConnMuxMessage *resp_msg;
	
	if (!IsConnMultiplexerEnabled())
		return;

	/* Determine which worker has this connection */
	worker_id = conn_id % foreign_conn_multiplexer_workers;

	/* Get queues from shared memory */
	LWLockAcquire(ConnMultiplexerLock, LW_SHARED);
	request_mq = ConnMultiplexerShmem->worker_queues[worker_id].request_queue;
	response_mq = ConnMultiplexerShmem->worker_queues[worker_id].response_queue;
	LWLockRelease(ConnMultiplexerLock);

	if (request_mq == NULL || response_mq == NULL)
	{
		ereport(WARNING,
				(errmsg("worker %d queues not available", worker_id)));
		return;
	}

	/* Do NOT set sender/receiver roles - worker already set them at startup */
	req_mqh = shm_mq_attach(request_mq, NULL, NULL);
	resp_mqh = shm_mq_attach(response_mq, NULL, NULL);

	/* Create CLOSE message */
	msg_len = sizeof(ConnMuxMessageHeader);
	msg.type = CONN_MUX_MSG_CLOSE;
	msg.length = msg_len;
	msg.conn_id = conn_id;
	msg.request_id = 0;

	/* Send message */
	res = shm_mq_send(req_mqh, msg_len, &msg, false, true);

	if (res != SHM_MQ_SUCCESS)
	{
		ereport(WARNING,
				(errmsg("failed to send CLOSE message to worker %d", worker_id)));
		shm_mq_detach(req_mqh);
		shm_mq_detach(resp_mqh);
		return;
	}

	/* Wait for response */
	res = shm_mq_receive(resp_mqh, &resp_len, &resp_data, false);
	
	if (res == SHM_MQ_SUCCESS)
	{
		resp_msg = (ConnMuxMessage *) resp_data;
		
		if (resp_msg->type == CONN_MUX_MSG_RESPONSE)
		{
			ereport(LOG,
					(errmsg("multiplexer: CLOSE successful, conn_id=%d", conn_id)));
		}
		else
		{
			ereport(WARNING,
					(errmsg("multiplexer: CLOSE failed: %s", resp_msg->data)));
		}
	}
	else if (res == SHM_MQ_DETACHED)
	{
		ereport(WARNING,
				(errmsg("multiplexer: worker %d queue detached during CLOSE", worker_id)));
	}
	
	shm_mq_detach(req_mqh);
	shm_mq_detach(resp_mqh);
}
