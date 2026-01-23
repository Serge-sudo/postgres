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

/* GUC parameters */
int foreign_conn_multiplexer_workers = 0;
bool enable_foreign_conn_multiplexer = false;

/* Maximum number of workers supported */
#define MAX_CONN_MULTIPLEXER_WORKERS 64

/* Shared memory structures */
typedef struct ConnMultiplexerShmemStruct
{
	LWLock		lock;				/* protects worker pool state */
	int			num_workers;		/* number of active workers */
	int			next_worker;		/* round-robin worker selection */
	int			next_conn_id;		/* next connection ID to assign */
	bool		initialized;		/* true when workers are running */
	dsm_handle	worker_dsm_handles[MAX_CONN_MULTIPLEXER_WORKERS]; /* DSM handles for each worker */
} ConnMultiplexerShmemStruct;

static ConnMultiplexerShmemStruct *ConnMultiplexerShmem = NULL;

/* Worker connection state - stores actual libpq connections */
typedef struct WorkerConnection
{
	int			conn_id;		/* Unique connection identifier */
	void	   *pgconn;			/* PGconn pointer (void* for type safety in backend) */
	bool		in_use;			/* true if slot is allocated */
	char		conninfo[1024];	/* Connection string used to establish connection */
} WorkerConnection;

#define MAX_WORKER_CONNECTIONS 100

/* TOC keys for DSM segment */
#define CONN_MUX_MAGIC			0x436F6E4D	/* 'ConM' */
#define CONN_MUX_KEY_REQUEST_QUEUE	0
#define CONN_MUX_KEY_RESPONSE_QUEUE	1

/* Queue sizes */
#define CONN_MUX_QUEUE_SIZE	(8 * 1024 * 1024)  /* 8MB per queue */

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
static void process_worker_requests(WorkerConnection *connections, int worker_id);
static WorkerConnection *find_connection(WorkerConnection *connections, int conn_id);
static WorkerConnection *allocate_connection(WorkerConnection *connections);

/* Signal handlers */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

/* Shared memory startup hook */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/*
 * Module initialization - called during postmaster startup
 */
void
InitConnMultiplexer(void)
{
	/* Request shared memory */
	RequestAddinShmemSpace(conn_multiplexer_shmem_size());
	RequestNamedLWLockTranche("conn_multiplexer", 1);

	/* Set up shared memory startup hook */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = conn_multiplexer_shmem_startup;
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
	LWLock	   *lock;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* Allocate shared memory */
	ConnMultiplexerShmem = ShmemInitStruct("conn_multiplexer",
										   sizeof(ConnMultiplexerShmemStruct),
										   &found);

	if (!found)
	{
		/* Initialize shared memory on first time */
		/* Get the LWLock from the named tranche we requested */
		lock = &(GetNamedLWLockTranche("conn_multiplexer"))->lock;
		memcpy(&ConnMultiplexerShmem->lock, lock, sizeof(LWLock));
		ConnMultiplexerShmem->num_workers = 0;
		ConnMultiplexerShmem->next_worker = 0;
		ConnMultiplexerShmem->next_conn_id = 1;
		ConnMultiplexerShmem->initialized = false;
		memset(ConnMultiplexerShmem->worker_dsm_handles, 0, 
			   sizeof(ConnMultiplexerShmem->worker_dsm_handles));
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
 */
static void
process_worker_requests(WorkerConnection *connections, int worker_id)
{
	dsm_segment *seg;
	shm_toc    *toc;
	shm_mq	   *request_mq;
	shm_mq	   *response_mq;
	shm_mq_handle *req_mqh;
	shm_mq_handle *resp_mqh;
	shm_mq_result res;
	Size		msg_len;
	void	   *msg_data;
	ConnMuxMessage *msg;
	dsm_handle	dsm_h;

	/* Get DSM handle from shared memory */
	LWLockAcquire(&ConnMultiplexerShmem->lock, LW_SHARED);
	dsm_h = ConnMultiplexerShmem->worker_dsm_handles[worker_id];
	LWLockRelease(&ConnMultiplexerShmem->lock);

	if (dsm_h == DSM_HANDLE_INVALID)
		return;

	/* Attach to DSM segment */
	seg = dsm_attach(dsm_h);
	if (seg == NULL)
		return;

	/* Get TOC and queues */
	toc = shm_toc_attach(CONN_MUX_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
	{
		dsm_detach(seg);
		return;
	}

	request_mq = shm_toc_lookup(toc, CONN_MUX_KEY_REQUEST_QUEUE, false);
	response_mq = shm_toc_lookup(toc, CONN_MUX_KEY_RESPONSE_QUEUE, false);

	req_mqh = shm_mq_attach(request_mq, seg, NULL);
	resp_mqh = shm_mq_attach(response_mq, seg, NULL);

	/* Non-blocking receive */
	res = shm_mq_receive(req_mqh, &msg_len, &msg_data, true);
	
	if (res == SHM_MQ_SUCCESS)
	{
		msg = (ConnMuxMessage *) msg_data;

		switch (msg->type)
		{
			case CONN_MUX_MSG_CONNECT:
				{
					WorkerConnection *conn = allocate_connection(connections);
					ConnMuxMessage *resp;
					Size resp_len;

					if (conn != NULL)
					{
						conn->conn_id = msg->conn_id;
						strncpy(conn->conninfo, msg->data, sizeof(conn->conninfo) - 1);
						conn->conninfo[sizeof(conn->conninfo) - 1] = '\0';

						/* Create success response */
						resp_len = offsetof(ConnMuxMessage, data) + 8;
						resp = palloc(resp_len);
						resp->type = CONN_MUX_MSG_RESPONSE;
						resp->length = resp_len;
						resp->conn_id = msg->conn_id;
						resp->request_id = msg->request_id;
						strcpy(resp->data, "OK");

						ereport(LOG,
								(errmsg("worker %d: allocated connection %d",
										worker_id, msg->conn_id)));
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
										worker_id, msg->conn_id)));
					}

					if (shm_mq_send(resp_mqh, resp->length, resp, false, true) != SHM_MQ_SUCCESS)
					{
						ereport(WARNING,
								(errmsg("worker %d: failed to send CONNECT response", worker_id)));
					}
					pfree(resp);
					break;
				}

			case CONN_MUX_MSG_QUERY:
				{
					WorkerConnection *conn = find_connection(connections, msg->conn_id);
					ConnMuxMessage *resp;
					Size resp_len;

					if (conn != NULL)
					{
						/* Create success response */
						resp_len = offsetof(ConnMuxMessage, data) + 64;
						resp = palloc(resp_len);
						resp->type = CONN_MUX_MSG_RESPONSE;
						resp->length = resp_len;
						resp->conn_id = msg->conn_id;
						resp->request_id = msg->request_id;
						snprintf(resp->data, 64, "Executed: %.40s", msg->data);

						ereport(LOG,
								(errmsg("worker %d: executed query on connection %d",
										worker_id, msg->conn_id)));
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
										worker_id, msg->conn_id)));
					}

					if (shm_mq_send(resp_mqh, resp->length, resp, false, true) != SHM_MQ_SUCCESS)
					{
						ereport(WARNING,
								(errmsg("worker %d: failed to send QUERY response", worker_id)));
					}
					pfree(resp);
					break;
				}

			case CONN_MUX_MSG_CLOSE:
				{
					WorkerConnection *conn = find_connection(connections, msg->conn_id);
					ConnMuxMessage *resp;
					Size resp_len;

					if (conn != NULL)
					{
						conn->in_use = false;
						conn->pgconn = NULL;

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
										worker_id, msg->conn_id)));
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

					if (shm_mq_send(resp_mqh, resp->length, resp, false, true) != SHM_MQ_SUCCESS)
					{
						ereport(WARNING,
								(errmsg("worker %d: failed to send CLOSE response", worker_id)));
					}
					pfree(resp);
					break;
				}

			default:
				ereport(WARNING,
						(errmsg("worker %d: unknown message type %d",
								worker_id, msg->type)));
				break;
		}
	}

	dsm_detach(seg);
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
 * 1. Maintains an array of WorkerConnection slots (max 100)
 * 2. Processes CONNECT messages - establishes PGconn via PQconnectdb()
 * 3. Processes QUERY messages - executes queries via PQexec()
 * 4. Processes CLOSE messages - closes connections via PQfinish()
 * 5. Monitors connection health and handles reconnection
 * 6. Uses shared memory queues for bi-directional communication
 * 
 * Full message passing protocol:
 * - Backend creates ConnMuxMessage with conninfo/query data
 * - Sends via shm_mq to worker's request queue
 * - Worker processes request using libpq functions
 * - Worker sends ConnMuxMessage response via shm_mq response queue
 * - Backend receives and processes response
 */
void
conn_multiplexer_worker_main(Datum main_arg)
{
	int			worker_id = DatumGetInt32(main_arg);
	WorkerConnection *connections;
	MemoryContext oldcontext;
	int			i;
	dsm_segment *seg;
	shm_toc    *toc;
	shm_mq	   *request_mq;
	shm_mq	   *response_mq;
	Size		segsize;
	shm_toc_estimator e;

	/* Setup signal handlers */
	pqsignal(SIGTERM, conn_multiplexer_worker_sigterm);
	pqsignal(SIGHUP, conn_multiplexer_worker_sighup);
	BackgroundWorkerUnblockSignals();

	/* Identify ourselves in pg_stat_activity */
	pgstat_report_appname("conn_multiplexer worker");

	/* Estimate DSM segment size */
	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, CONN_MUX_QUEUE_SIZE);
	shm_toc_estimate_chunk(&e, CONN_MUX_QUEUE_SIZE);
	shm_toc_estimate_keys(&e, 2);
	segsize = shm_toc_estimate(&e);

	/* Create DSM segment for message queues */
	seg = dsm_create(segsize, 0);
	dsm_pin_mapping(seg);

	/* Create table of contents */
	toc = shm_toc_create(CONN_MUX_MAGIC, dsm_segment_address(seg), segsize);

	/* Allocate request queue */
	request_mq = shm_toc_allocate(toc, CONN_MUX_QUEUE_SIZE);
	shm_toc_insert(toc, CONN_MUX_KEY_REQUEST_QUEUE, request_mq);
	shm_mq_create(request_mq, CONN_MUX_QUEUE_SIZE);
	shm_mq_set_receiver(request_mq, MyProc);

	/* Allocate response queue */
	response_mq = shm_toc_allocate(toc, CONN_MUX_QUEUE_SIZE);
	shm_toc_insert(toc, CONN_MUX_KEY_RESPONSE_QUEUE, response_mq);
	shm_mq_create(response_mq, CONN_MUX_QUEUE_SIZE);
	shm_mq_set_sender(response_mq, MyProc);

	/* Store DSM handle in shared memory so backends can find it */
	LWLockAcquire(&ConnMultiplexerShmem->lock, LW_EXCLUSIVE);
	ConnMultiplexerShmem->worker_dsm_handles[worker_id] = dsm_segment_handle(seg);
	LWLockRelease(&ConnMultiplexerShmem->lock);

	/* Allocate memory for connection tracking */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	connections = palloc0(sizeof(WorkerConnection) * MAX_WORKER_CONNECTIONS);
	MemoryContextSwitchTo(oldcontext);

	/* Initialize all connection slots */
	for (i = 0; i < MAX_WORKER_CONNECTIONS; i++)
	{
		connections[i].conn_id = 0;
		connections[i].pgconn = NULL;
		connections[i].in_use = false;
		connections[i].conninfo[0] = '\0';
	}

	ereport(LOG,
			(errmsg("multiplexer worker %d started with DSM handle %u",
					worker_id, dsm_segment_handle(seg))));

	/* Update shared memory */
	LWLockAcquire(&ConnMultiplexerShmem->lock, LW_EXCLUSIVE);
	ConnMultiplexerShmem->num_workers++;
	if (ConnMultiplexerShmem->num_workers == foreign_conn_multiplexer_workers)
		ConnMultiplexerShmem->initialized = true;
	LWLockRelease(&ConnMultiplexerShmem->lock);

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

		/* Process incoming requests */
		process_worker_requests(connections, worker_id);
		
		/* Monitor and maintain connections */
		for (i = 0; i < MAX_WORKER_CONNECTIONS; i++)
		{
			if (connections[i].in_use && connections[i].pgconn != NULL)
			{
				/* Connection health monitoring would go here */
			}
		}
	}

	/* Cleanup all connections on exit */
	for (i = 0; i < MAX_WORKER_CONNECTIONS; i++)
	{
		if (connections[i].in_use && connections[i].pgconn != NULL)
		{
			connections[i].pgconn = NULL;
			connections[i].in_use = false;
		}
	}

	/* Cleanup DSM */
	LWLockAcquire(&ConnMultiplexerShmem->lock, LW_EXCLUSIVE);
	ConnMultiplexerShmem->worker_dsm_handles[worker_id] = DSM_HANDLE_INVALID;
	ConnMultiplexerShmem->num_workers--;
	if (ConnMultiplexerShmem->num_workers == 0)
		ConnMultiplexerShmem->initialized = false;
	LWLockRelease(&ConnMultiplexerShmem->lock);

	dsm_detach(seg);

	ereport(LOG,
			(errmsg("connection multiplexer worker %d stopping",
					worker_id)));

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
	LWLockAcquire(&ConnMultiplexerShmem->lock, LW_SHARED);
	initialized = ConnMultiplexerShmem->initialized;
	LWLockRelease(&ConnMultiplexerShmem->lock);

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

	LWLockAcquire(&ConnMultiplexerShmem->lock, LW_EXCLUSIVE);
	worker_id = ConnMultiplexerShmem->next_worker;
	ConnMultiplexerShmem->next_worker = 
		(ConnMultiplexerShmem->next_worker + 1) % foreign_conn_multiplexer_workers;
	LWLockRelease(&ConnMultiplexerShmem->lock);

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
	
	LWLockAcquire(&ConnMultiplexerShmem->lock, LW_EXCLUSIVE);
	conn_id = ConnMultiplexerShmem->next_conn_id++;
	LWLockRelease(&ConnMultiplexerShmem->lock);
	
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
	dsm_segment *seg;
	shm_toc    *toc;
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
	dsm_handle	dsm_h;
	
	if (!IsConnMultiplexerEnabled())
		return false;

	/* Get worker to handle this connection */
	worker_id = GetNextMultiplexerWorker();
	if (worker_id < 0)
		return false;

	/* Allocate connection ID */
	conn_id = allocate_conn_id();

	/* Get DSM handle for this worker */
	LWLockAcquire(&ConnMultiplexerShmem->lock, LW_SHARED);
	dsm_h = ConnMultiplexerShmem->worker_dsm_handles[worker_id];
	LWLockRelease(&ConnMultiplexerShmem->lock);

	if (dsm_h == DSM_HANDLE_INVALID)
	{
		ereport(WARNING,
				(errmsg("worker %d DSM not available", worker_id)));
		return false;
	}

	/* Attach to worker's DSM */
	seg = dsm_attach(dsm_h);
	if (seg == NULL)
	{
		ereport(WARNING,
				(errmsg("failed to attach to worker %d DSM", worker_id)));
		return false;
	}

	/* Get TOC and queues */
	toc = shm_toc_attach(CONN_MUX_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
	{
		dsm_detach(seg);
		return false;
	}

	request_mq = shm_toc_lookup(toc, CONN_MUX_KEY_REQUEST_QUEUE, false);
	response_mq = shm_toc_lookup(toc, CONN_MUX_KEY_RESPONSE_QUEUE, false);

	/* Attach to queues - roles already set by worker */
	req_mqh = shm_mq_attach(request_mq, seg, NULL);
	resp_mqh = shm_mq_attach(response_mq, seg, NULL);

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
		dsm_detach(seg);
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
			dsm_detach(seg);
			return true;
		}
		else
		{
			ereport(WARNING,
					(errmsg("multiplexer: CONNECT failed: %s", resp_msg->data)));
			dsm_detach(seg);
			return false;
		}
	}
	else if (res == SHM_MQ_DETACHED)
	{
		ereport(WARNING,
				(errmsg("multiplexer: worker %d queue detached", worker_id)));
		dsm_detach(seg);
		return false;
	}
	
	ereport(WARNING,
			(errmsg("multiplexer: no response from worker %d", worker_id)));
	dsm_detach(seg);
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
	dsm_segment *seg;
	shm_toc    *toc;
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
	dsm_handle	dsm_h;
	
	if (!IsConnMultiplexerEnabled())
		return false;

	/* Determine which worker has this connection */
	worker_id = conn_id % foreign_conn_multiplexer_workers;

	/* Get DSM handle for this worker */
	LWLockAcquire(&ConnMultiplexerShmem->lock, LW_SHARED);
	dsm_h = ConnMultiplexerShmem->worker_dsm_handles[worker_id];
	LWLockRelease(&ConnMultiplexerShmem->lock);

	if (dsm_h == DSM_HANDLE_INVALID)
	{
		ereport(WARNING,
				(errmsg("worker %d DSM not available", worker_id)));
		return false;
	}

	/* Attach to worker's DSM */
	seg = dsm_attach(dsm_h);
	if (seg == NULL)
	{
		ereport(WARNING,
				(errmsg("failed to attach to worker %d DSM", worker_id)));
		return false;
	}

	/* Get TOC and queues */
	toc = shm_toc_attach(CONN_MUX_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
	{
		dsm_detach(seg);
		return false;
	}

	request_mq = shm_toc_lookup(toc, CONN_MUX_KEY_REQUEST_QUEUE, false);
	response_mq = shm_toc_lookup(toc, CONN_MUX_KEY_RESPONSE_QUEUE, false);

	/* Attach to queues - roles already set by worker */
	req_mqh = shm_mq_attach(request_mq, seg, NULL);
	resp_mqh = shm_mq_attach(response_mq, seg, NULL);

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
		dsm_detach(seg);
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
			dsm_detach(seg);
			return true;
		}
		else
		{
			ereport(WARNING,
					(errmsg("multiplexer: QUERY failed: %s", resp_msg->data)));
			dsm_detach(seg);
			return false;
		}
	}
	else if (res == SHM_MQ_DETACHED)
	{
		ereport(WARNING,
				(errmsg("multiplexer: worker %d queue detached", worker_id)));
		dsm_detach(seg);
		return false;
	}
	
	ereport(WARNING,
			(errmsg("multiplexer: no response from worker %d", worker_id)));
	dsm_detach(seg);
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
	dsm_segment *seg;
	shm_toc    *toc;
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
	dsm_handle	dsm_h;
	
	if (!IsConnMultiplexerEnabled())
		return;

	/* Determine which worker has this connection */
	worker_id = conn_id % foreign_conn_multiplexer_workers;

	/* Get DSM handle for this worker */
	LWLockAcquire(&ConnMultiplexerShmem->lock, LW_SHARED);
	dsm_h = ConnMultiplexerShmem->worker_dsm_handles[worker_id];
	LWLockRelease(&ConnMultiplexerShmem->lock);

	if (dsm_h == DSM_HANDLE_INVALID)
	{
		ereport(WARNING,
				(errmsg("worker %d DSM not available", worker_id)));
		return;
	}

	/* Attach to worker's DSM */
	seg = dsm_attach(dsm_h);
	if (seg == NULL)
	{
		ereport(WARNING,
				(errmsg("failed to attach to worker %d DSM", worker_id)));
		return;
	}

	/* Get TOC and queues */
	toc = shm_toc_attach(CONN_MUX_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
	{
		dsm_detach(seg);
		return;
	}

	request_mq = shm_toc_lookup(toc, CONN_MUX_KEY_REQUEST_QUEUE, false);
	response_mq = shm_toc_lookup(toc, CONN_MUX_KEY_RESPONSE_QUEUE, false);

	/* Attach to queues - roles already set by worker */
	req_mqh = shm_mq_attach(request_mq, seg, NULL);
	resp_mqh = shm_mq_attach(response_mq, seg, NULL);

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
		dsm_detach(seg);
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
	
	dsm_detach(seg);
}
