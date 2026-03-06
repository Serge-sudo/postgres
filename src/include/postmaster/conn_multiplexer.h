/*-------------------------------------------------------------------------
 *
 * conn_multiplexer.h
 *	  Connection multiplexer for distributed PostgreSQL transport
 *
 * The connection multiplexer implements a new transport and connection model
 * for distributed PostgreSQL. Each node runs one multiplexer process that:
 *  - Connects to peer multiplexers via a single connection per remote node
 *  - Maintains a pool of workers that execute sub-statement level queries
 *  - Communicates with workers and backends via shared-memory queues
 *
 * This dramatically reduces the total number of connections and processes
 * in a cluster compared to the traditional per-connection model.
 *
 * Architecture:
 *   Total connections = M + N  (M = external clients, N = nodes)
 *   Total processes   = M + N*W (W = workers per node)
 *
 * The multiplexer event loop moves data between:
 *   - Local backends ↔ local workers (via shm_mq)
 *   - Local workers  ↔ remote nodes  (via single TCP connection per node)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 * src/include/postmaster/conn_multiplexer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONN_MULTIPLEXER_H
#define CONN_MULTIPLEXER_H

#include "storage/latch.h"
#include "storage/shm_mq.h"
#include "storage/spin.h"
#include "utils/timestamp.h"

/* ----------------------------------------------------------------
 * Configuration constants
 * ---------------------------------------------------------------- */

/* Maximum number of worker slots per multiplexer */
#define MUX_MAX_WORKERS			64

/* Size of each per-worker shared-memory message queue (bytes) */
#define MUX_QUEUE_SIZE			(64 * 1024)		/* 64 kB */

/* Maximum number of simultaneous remote-node connections */
#define MUX_MAX_REMOTE_CONNS	64

/* Maximum length of a connection string stored in shared memory */
#define MUX_CONNSTR_MAXLEN		256

/* ----------------------------------------------------------------
 * Message types exchanged through the shared-memory queues
 * ---------------------------------------------------------------- */
typedef enum MuxMessageType
{
	MUX_MSG_QUERY = 1,			/* backend → worker: execute a query */
	MUX_MSG_RESULT,				/* worker  → backend: query result */
	MUX_MSG_CLOSE,				/* either direction: close/cancel request */
	MUX_MSG_TXSTATE,			/* coordinator → worker: transaction state */
	MUX_MSG_ERROR,				/* worker  → backend: error report */
} MuxMessageType;

/*
 * Header prepended to every message placed in a shm_mq queue.
 * The message payload immediately follows this struct.
 */
typedef struct MuxMsgHeader
{
	MuxMessageType msg_type;	/* message discriminator */
	uint32		conn_id;		/* logical connection / request identifier */
	uint32		payload_len;	/* bytes of payload following this header */
	int32		requester_pid;	/* PID of the requesting backend */
} MuxMsgHeader;

/* ----------------------------------------------------------------
 * Per-worker slot in shared memory
 * ---------------------------------------------------------------- */
typedef enum MuxWorkerPhase
{
	MUX_WORKER_DEAD = 0,		/* slot not in use / worker not running */
	MUX_WORKER_STARTING,		/* worker registered, not yet ready */
	MUX_WORKER_IDLE,			/* worker ready and waiting for work */
	MUX_WORKER_BUSY,			/* worker currently executing a request */
} MuxWorkerPhase;

/*
 * One slot in the worker pool, stored in shared memory.
 *
 * The two shm_mq queues are embedded directly in this struct so that no
 * DSM segment is needed for basic multiplexer ↔ worker communication.
 */
typedef struct MuxWorkerSlot
{
	/* Synchronisation */
	slock_t		mutex;

	/* Identity */
	int			worker_id;		/* 0-based index in the pool */
	pid_t		pid;			/* worker OS PID; 0 if not running */

	/* State */
	MuxWorkerPhase phase;

	/* Current request being processed */
	MuxMessageType current_request_type;
	uint32		current_conn_id;	/* conn_id of the active request */
	int32		requester_pid;	/* PID of the backend that sent the request */

	/* Cumulative statistics */
	uint64		requests_completed;
	uint64		count_queries;
	uint64		count_connects;
	uint64		count_closes;
	uint64		count_errors;
	TimestampTz last_active;		/* time of last request start */

	/* Latches for wake-up signalling */
	Latch	   *worker_latch;	/* pointer to worker's MyProc->procLatch */

	/*
	 * Shared-memory queues.
	 *
	 * mux_to_worker: multiplexer writes job requests; worker reads them.
	 * worker_to_mux: worker writes results; multiplexer reads them.
	 *
	 * Each queue consists of a shm_mq header followed by the ring buffer.
	 * The total memory for both queues is MUX_QUEUE_SIZE * 2 bytes.
	 */
	char		mux_to_worker_buf[MUX_QUEUE_SIZE];
	char		worker_to_mux_buf[MUX_QUEUE_SIZE];
} MuxWorkerSlot;

/* ----------------------------------------------------------------
 * Per-remote-connection slot in shared memory
 * ---------------------------------------------------------------- */
typedef enum MuxConnPhase
{
	MUX_CONN_UNUSED = 0,		/* slot available */
	MUX_CONN_CONNECTING,		/* TCP handshake / authentication in progress */
	MUX_CONN_ACTIVE,			/* fully connected */
	MUX_CONN_ERROR,				/* connection failed or was lost */
} MuxConnPhase;

typedef struct MuxRemoteConn
{
	slock_t		mutex;
	MuxConnPhase phase;
	uint32		conn_id;		/* identifier used in MuxMsgHeader */
	char		connstr[MUX_CONNSTR_MAXLEN]; /* libpq connection string */

	/* Statistics */
	uint64		bytes_sent;
	uint64		bytes_recv;
	uint64		msgs_sent;
	uint64		msgs_recv;
	TimestampTz connect_time;
} MuxRemoteConn;

/* ----------------------------------------------------------------
 * Global multiplexer shared-memory state
 * ---------------------------------------------------------------- */
typedef struct MuxSharedState
{
	/* Spinlock protecting global fields */
	slock_t		mutex;

	/* PID of the multiplexer process (0 if not running) */
	pid_t		mux_pid;

	/* Latch of the multiplexer process for wake-up from workers/backends */
	Latch	   *mux_latch;

	/* Worker pool */
	int			num_workers;		/* configured pool size (GUC) */
	MuxWorkerSlot workers[MUX_MAX_WORKERS];

	/* Remote connections */
	MuxRemoteConn remote_conns[MUX_MAX_REMOTE_CONNS];

	/* Global counters */
	uint64		total_requests;
	uint64		active_connections;
} MuxSharedState;

/* ----------------------------------------------------------------
 * GUC variables (defined in conn_multiplexer.c)
 * ---------------------------------------------------------------- */
extern PGDLLIMPORT int mux_worker_count;

/* ----------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------- */

/* Shared memory sizing / initialisation (called from ipci.c) */
extern Size ConnMuxShmemSize(void);
extern void ConnMuxShmemInit(void);

/* Register the multiplexer background worker (called from postmaster.c) */
extern void ConnMuxRegister(void);

/* Entry point for the multiplexer background worker */
extern PGDLLIMPORT void ConnMuxMain(Datum main_arg);

/* Entry point for each pool worker background worker */
extern PGDLLIMPORT void ConnMuxWorkerMain(Datum main_arg);

/* Wake the multiplexer from a backend or worker */
extern void ConnMuxWakeup(void);

/*
 * Statistics accessor – fills a caller-allocated array of MuxWorkerSlot
 * copies (for the stats view).  Returns the number of slots filled.
 */
extern int	ConnMuxGetWorkerStats(MuxWorkerSlot *slots, int max_slots);

#endif							/* CONN_MULTIPLEXER_H */
