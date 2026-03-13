/*-------------------------------------------------------------------------
 *
 * conn_multiplexer.h
 *	  Connection multiplexer for distributed PostgreSQL transport
 *
 * Architecture (C10K-style distributed model):
 *
 *   Each node runs ONE multiplexer process, N local workers, and ONE networker.
 *
 *   - Local workers execute queries on the CURRENT node only (via SPI).
 *     Workers never make outbound TCP connections to any remote servers.
 *     Workers receive the query text and transaction state from the multiplexer
 *     via shm_mq, execute locally, and return results back through shm_mq.
 *
 *   - The multiplexer holds the transaction state and routes queries:
 *       * Local queries  → dispatched to an idle local worker via shm_mq.
 *       * Remote queries → signalled to the networker via MuxQuerySlot.
 *     The multiplexer never opens TCP connections itself.
 *
 *   - The networker (ConnMuxNetworkerMain, one process per node, registered in
 *     postgres_fdw) is the sole owner of all outbound TCP connections.  It
 *     maintains one persistent libpq PGconn* per registered remote server and
 *     services all remote MuxQuerySlot requests.  There is exactly ONE networker
 *     per node — not one per remote server.  Connection pooling (eviction when
 *     max_mux_connections is reached) is managed entirely by the networker.
 *
 *   Per-node process count (single node view):
 *     1 multiplexer + W local workers + 1 networker
 *     + M backend processes (one per external client)
 *
 *   Cluster-wide connection count: M + N
 *     (M external clients to any node, N persistent TCP connections total)
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

/*
 * Query request/response slot sizing for backend→multiplexer routing.
 * A backend that wants to execute a query on a foreign server via the
 * multiplexer posts a request into one of these fixed-size slots.
 */
#define MUX_MAX_QUERY_SLOTS		64		/* max concurrent foreign queries */
#define MUX_SQL_MAXLEN			4096	/* max SQL text length per request */
#define MUX_RESULT_MAXLEN		(64 * 1024) /* max serialised result (64 kB) */

/*
 * Maximum "use count" for the clock-sweep eviction algorithm used by the
 * networker to decide which remote connection to evict.  Each time a query
 * is routed through a remote connection its use_count is reset to this value.
 * The networker's clock sweep decrements it; when it reaches zero the
 * connection is eligible for eviction.
 */
#define MUX_USE_COUNT_MAX		5

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
 * One slot in the local worker pool, stored in shared memory.
 *
 * The two shm_mq queues are embedded directly in this struct so that no
 * DSM segment is needed for basic multiplexer ↔ worker communication.
 *
 * Local workers execute sub-statement queries via SPI on the local database.
 * They never make outbound TCP connections; all remote communication is
 * handled by the networker process (ConnMuxNetworkerMain).
 *
 * The multiplexer sends the query text and transaction state to the worker
 * via mux_to_worker_buf (MUX_MSG_QUERY / MUX_MSG_TXSTATE), and the worker
 * returns serialised results via worker_to_mux_buf (MUX_MSG_RESULT).
 */
typedef struct MuxWorkerSlot
{
	/* Synchronization */
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

	/* Foreign server identity (set when the server is registered) */
	Oid			server_oid;		/* pg_foreign_server.oid */
	char		server_name[NAMEDATALEN];	/* pg_foreign_server.srvname */
	char		connstr[MUX_CONNSTR_MAXLEN]; /* libpq connection string */

	/*
	 * Clock-sweep counter for the networker's connection eviction algorithm.
	 * Bumped to MUX_USE_COUNT_MAX whenever a query is routed through this
	 * connection.  The networker's sweep decrements it; when it reaches zero
	 * the connection is eligible for eviction when the pool is full.
	 */
	uint8		use_count;

	/* Statistics */
	uint64		bytes_sent;
	uint64		bytes_recv;
	uint64		msgs_sent;
	uint64		msgs_recv;
	TimestampTz connect_time;
} MuxRemoteConn;

/* ----------------------------------------------------------------
 * Networker slot in shared memory
 *
 * One ConnMuxNetworkerMain process per node (registered in postgres_fdw)
 * holds ALL persistent TCP connections to remote servers.  The multiplexer
 * wakes it when remote MuxQuerySlots are pending; the networker polls the
 * slot array, executes the query on the appropriate PGconn*, serialises the
 * result, and signals the waiting backend.
 *
 * There is exactly ONE networker per node — not one per remote server.
 * The actual PGconn* pool lives in the networker's private process memory,
 * not in shared memory.
 * ---------------------------------------------------------------- */
typedef struct MuxNetworkerSlot
{
	slock_t		mutex;

	/* Networker process identity */
	pid_t		pid;			/* networker OS PID; 0 if not running */
	Latch	   *latch;			/* pointer to networker's MyProc->procLatch */
	MuxWorkerPhase phase;

	/* Cumulative statistics */
	uint64		requests_completed;
	uint64		count_queries;
	uint64		count_errors;
} MuxNetworkerSlot;

/* ----------------------------------------------------------------
 * Query routing slot (backend → multiplexer → foreign server)
 *
 * When a backend wants to execute a query on a foreign server through the
 * multiplexer (rather than making its own TCP connection), it fills one of
 * these slots and waits for the multiplexer to complete the request.
 * ---------------------------------------------------------------- */
typedef struct MuxQuerySlot
{
	slock_t		mutex;
	bool		in_use;			/* slot is taken by a backend */

	/* ------ Request (filled by backend) ------ */
	Oid			server_oid;		/* which foreign server to query */
	char		sql[MUX_SQL_MAXLEN];	/* query text */
	pid_t		requester_pid;
	Latch	   *requester_latch; /* backend's MyProc->procLatch */

	/* ------ Result (filled by multiplexer) ------ */
	volatile bool completed;	/* set to true when result is ready */
	bool		is_error;		/* query failed */
	char		error_msg[512];	/* error text when is_error */

	/*
	 * Serialised result rows.
	 *
	 * Format: nfields int32, ntuples int32, then for each field: name as
	 * NUL-terminated string + type OID int32; then for each tuple, for each
	 * field: value_len int32 (-1 for NULL) + value bytes (not NUL-terminated).
	 */
	int			result_nfields;
	int			result_ntuples;
	bool		result_truncated; /* true if result exceeded MUX_RESULT_MAXLEN */
	char		result_data[MUX_RESULT_MAXLEN];
	int			result_len;
} MuxQuerySlot;

/* ----------------------------------------------------------------
 * Sentinel PGconn* for multiplexer-routed postgres_fdw connections.
 *
 * When the multiplexer is active for a foreign server, postgres_fdw stores
 * a pointer to a MuxConnSentinel cast to PGconn* in the connection cache
 * entry instead of a real libpq connection.  Code that needs to distinguish
 * the two cases uses IS_MUX_CONN().
 *
 * The sentinel is lightweight: it only identifies the server.  The actual
 * peer connection is managed by the multiplexer (not the backend), so there
 * is no per-backend worker-slot reference.
 * ---------------------------------------------------------------- */
#define MUX_CONN_MAGIC		0x4D555803U	/* 'M','U','X','\3' */

typedef struct MuxConnSentinel
{
	uint32		magic;			/* always MUX_CONN_MAGIC */
	Oid			server_oid;		/* foreign server this represents */
	char		server_name[NAMEDATALEN];
} MuxConnSentinel;

/*
 * Allocate a new sentinel for the given server.  The sentinel is
 * palloc'd in the current memory context.
 */
static inline MuxConnSentinel *
MuxConnSentinelCreate(Oid serverOid, const char *serverName)
{
	MuxConnSentinel *s = (MuxConnSentinel *) palloc(sizeof(MuxConnSentinel));

	s->magic = MUX_CONN_MAGIC;
	s->server_oid = serverOid;
	strlcpy(s->server_name, serverName, NAMEDATALEN);
	return s;
}

/* Test whether a PGconn* is actually a MuxConnSentinel */
#define IS_MUX_CONN(conn) \
	((conn) != NULL && \
	 ((const MuxConnSentinel *)(conn))->magic == MUX_CONN_MAGIC)

/* Extract the foreign server OID from a sentinel */
#define MUX_CONN_SRVOID(conn) \
	(((const MuxConnSentinel *)(conn))->server_oid)

/*
 * Magic value for MuxPGresult – a palloc'd result struct returned by
 * pgfdw_exec_query() when a query was executed via the multiplexer.
 * Having this constant in the shared header lets both connection.c and
 * postgres_fdw.h use IS_MUX_RESULT without a forward declaration.
 */
#define MUX_RESULT_MAGIC	0x4D555852U		/* 'M','U','X','R' */

/*
 * IS_MUX_RESULT – test whether a PGresult* is actually a MuxPGresult*
 * returned by pgfdw_exec_query.  Uses a raw uint32 cast so no MuxPGresult
 * type definition is needed at the call site.
 */
#define IS_MUX_RESULT(res) \
	((res) != NULL && \
	 *((const uint32 *)(res)) == MUX_RESULT_MAGIC)

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

	/* Local worker pool (all workers execute queries on this node only) */
	int			num_workers;		/* configured pool size (GUC) */
	MuxWorkerSlot workers[MUX_MAX_WORKERS];

	/* Remote connections (metadata; actual PGconn* lives in the networker) */
	MuxRemoteConn remote_conns[MUX_MAX_REMOTE_CONNS];

	/*
	 * Networker: the single extension process (ConnMuxNetworkerMain) that
	 * holds all outbound TCP connections.  The multiplexer wakes it by
	 * setting networker.latch when remote MuxQuerySlots are pending.
	 */
	MuxNetworkerSlot networker;

	/* Query routing slots for backend→mux foreign-server queries */
	MuxQuerySlot query_slots[MUX_MAX_QUERY_SLOTS];

	/* Global counters */
	uint64		total_requests;
	uint64		active_connections;
} MuxSharedState;

/* ----------------------------------------------------------------
 * GUC variables (defined in conn_multiplexer.c)
 * ---------------------------------------------------------------- */
extern PGDLLIMPORT int mux_worker_count;

/*
 * Maximum number of persistent remote-server connections maintained by the
 * networker.  When the pool is full and a new server needs a connection, the
 * networker's clock-sweep algorithm evicts the least-recently-used idle
 * connection.  Default: 64.
 */
extern PGDLLIMPORT int max_mux_connections;

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

/* Entry point for each local pool worker background worker */
extern PGDLLIMPORT void ConnMuxWorkerMain(Datum main_arg);

/* Wake the multiplexer from a backend or worker */
extern void ConnMuxWakeup(void);

/*
 * Statistics accessor – fills a caller-allocated array of MuxWorkerSlot
 * copies (for the stats view).  Returns the number of slots filled.
 */
extern int	ConnMuxGetWorkerStats(MuxWorkerSlot *slots, int max_slots);

/*
 * Foreign server connection routing – called from postgres_fdw and any
 * other code that wants to route through the multiplexer instead of
 * making a direct TCP connection.
 */

/*
 * Check whether the multiplexer is running.  If true, the caller should
 * register the server with ConnMuxRegisterServer and store a sentinel
 * in the connection cache entry.  All subsequent queries go through
 * ConnMuxSubmitQuery / ConnMuxSendCommand.
 */
extern bool ConnMuxIsAvailable(Oid serverOid);

/*
 * Register a foreign server with the multiplexer.  Records the server
 * metadata in MuxState->remote_conns so the networker (ConnMuxNetworkerMain)
 * can connect to it when the first query arrives.
 * connstr must be a libpq-compatible connection string.
 * Returns the conn_id on success, or (uint32) -1 on failure.
 * Safe to call from any backend.
 */
extern uint32 ConnMuxRegisterServer(Oid serverOid, const char *serverName,
									const char *connstr);

/*
 * Return a pointer to the MuxSharedState segment.  Call ConnMuxShmemInit()
 * first (which is a no-op if already initialised).  Intended for use by
 * ConnMuxNetworkerMain running inside the postgres_fdw extension.
 */
extern MuxSharedState *ConnMuxGetSharedState(void);

/*
 * Submit a query for execution on a remote node via the multiplexer.
 *
 * The backend posts the request into a MuxQuerySlot and waits.  The
 * multiplexer wakes the networker (ConnMuxNetworkerMain), which executes
 * the query on the appropriate persistent TCP connection and writes the
 * result back into the slot.
 *
 * result_data receives a compact binary stream (nfields, ntuples, field
 * descriptors, then row values).  The caller must supply a buffer of at
 * least MUX_RESULT_MAXLEN bytes.
 *
 * Returns true on success, false on error (error_msg is then filled with
 * a NUL-terminated message string).
 *
 * Blocks until the networker completes the request (or timeout occurs).
 */
extern bool ConnMuxSubmitQuery(Oid serverOid, const char *sql,
							   char *result_data, int result_data_size,
							   int *nfields_out, int *ntuples_out,
							   bool *truncated_out,
							   char *error_msg, int error_msg_size);

/*
 * Send a command (no result expected beyond success/failure) to a remote
 * server via the multiplexer.  Used for BEGIN, COMMIT, ROLLBACK, SAVEPOINT,
 * and similar control statements.
 *
 * Returns true on success, false on error (error_msg is filled).
 */
extern bool ConnMuxSendCommand(Oid serverOid, const char *sql,
							   char *error_msg, int error_msg_size);

#endif							/* CONN_MULTIPLEXER_H */
