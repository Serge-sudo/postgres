/*-------------------------------------------------------------------------
 *
 * conn_multiplexer.h
 *	  Connection multiplexer for distributed PostgreSQL transport
 *
 * Architecture:
 *
 *   Each node runs ONE multiplexer process and N strictly-local workers.
 *   There is NO separate networker process.
 *
 *   - Local workers execute queries on the CURRENT node only (via SPI).
 *     Workers never make outbound TCP connections to any remote servers.
 *     Workers receive the query text and transaction state from the multiplexer
 *     via shm_mq, execute locally, and return results back through shm_mq.
 *
 *   - The multiplexer manages ALL TCP sockets to peer multiplexers directly
 *     inside ConnMuxMain.  TCP connections are lazy: established on first use.
 *     The multiplexer listens on mux_tcp_port (default 7432) for incoming
 *     peer connections and connects outbound to peers when remote queries
 *     arrive.
 *
 *   Query routing:
 *       * Local queries  → dispatched to an idle local worker via shm_mq.
 *       * Remote queries → sent via raw TCP to the peer multiplexer.
 *     When a remote query arrives via TCP, the multiplexer dispatches it to
 *     a local worker, waits for the result, then sends it back via TCP.
 *
 *   Per-node process count (single node view):
 *     1 multiplexer + W local workers
 *     + M backend processes (one per external client)
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
 * Logging
 *
 * All multiplexer events (startup, shutdown, query requests, results,
 * worker lifecycle, TCP connections) are emitted at this level so that
 * they appear in the server log by default.
 * ---------------------------------------------------------------- */
#define MULTIPLEXER_LOG_LEVEL	WARNING

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

/* Maximum length of a peer host string stored in shared memory */
#define MUX_PEER_HOST_MAXLEN	256

/* Default peer host when none is specified in server options */
#define MUX_DEFAULT_PEER_HOST	"127.0.0.1"

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
 * TCP wire protocol for inter-multiplexer communication
 * ---------------------------------------------------------------- */

#define MUXNET_MAGIC  0x4D58544EU	/* 'M','X','T','N' */

typedef enum MuxNetMsgType
{
	MUXNET_MSG_QUERY  = 1,		/* sender→receiver: execute query */
	MUXNET_MSG_RESULT = 2,		/* receiver→sender: query result */
	MUXNET_MSG_ERROR  = 3,		/* receiver→sender: query error */
} MuxNetMsgType;

/* Fixed-size TCP message header sent before payload */
typedef struct MuxNetMsgHeader
{
	uint32		magic;			/* always MUXNET_MAGIC */
	uint32		msg_type;		/* MuxNetMsgType */
	uint32		slot_id;		/* sender's MuxQuerySlot index */
	uint32		server_oid;		/* (QUERY only) target local server OID on receiver */
	uint32		payload_len;	/* length of payload following this header */
	uint32		is_error;		/* (RESULT/ERROR) 1 if error, 0 if success */
} MuxNetMsgHeader;

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

	/* Hostname of the peer multiplexer (for outbound TCP connections) */
	char		peer_host[MUX_PEER_HOST_MAXLEN];

	/*
	 * Clock-sweep counter for the connection eviction algorithm.
	 * Bumped to MUX_USE_COUNT_MAX whenever a query is routed through this
	 * connection.  The clock sweep decrements it; when it reaches zero
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

	/* Remote connections (metadata; actual TCP sockets live in the multiplexer process) */
	MuxRemoteConn remote_conns[MUX_MAX_REMOTE_CONNS];

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
 * multiplexer.  When the pool is full and a new server needs a connection, the
 * clock-sweep algorithm evicts the least-recently-used idle connection.
 * Default: 64.
 */
extern PGDLLIMPORT int max_mux_connections;

/*
 * TCP port on which each multiplexer listens for incoming peer connections.
 * All multiplexers in a cluster must use the same port.  Default: 7432.
 */
extern PGDLLIMPORT int mux_tcp_port;

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
 * metadata in MuxState->remote_conns so the multiplexer can connect to it
 * when the first query arrives.
 * connstr must be a libpq-compatible connection string.
 * peer_host is the hostname/IP of the peer multiplexer to connect to.
 * Returns the conn_id on success, or (uint32) -1 on failure.
 * Safe to call from any backend.
 */
extern uint32 ConnMuxRegisterServer(Oid serverOid, const char *serverName,
									const char *connstr,
									const char *peer_host);

/*
 * Submit a query for execution on a remote node via the multiplexer.
 *
 * The backend posts the request into a MuxQuerySlot and waits.  The
 * multiplexer sends it via raw TCP to the peer multiplexer, which executes
 * the query via a local worker and writes the result back.
 *
 * result_data receives a compact binary stream (nfields, ntuples, field
 * descriptors, then row values).  The caller must supply a buffer of at
 * least MUX_RESULT_MAXLEN bytes.
 *
 * Returns true on success, false on error (error_msg is then filled with
 * a NUL-terminated message string).
 *
 * Blocks until the peer multiplexer completes the request (or timeout occurs).
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
