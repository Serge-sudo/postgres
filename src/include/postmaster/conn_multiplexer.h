/*-------------------------------------------------------------------------
 *
 * conn_multiplexer.h
 *		Connection multiplexer: one process per node with N local workers.
 *		Reduces total cluster connections from O(M×N) to O(M+N).
 *
 * Architecture:
 *  - One ConnMuxMain (multiplexer) process per node
 *  - N ConnMuxWorkerMain (SPI worker) processes per node (default 4)
 *  - Backends submit queries via MuxQuerySlot in shared memory + latch signal
 *  - Workers execute queries via SPI and return results via shm_mq
 *  - Inter-node communication via lazy TCP sockets to peer multiplexers
 *  - Transaction affinity: same-transaction queries go to the same worker
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 * src/include/postmaster/conn_multiplexer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONN_MULTIPLEXER_H
#define CONN_MULTIPLEXER_H

#include "libpq/pqcomm.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shm_mq.h"
#include "storage/spin.h"
#include "utils/timestamp.h"

/* -------------------------------------------------------------------------
 * Configuration limits
 * -------------------------------------------------------------------------
 */
#define MUX_MAX_WORKERS			32		/* max N workers per node */
#define MUX_DEFAULT_WORKERS		4		/* default worker count */
#define MUX_MAX_SESSIONS		256		/* max concurrent backend sessions */
#define MUX_MAX_PEERS			64		/* max peer multiplexer connections */
#define MUX_TCP_PORT_DEFAULT	7432	/* default inter-mux TCP port */
#define MUX_PROTOCOL_VERSION	196608	/* PG v3 protocol for mux clients */
#define MUX_MQ_SIZE				(128 * 1024)	/* shm_mq ring buffer size */
#define MUX_QUERY_MAX			8192	/* max query length */
#define MUX_RESULT_MAX			(256 * 1024)	/* max result size */
#define MUX_ERROR_MAX			512		/* max error message length */
#define MUX_CLOCK_SWEEP_MAX		8		/* clock-sweep hand max position */

/* -------------------------------------------------------------------------
 * Inter-mux wire protocol message types (1-byte type tag)
 * -------------------------------------------------------------------------
 */
#define MUX_MSG_QUERY		'Q'		/* query request */
#define MUX_MSG_RESULT		'R'		/* query result (data) */
#define MUX_MSG_ERROR		'E'		/* query result (error) */
#define MUX_MSG_PING		'P'		/* keepalive ping */
#define MUX_MSG_PONG		'p'		/* keepalive pong */
#define MUX_MSG_BEGIN		'B'		/* begin transaction */
#define MUX_MSG_COMMIT		'C'		/* commit transaction */
#define MUX_MSG_ABORT		'A'		/* abort transaction */

/* -------------------------------------------------------------------------
 * MuxWorkerSlot — one per local worker process
 *
 * Shared memory layout inside the slot:
 *   [MuxWorkerSlot struct][mq_request_buf][mq_response_buf]
 * The shm_mq structs live at the start of each buffer.
 * -------------------------------------------------------------------------
 */
typedef enum MuxWorkerPhase
{
	MWP_IDLE = 0,				/* waiting for a query */
	MWP_BUSY,					/* executing a query */
	MWP_SHUTDOWN,				/* shutting down */
} MuxWorkerPhase;

typedef struct MuxWorkerSlot
{
	pid_t		worker_pid;		/* PID of the worker process, 0 = not started */
	MuxWorkerPhase phase;
	int			session_id;		/* which MuxQuerySlot we're handling (-1=none) */
	TransactionId xid;			/* current transaction XID for affinity */
	int			use_count;		/* for clock-sweep idle reuse */
	bool		mq_ready;		/* true once worker has initialized the mqs */
	slock_t		mutex;			/* protects phase + session_id */

	/* shm_mq ring buffers embedded directly in the slot */
	char		mq_req_buf[MUX_MQ_SIZE];	/* backend→worker direction */
	char		mq_resp_buf[MUX_MQ_SIZE];	/* worker→mux direction */
} MuxWorkerSlot;

/* -------------------------------------------------------------------------
 * MuxQuerySlot — one per backend that wants the mux to execute a query
 * -------------------------------------------------------------------------
 */
typedef enum MuxQueryStatus
{
	MQS_FREE = 0,				/* slot available */
	MQS_PENDING,				/* backend has written query, waiting for mux */
	MQS_ASSIGNED,				/* mux has assigned a worker */
	MQS_DONE,					/* result is ready */
	MQS_ERROR,					/* error has occurred */
} MuxQueryStatus;

typedef struct MuxQuerySlot
{
	MuxQueryStatus status;		/* protected by MuxSharedState.mutex */
	pid_t		backend_pid;	/* requesting backend's PID */
	Latch	   *backend_latch;	/* latch to wake backend when done */

	/* routing info */
	Oid			server_oid;		/* InvalidOid = local; otherwise remote */
	char		server_name[NAMEDATALEN];	/* foreign server name */
	int			mux_port;		/* peer mux TCP port for this server */
	char		peer_host[256]; /* peer mux host */

	/* transaction context */
	TransactionId xid;			/* transaction XID for affinity */
	int			assigned_worker; /* worker handling this (-1 = none yet) */

	/* query payload */
	bool		is_readonly;
	int			query_len;
	char		query[MUX_QUERY_MAX];

	/* result payload */
	int			result_len;
	char		result_buf[MUX_RESULT_MAX];

	/* error payload */
	char		error_msg[MUX_ERROR_MAX];
} MuxQuerySlot;

/* -------------------------------------------------------------------------
 * MuxRemoteConn — one TCP connection to a peer multiplexer
 * Managed with a clock-sweep eviction policy.
 * -------------------------------------------------------------------------
 */
typedef struct MuxRemoteConn
{
	pgsocket	sock;			/* TCP socket to peer mux, PGINVALID_SOCKET = free */
	char		peer_host[256]; /* peer host */
	int			peer_port;		/* peer mux_port */
	TimestampTz last_used;		/* for clock-sweep */
	int			clock_mark;		/* clock-sweep hand position 0..MUX_CLOCK_SWEEP_MAX */
	int			pending_session; /* session_id waiting for response, -1 = idle */
	bool		recv_in_progress; /* partial read in progress */
	int			recv_total;		/* expected bytes for current message */
	int			recv_got;		/* bytes received so far */
	char		recv_buf[MUX_RESULT_MAX + 16]; /* receive buffer */
} MuxRemoteConn;

/* -------------------------------------------------------------------------
 * MuxSharedState — global singleton in shared memory
 * -------------------------------------------------------------------------
 */
typedef struct MuxSharedState
{
	/* Latch to wake the multiplexer when a new query slot is posted */
	Latch		mux_latch;
	pid_t		mux_pid;		/* PID of ConnMuxMain, 0 = not running */
	bool		mux_ready;		/* true once mux is listening */
	int			n_workers;		/* current mux_worker_count setting */
	slock_t		mutex;			/* protects session slots array */

	/* Worker slots — fixed array, one per possible worker */
	MuxWorkerSlot workers[MUX_MAX_WORKERS];

	/* Session slots — one per backend using the mux */
	MuxQuerySlot sessions[MUX_MAX_SESSIONS];
} MuxSharedState;

/* -------------------------------------------------------------------------
 * Public API
 * -------------------------------------------------------------------------
 */

/* Startup registration (called from postmaster before fork) */
extern void ConnMuxRegister(void);

/* Shared memory sizing + initialization */
extern Size ConnMuxShmemSize(void);
extern void ConnMuxShmemInit(void);

/* Background worker entry points (registered in InternalBGWorkers[]) */
extern void ConnMuxMain(Datum main_arg);
extern void ConnMuxWorkerMain(Datum main_arg);

/* Backend-facing API: submit a query to the mux and wait for result */
extern bool ConnMuxIsAvailable(void);
extern bool ConnMuxIsWorkerProcess(void);
extern int	ConnMuxSubmitQuery(Oid server_oid,
							   const char *server_name,
							   const char *peer_host,
							   int mux_port,
							   TransactionId xid,
							   bool is_readonly,
							   const char *query,
							   char *result_buf,
							   int result_buf_size,
							   int *result_len,
							   char *error_buf,
							   int error_buf_size);
extern bool ConnMuxAdoptSocket(pgsocket sock);

/* GUC variables */
extern PGDLLIMPORT int mux_worker_count;
extern PGDLLIMPORT int mux_tcp_port;

#endif							/* CONN_MULTIPLEXER_H */
