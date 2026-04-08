/*-------------------------------------------------------------------------
 *
 * conn_multiplexer.h
 *		Connection multiplexer: transaction-level connection pool.
 *		Reduces total cluster connections from O(M×N) to O(M+N).
 *
 * Architecture:
 *  - One ConnMuxMain process per node, listening on mux_tcp_port
 *  - Local backends connect to the mux instead of directly to the remote PG
 *  - Mux speaks the PG wire protocol during startup, then becomes a tunnel
 *  - Workers are persistent PG sessions on the remote node, pooled by
 *    (database, username).  Multiple backends share workers; each backend
 *    holds a worker exclusively only while it has an open transaction.
 *  - Inter-mux protocol: control messages (TX_CONNECT / TX_BEGIN / TX_END /
 *    TX_DISCONNECT) on each per-backend TCP socket; raw PG bytes tunneled
 *    during a transaction.
 *
 * Control message wire format (all integers big-endian):
 *   [type 1B][channel_id 4B][payload_len 4B][payload payload_len B]
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
#include "storage/spin.h"
#include "utils/timestamp.h"

/* -------------------------------------------------------------------------
 * Configuration limits
 * -------------------------------------------------------------------------
 */
#define MUX_MAX_WORKERS			32		/* max worker PG sessions per node */
#define MUX_DEFAULT_WORKERS		4		/* default worker count GUC */
#define MUX_MAX_CHANNELS		256		/* max concurrent backend channels */
#define MUX_MAX_CTRL_CONNS		64		/* max incoming ctrl connections */
#define MUX_TCP_PORT_DEFAULT	7432	/* default inter-mux TCP port */
#define MUX_PG_PROTOCOL_V3		196608	/* PG v3 startup packet marker */
#define MUX_CLOCK_SWEEP_MAX		8		/* clock-sweep eviction threshold */
#define MUX_STARTUP_MAX			8192	/* max PG startup packet bytes */
#define MUX_PROXY_BUF			32768	/* tunnel data buffer size */
#define MUX_STARTUP_RESP_MAX	4096	/* max stored startup response bytes */
#define MUX_CTRL_HDR_LEN		9		/* type(1)+channel_id(4)+payload_len(4) */
#define MUX_CTRL_PAYLOAD_MAX	(MUX_STARTUP_RESP_MAX + 64)

/* -------------------------------------------------------------------------
 * Inter-mux control protocol message types (1-byte type tag, must be != 0
 * so they are not confused with the first byte of a PG startup packet)
 * -------------------------------------------------------------------------
 */
#define MUX_MSG_CONNECT			'C'		/* local→remote: request worker for (db,user) */
#define MUX_MSG_CONNECT_OK		'c'		/* remote→local: worker assigned, startup resp */
#define MUX_MSG_CONNECT_FAIL	'!'		/* remote→local: no free slot, fall back */
#define MUX_MSG_DISCONNECT		'D'		/* local→remote: backend fully done */
#define MUX_MSG_TX_BEGIN		'B'		/* local→remote: claim worker for a transaction */
#define MUX_MSG_TX_BEGIN_OK		'b'		/* remote→local: worker reserved, tunnel open */
#define MUX_MSG_TX_BEGIN_WAIT	'w'		/* remote→local: no idle worker, retry later */
#define MUX_MSG_TX_END			'E'		/* local→remote: transaction done, release */
#define MUX_MSG_PING			'P'		/* either direction: keepalive */
#define MUX_MSG_PONG			'p'		/* either direction: keepalive reply */

/* -------------------------------------------------------------------------
 * MuxWorkerSlot — one persistent PG session on the remote node.
 * Managed entirely in process-local memory of ConnMuxMain.
 * -------------------------------------------------------------------------
 */
typedef struct MuxWorkerSlot
{
	pgsocket	worker_sock;					/* TCP socket to PG backend session */
	char		database[NAMEDATALEN];			/* logged-in database */
	char		username[NAMEDATALEN];			/* logged-in user */
	int			connect_cnt;					/* # channels sharing this worker */
	bool		in_tx;							/* true = exclusively held for a TX */
	int32		active_channel;				/* channel_id holding the TX, -1 if idle */
	/* stored PG startup response to replay for newly connected channels */
	char		startup_resp[MUX_STARTUP_RESP_MAX];
	int			startup_resp_len;
} MuxWorkerSlot;

/* -------------------------------------------------------------------------
 * MuxChannelState — state machine for a local-mux channel (one per backend)
 * -------------------------------------------------------------------------
 */
typedef enum MuxChannelState
{
	MCH_EMPTY = 0,		/* free slot */
	MCH_STARTUP,		/* reading PG startup packet from backend */
	MCH_CONNECTING,		/* sent CONNECT, waiting for CONNECT_OK/FAIL */
	MCH_READY,			/* connected, between transactions */
	MCH_TX_PENDING,		/* sent TX_BEGIN, waiting for TX_BEGIN_OK/WAIT */
	MCH_IN_TX,			/* tunnel mode: raw PG bytes flowing */
} MuxChannelState;

/* -------------------------------------------------------------------------
 * MuxChannelSlot — one per backend connected to the local mux side.
 * Managed entirely in process-local memory of ConnMuxMain.
 * -------------------------------------------------------------------------
 */
typedef struct MuxChannelSlot
{
	int32			channel_id;
	MuxChannelState	state;

	char			database[NAMEDATALEN];
	char			username[NAMEDATALEN];
	char			target_host[256];		/* mux_target_host from startup */
	int32			target_port;			/* mux_target_port from startup */
	int32			target_mux_port;		/* mux_target_mux_port from startup (remote mux port) */

	pgsocket		backend_sock;			/* TCP socket to local backend */
	pgsocket		ctrl_sock;				/* TCP socket to remote mux */

	/* startup data: reading PG startup packet from backend */
	char			startup_buf[MUX_STARTUP_MAX];
	int				startup_len;			/* total expected length (0 = not yet known) */
	int				startup_off;			/* bytes read so far */

	/* control response reading (CONNECT_OK/FAIL etc.) */
	char			ctrl_hdr[MUX_CTRL_HDR_LEN];
	int				ctrl_hdr_off;			/* bytes of ctrl header read so far */
	char		   *ctrl_payload_buf;		/* palloc'd, NULL if no payload yet */
	int				ctrl_payload_len;		/* expected payload length */
	int				ctrl_payload_off;		/* bytes of payload read so far */

	/* tunnel data buffers */
	char			c2r_buf[MUX_PROXY_BUF];	/* backend→remote */
	int				c2r_len;
	int				c2r_off;
	char			r2c_buf[MUX_PROXY_BUF];	/* remote→backend */
	int				r2c_len;
	int				r2c_off;

	/* ReadyForQuery 'I' scanner state (for TX_END detection) */
	int				rfq_scan_pos;			/* bytes matched in RFQ 'I' pattern */
	bool			pending_tx_end;			/* RFQ 'I' seen; need to send TX_END */
} MuxChannelSlot;

/* -------------------------------------------------------------------------
 * MuxCtrlConnState — state of an incoming control connection (remote side)
 * -------------------------------------------------------------------------
 */
typedef enum MuxCtrlConnState
{
	MCC_EMPTY = 0,
	MCC_READING_HDR,	/* reading 9-byte control message header */
	MCC_READING_PAYLOAD,/* reading variable-length payload */
	MCC_IN_TX,			/* tunnel mode: forwarding raw PG bytes */
} MuxCtrlConnState;

/* -------------------------------------------------------------------------
 * MuxCtrlConn — one incoming control connection from a peer mux.
 * Managed entirely in process-local memory of ConnMuxMain.
 * -------------------------------------------------------------------------
 */
typedef struct MuxCtrlConn
{
	pgsocket		ctrl_sock;
	MuxCtrlConnState state;

	/* control message header being read */
	char			hdr_buf[MUX_CTRL_HDR_LEN];
	int				hdr_off;
	char			msg_type;
	int32			channel_id;
	int				payload_len;
	char			payload_buf[MUX_CTRL_PAYLOAD_MAX];
	int				payload_off;

	/* assigned worker for this channel (set at TX_CONNECT) */
	int				worker_idx;			/* index into mux_workers[], -1 if none */

	/* database/user for this channel */
	char			database[NAMEDATALEN];
	char			username[NAMEDATALEN];

	/* tunnel buffers (used when state == MCC_IN_TX) */
	char			c2w_buf[MUX_PROXY_BUF];	/* ctrl→worker */
	int				c2w_len;
	int				c2w_off;
	char			w2c_buf[MUX_PROXY_BUF];	/* worker→ctrl */
	int				w2c_len;
	int				w2c_off;

	/* ReadyForQuery 'I' scanner state (for TX_END detection on remote side) */
	int				rfq_scan_pos;
	bool			rfq_detected;	/* RFQ 'I' seen; flush w2c_buf before exiting tunnel */

	/* outbound control message write buffer */
	char			send_buf[MUX_CTRL_HDR_LEN + MUX_CTRL_PAYLOAD_MAX];
	int				send_len;
	int				send_off;
} MuxCtrlConn;

/* -------------------------------------------------------------------------
 * MuxSharedState — minimal shared-memory singleton.
 * In the new architecture the heavy data lives in process-local memory of
 * ConnMuxMain; shared memory is only used so backends can check mux_ready.
 * -------------------------------------------------------------------------
 */
typedef struct MuxSharedState
{
	Latch		mux_latch;		/* kept for potential future use / compat */
	pid_t		mux_pid;		/* PID of ConnMuxMain, 0 = not running */
	bool		mux_ready;		/* true once mux is listening */
	slock_t		mutex;
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

/* Background worker entry point */
extern void ConnMuxMain(Datum main_arg);

/* Backend-facing checks */
extern bool ConnMuxIsAvailable(void);
extern bool ConnMuxIsWorkerProcess(void);

/* GUC variables */
extern PGDLLIMPORT bool enable_multiplexer;
extern PGDLLIMPORT int mux_worker_count;
extern PGDLLIMPORT int mux_tcp_port;

#endif							/* CONN_MULTIPLEXER_H */
