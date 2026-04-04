/*-------------------------------------------------------------------------
 *
 * conn_multiplexer.c
 * Connection multiplexer: one process per node, N strictly local workers.
 *
 * Reduces total cluster TCP connections from O(M×N) to O(M+N):
 *   M  = external client connections to this node
 *   N  = inter-node TCP sockets (one per peer node, held by ConnMuxMain)
 *   N×W = local worker processes (W = mux_worker_count, default 4)
 *
 * Data flow for a remote query:
 *   Backend writes query to MuxQuerySlot, signals mux_latch.
 *   ConnMuxMain wakes, opens (or reuses) TCP socket to peer mux.
 *   Peer ConnMuxMain routes to a local SPI worker via shm_mq.
 *   Worker executes via SPI, writes result to shm_mq.
 *   Peer mux reads result, sends back over TCP.
 *   Local mux writes result to MuxQuerySlot, wakes backend latch.
 *
 * Data flow for a local query (server_oid == InvalidOid):
 *   Backend writes query to MuxQuerySlot, signals mux_latch.
 *   ConnMuxMain picks an idle (or transaction-pinned) worker.
 *   Worker executes via SPI, writes result to shm_mq.
 *   Mux reads result, wakes backend latch.
 *
 * Inter-mux TCP wire protocol (all integers big-endian):
 *   Request:  [type 'Q'][session_id 4B][xid 4B][rdonly 1B][qlen 4B][query qlen B]
 *   Response: [type 'R' or 'E'][session_id 4B][rlen 4B][result/errmsg rlen B]
 *   Ping/Pong: [type 'P'/'p']
 *
 * Result buffer format (for 'R' responses):
 *   [ncols 4B][nrows 4B]
 *   For each column: [namelen 2B][name namelen B][typid 4B]
 *   For each row, for each col: [vlen 4B][value vlen B]  (vlen=-1 for NULL)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *  src/backend/postmaster/conn_multiplexer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/types.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "port.h"
#include "postmaster/bgworker.h"
#include "postmaster/conn_multiplexer.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/shm_mq.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#ifndef MULTIPLEXER_LOG_LEVEL
#define MULTIPLEXER_LOG_LEVEL WARNING
#endif

#define MUX_LOG(fmt, ...)          \
	ereport(MULTIPLEXER_LOG_LEVEL, \
			(errmsg_internal("multiplexer: " fmt, ##__VA_ARGS__)))

/* -------------------------------------------------------------------------
 * GUC variables
 * -------------------------------------------------------------------------
 */
int mux_worker_count = MUX_DEFAULT_WORKERS;
int mux_tcp_port = MUX_TCP_PORT_DEFAULT;

/* -------------------------------------------------------------------------
 * Shared state pointer (set once at shmem init)
 * -------------------------------------------------------------------------
 */
MuxSharedState *MuxState = NULL;
static uint32 mux_we_wait = 0;

/* -------------------------------------------------------------------------
 * Local (per-process) state for ConnMuxMain
 * -------------------------------------------------------------------------
 */
static pgsocket mux_listen_sock = PGINVALID_SOCKET; /* TCP server socket */
static MuxRemoteConn mux_peers[MUX_MAX_PEERS];		/* peer connections */
static int mux_n_peers = 0;
static pgsocket mux_pending[MUX_MAX_PEERS];
static int mux_pending_count = 0;
static bool mux_pending_adopted[MUX_MAX_PEERS];

#define MUX_PROXY_MAX MUX_MAX_SESSIONS
#define MUX_PROXY_BUF 32768
#define MUX_STARTUP_MAX 8192
#define MUX_REMOTE_QUERY_TIMEOUT_MS 60000
#define MUX_CHAIN_STARTUP_OPT "-c mux_chain=1"

typedef enum MuxProxyState
{
	MPX_EMPTY = 0,
	MPX_STARTUP,
	MPX_ACTIVE
} MuxProxyState;

typedef struct MuxProxyConn
{
	pgsocket client_sock;
	pgsocket remote_sock;
	MuxProxyState state;

	int startup_len;
	int startup_off;
	char startup_buf[MUX_STARTUP_MAX];

	int c2r_len;
	int c2r_off;
	char c2r_buf[MUX_PROXY_BUF];

	int r2c_len;
	int r2c_off;
	char r2c_buf[MUX_PROXY_BUF];
} MuxProxyConn;

static MuxProxyConn mux_proxies[MUX_PROXY_MAX];

/* shm_mq handles maintained by the mux (one pair per worker) */
static shm_mq_handle *mux_worker_req_handle[MUX_MAX_WORKERS];
static shm_mq_handle *mux_worker_resp_handle[MUX_MAX_WORKERS];

/* WaitEventSet user_data encoding */
#define MUX_EVENT_KIND_SHIFT 16
#define MUX_EVENT_KIND_MASK 0xFFFF0000
#define MUX_EVENT_IDX_MASK 0x0000FFFF
#define MUX_EVENT(kind, idx) \
	((void *)(intptr_t)(((kind) << MUX_EVENT_KIND_SHIFT) | (idx)))
#define MUX_EVENT_KIND(val) \
	(((int)(intptr_t)(val)) >> MUX_EVENT_KIND_SHIFT)
#define MUX_EVENT_IDX(val) \
	(((int)(intptr_t)(val)) & MUX_EVENT_IDX_MASK)

enum
{
	MUX_EVENT_PEER = 1,
	MUX_EVENT_PENDING = 2,
	MUX_EVENT_PROXY_CLIENT = 3,
	MUX_EVENT_PROXY_REMOTE = 4
};

/* Forward declarations */
static void ConnMuxSetupListenSocket(void);
static void ConnMuxEventLoop(void);
static void ConnMuxProcessNewSessions(void);
static void ConnMuxRouteToWorker(int session_id, int worker_id);
static void ConnMuxRouteToRemote(int session_id);
static int ConnMuxFindWorker(int session_id);
static void ConnMuxPollWorkerResponses(void);
static void ConnMuxAcceptPeer(void);
static void ConnMuxReadPeerData(int peer_idx);
static pgsocket ConnMuxOpenPeerSocket(const char *host, int port);
static int ConnMuxFindOrOpenPeer(const char *host, int port);
static void ConnMuxCloseIdle(void);
static void ConnMuxCompleteSession(int session_id, const char *result,
								   int result_len, const char *error);
static void ConnMuxSignalWorker(int worker_id);
static void ConnMuxSpawnWorker(int worker_id);
static void ConnMuxHandleRemoteQuery(pgsocket client_sock,
									 int remote_session_id,
									 TransactionId xid,
									 bool is_readonly,
									 const char *query, int qlen);
static bool ConnMuxAttachWorkerHandles(int worker_id);
static void ConnMuxInitPendingSockets(void);
static void ConnMuxInitProxies(void);
static void ConnMuxAddPendingSocket(pgsocket sock, bool adopted);
static void ConnMuxClassifyPending(int idx);
static void ConnMuxProxyClientEvent(int idx, uint32 events);
static void ConnMuxProxyRemoteEvent(int idx, uint32 events);
static void ConnMuxProxyClose(int idx);
static bool ConnMuxProxyReadStartup(int idx);
static bool ConnMuxProxyParseStartup(MuxProxyConn *proxy,
						 char *target_host, Size host_len,
						 int *target_port,
						 char *connect_host, Size connect_host_len,
						 int *connect_port,
						 bool *is_mux_chain,
						 char **startup_forward,
						 int *startup_forward_len);
static void ConnMuxReadPeerData(int peer_idx);
static void ConnMuxPumpSocketsOnce(void);

/* =========================================================================
 * Wire protocol helpers
 * ========================================================================= */

static bool
mux_write_all(pgsocket sock, const void *buf, int len)
{
	const char *p = (const char *)buf;
	int remaining = len;

	while (remaining > 0)
	{
		CHECK_FOR_INTERRUPTS();

		int n = send(sock, p, remaining, 0);

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			return false;
		}
		p += n;
		remaining -= n;
	}
	return true;
}

static bool
mux_read_all(pgsocket sock, void *buf, int len)
{
	char *p = (char *)buf;
	int remaining = len;

	while (remaining > 0)
	{
		CHECK_FOR_INTERRUPTS();

		int n = recv(sock, p, remaining, 0);

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			if (errno == EAGAIN || errno == EWOULDBLOCK)
			{
				pg_usleep(1000);
				continue;
			}
			return false;
		}
		if (n == 0)
			return false;
		p += n;
		remaining -= n;
	}
	return true;
}

static void
put_be32(char *p, uint32 v)
{
	p[0] = (v >> 24) & 0xFF;
	p[1] = (v >> 16) & 0xFF;
	p[2] = (v >> 8) & 0xFF;
	p[3] = v & 0xFF;
}

static uint32
get_be32(const char *p)
{
	return ((uint32)(unsigned char)p[0] << 24) |
		   ((uint32)(unsigned char)p[1] << 16) |
		   ((uint32)(unsigned char)p[2] << 8) |
		   ((uint32)(unsigned char)p[3]);
}

static void
put_be16(char *p, uint16 v)
{
	p[0] = (v >> 8) & 0xFF;
	p[1] = v & 0xFF;
}

static uint16
get_be16(const char *p)
{
	return ((uint16)(unsigned char)p[0] << 8) |
		   ((uint16)(unsigned char)p[1]);
}

/* =========================================================================
 * Shared memory
 * ========================================================================= */

Size ConnMuxShmemSize(void)
{
	return MAXALIGN(sizeof(MuxSharedState));
}

void ConnMuxShmemInit(void)
{
	bool found;

	MuxState = (MuxSharedState *)
		ShmemInitStruct("Connection Multiplexer Data",
						ConnMuxShmemSize(),
						&found);
	if (!found)
	{
		int i;

		MemSet(MuxState, 0, sizeof(MuxSharedState));
		InitSharedLatch(&MuxState->mux_latch);
		SpinLockInit(&MuxState->mutex);
		MuxState->mux_pid = 0;
		MuxState->mux_ready = false;
		MuxState->n_workers = 0;

		for (i = 0; i < MUX_MAX_WORKERS; i++)
		{
			MuxWorkerSlot *ws = &MuxState->workers[i];

			SpinLockInit(&ws->mutex);
			ws->worker_pid = 0;
			ws->phase = MWP_IDLE;
			ws->session_id = -1;
			ws->xid = InvalidTransactionId;
			ws->use_count = 0;
			ws->mq_ready = false;
		}

		for (i = 0; i < MUX_MAX_SESSIONS; i++)
		{
			MuxState->sessions[i].status = MQS_FREE;
			MuxState->sessions[i].assigned_worker = -1;
		}
	}
}

/* =========================================================================
 * ConnMuxRegister — called from postmaster at startup
 * ========================================================================= */

void ConnMuxRegister(void)
{
	BackgroundWorker bgw;

	if (mux_worker_count <= 0)
		return;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_PostmasterStart;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ConnMuxMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "connection multiplexer");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "connection multiplexer");
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum)0;

	RegisterBackgroundWorker(&bgw);
}

/* =========================================================================
 * ConnMuxIsAvailable — can be called from any backend
 * ========================================================================= */

bool ConnMuxIsAvailable(void)
{
	if (MuxState == NULL)
		return false;
	return MuxState->mux_ready;
}

bool
ConnMuxIsWorkerProcess(void)
{
	int i;

	if (MuxState == NULL)
		return false;

	for (i = 0; i < mux_worker_count; i++)
	{
		if (MuxState->workers[i].worker_pid == MyProcPid)
			return true;
	}

	return false;
}

/* =========================================================================
 * ConnMuxSubmitQuery — backend API to submit a query to the mux
 *
 * Returns 0 on success, -1 on error.
 * ========================================================================= */

int ConnMuxSubmitQuery(Oid server_oid,
					   const char *server_name,
					   const char *peer_host,
					   int mux_port_param,
					   TransactionId xid,
					   bool is_readonly,
					   const char *query,
					   char *result_buf,
					   int result_buf_size,
					   int *result_len,
					   char *error_buf,
					   int error_buf_size)
{
	int slot_id = -1;
	int i;
	MuxQuerySlot *slot;

	if (MuxState == NULL || !MuxState->mux_ready)
	{
		if (error_buf)
			snprintf(error_buf, error_buf_size, "multiplexer not available");
		return -1;
	}

	/* Allocate a free session slot */
	SpinLockAcquire(&MuxState->mutex);
	for (i = 0; i < MUX_MAX_SESSIONS; i++)
	{
		if (MuxState->sessions[i].status == MQS_FREE)
		{
			slot_id = i;
			MuxState->sessions[i].status = MQS_PENDING;
			break;
		}
	}
	SpinLockRelease(&MuxState->mutex);

	if (slot_id < 0)
	{
		if (error_buf)
			snprintf(error_buf, error_buf_size, "no free multiplexer session slots");
		return -1;
	}

	slot = &MuxState->sessions[slot_id];
	slot->backend_pid = MyProcPid;
	slot->backend_latch = &MyProc->procLatch;
	slot->server_oid = server_oid;
	if (server_name)
		strlcpy(slot->server_name, server_name, sizeof(slot->server_name));
	else
		slot->server_name[0] = '\0';
	if (peer_host)
		strlcpy(slot->peer_host, peer_host, sizeof(slot->peer_host));
	else
		slot->peer_host[0] = '\0';
	slot->mux_port = mux_port_param;
	slot->xid = xid;
	slot->is_readonly = is_readonly;
	slot->assigned_worker = -1;
	slot->result_len = 0;
	slot->error_msg[0] = '\0';

	if (strlen(query) >= MUX_QUERY_MAX)
	{
		SpinLockAcquire(&MuxState->mutex);
		slot->status = MQS_FREE;
		SpinLockRelease(&MuxState->mutex);
		if (error_buf)
			snprintf(error_buf, error_buf_size, "query too long for multiplexer");
		return -1;
	}
	slot->query_len = strlen(query);
	memcpy(slot->query, query, slot->query_len + 1);

	/* Signal the multiplexer */
	SetLatch(&MuxState->mux_latch);

	/* Wait for the result */
	for (;;)
	{
		MuxQueryStatus st;

		if (mux_we_wait == 0)
			mux_we_wait = WaitEventExtensionNew("ConnMuxWait");

		(void)WaitLatch(&MyProc->procLatch,
						WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						1000,
						mux_we_wait);
		ResetLatch(&MyProc->procLatch);

		SpinLockAcquire(&MuxState->mutex);
		st = slot->status;
		SpinLockRelease(&MuxState->mutex);

		if (st == MQS_DONE)
		{
			int safe_result_buf_size = (result_buf_size > 0) ? result_buf_size - 1 : 0;
			int copy_len = Min(slot->result_len, safe_result_buf_size);

			if (result_buf && copy_len > 0)
				memcpy(result_buf, slot->result_buf, copy_len);
			if (result_buf && result_buf_size > 0)
				result_buf[copy_len] = '\0';
			if (result_len)
				*result_len = slot->result_len;

			SpinLockAcquire(&MuxState->mutex);
			slot->status = MQS_FREE;
			SpinLockRelease(&MuxState->mutex);
			return 0;
		}
		else if (st == MQS_ERROR)
		{
			if (error_buf)
				strlcpy(error_buf, slot->error_msg, error_buf_size);

			SpinLockAcquire(&MuxState->mutex);
			slot->status = MQS_FREE;
			SpinLockRelease(&MuxState->mutex);
			return -1;
		}

		CHECK_FOR_INTERRUPTS();
	}
}

/* =========================================================================
 * ConnMuxMain — main loop of the multiplexer process
 * ========================================================================= */

void ConnMuxMain(Datum main_arg)
{
	int i;

	pqsignal(SIGTERM, die);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	Assert(MuxState != NULL);
	OwnLatch(&MuxState->mux_latch);
	MuxState->mux_pid = MyProcPid;

	for (i = 0; i < MUX_MAX_PEERS; i++)
	{
		mux_peers[i].sock = PGINVALID_SOCKET;
		mux_peers[i].pending_session = -1;
		mux_peers[i].clock_mark = 0;
		mux_pending_adopted[i] = false;
	}

	ConnMuxInitPendingSockets();
	ConnMuxInitProxies();

	for (i = 0; i < MUX_MAX_WORKERS; i++)
	{
		mux_worker_req_handle[i] = NULL;
		mux_worker_resp_handle[i] = NULL;
	}

	ConnMuxSetupListenSocket();

	MuxState->n_workers = mux_worker_count;

	for (i = 0; i < mux_worker_count; i++)
		ConnMuxSpawnWorker(i);

	MuxState->mux_ready = true;

	elog(LOG, "connection multiplexer started on TCP port %d with %d workers",
		 mux_tcp_port, mux_worker_count);

	set_ps_display("idle");

	ConnMuxEventLoop();

	MuxState->mux_ready = false;
	MuxState->mux_pid = 0;
	DisownLatch(&MuxState->mux_latch);

	if (mux_listen_sock != PGINVALID_SOCKET)
	{
		closesocket(mux_listen_sock);
		mux_listen_sock = PGINVALID_SOCKET;
	}

	for (i = 0; i < MUX_MAX_PEERS; i++)
	{
		if (mux_peers[i].sock != PGINVALID_SOCKET)
		{
			closesocket(mux_peers[i].sock);
			mux_peers[i].sock = PGINVALID_SOCKET;
		}
	}

	proc_exit(0);
}

static void
ConnMuxSetupListenSocket(void)
{
	struct sockaddr_in addr;
	int one = 1;

	mux_listen_sock = socket(AF_INET, SOCK_STREAM, 0);
	if (mux_listen_sock == PGINVALID_SOCKET)
		ereport(FATAL,
				(errmsg("connection multiplexer: could not create listen socket: %m")));

	if (setsockopt(mux_listen_sock, SOL_SOCKET, SO_REUSEADDR,
				   (char *)&one, sizeof(one)) < 0)
		ereport(FATAL,
				(errmsg("connection multiplexer: setsockopt(SO_REUSEADDR) failed: %m")));

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_ANY);
	addr.sin_port = htons((uint16)mux_tcp_port);

	if (bind(mux_listen_sock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
		ereport(FATAL,
				(errmsg("connection multiplexer: could not bind to port %d: %m",
						mux_tcp_port)));

	if (listen(mux_listen_sock, 128) < 0)
		ereport(FATAL,
				(errmsg("connection multiplexer: could not listen on socket: %m")));

	if (!pg_set_noblock(mux_listen_sock))
		ereport(FATAL,
				(errmsg("connection multiplexer: could not set listen socket non-blocking: %m")));

	if (fcntl(mux_listen_sock, F_SETFD, FD_CLOEXEC) < 0)
		ereport(FATAL,
				(errmsg("connection multiplexer: fcntl(F_SETFD) on listen socket: %m")));
}

static void
ConnMuxInitPendingSockets(void)
{
	int i;

	for (i = 0; i < MUX_MAX_PEERS; i++)
	{
		mux_pending[i] = PGINVALID_SOCKET;
		mux_pending_adopted[i] = false;
	}
	mux_pending_count = 0;
}

static void
ConnMuxInitProxies(void)
{
	int i;

	for (i = 0; i < MUX_PROXY_MAX; i++)
	{
		mux_proxies[i].client_sock = PGINVALID_SOCKET;
		mux_proxies[i].remote_sock = PGINVALID_SOCKET;
		mux_proxies[i].state = MPX_EMPTY;
		mux_proxies[i].startup_len = 0;
		mux_proxies[i].startup_off = 0;
		mux_proxies[i].c2r_len = 0;
		mux_proxies[i].c2r_off = 0;
		mux_proxies[i].r2c_len = 0;
		mux_proxies[i].r2c_off = 0;
	}
}

static void
ConnMuxEventLoop(void)
{
	for (;;)
	{
		WaitEventSet *wes;
		WaitEvent events[128];
		int n_events;
		int i;

		wes = CreateWaitEventSet(CurrentResourceOwner,
								 4 + (MUX_MAX_PEERS * 2) +
									 (MUX_PROXY_MAX * 2));
		AddWaitEventToSet(wes, WL_LATCH_SET, PGINVALID_SOCKET,
						  &MuxState->mux_latch, NULL);
		AddWaitEventToSet(wes, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
						  NULL, NULL);
		AddWaitEventToSet(wes, WL_SOCKET_READABLE, mux_listen_sock,
						  NULL, (void *)(intptr_t)-1);

		for (i = 0; i < mux_n_peers; i++)
		{
			if (mux_peers[i].sock != PGINVALID_SOCKET)
				AddWaitEventToSet(wes, WL_SOCKET_READABLE, mux_peers[i].sock,
								  NULL, MUX_EVENT(MUX_EVENT_PEER, i));
		}

		for (i = 0; i < MUX_MAX_PEERS; i++)
		{
			if (mux_pending[i] != PGINVALID_SOCKET)
				AddWaitEventToSet(wes, WL_SOCKET_READABLE, mux_pending[i],
								  NULL, MUX_EVENT(MUX_EVENT_PENDING, i));
		}

		for (i = 0; i < MUX_PROXY_MAX; i++)
		{
			MuxProxyConn *proxy = &mux_proxies[i];
			uint32 evmask;

			if (proxy->state == MPX_EMPTY)
				continue;

			if (proxy->client_sock != PGINVALID_SOCKET)
			{
				evmask = 0;

				if (proxy->state == MPX_STARTUP ||
					proxy->c2r_len < MUX_PROXY_BUF)
					evmask |= WL_SOCKET_READABLE;
				if (proxy->r2c_len > 0)
					evmask |= WL_SOCKET_WRITEABLE;

				if (evmask != 0)
					AddWaitEventToSet(wes, evmask, proxy->client_sock,
									  NULL, MUX_EVENT(MUX_EVENT_PROXY_CLIENT, i));
			}

			if (proxy->state == MPX_ACTIVE &&
				proxy->remote_sock != PGINVALID_SOCKET)
			{
				evmask = 0;

				if (proxy->r2c_len < MUX_PROXY_BUF)
					evmask |= WL_SOCKET_READABLE;
				if (proxy->c2r_len > 0)
					evmask |= WL_SOCKET_WRITEABLE;

				if (evmask != 0)
					AddWaitEventToSet(wes, evmask, proxy->remote_sock,
									  NULL, MUX_EVENT(MUX_EVENT_PROXY_REMOTE, i));
			}
		}

		if (mux_we_wait == 0)
			mux_we_wait = WaitEventExtensionNew("ConnMuxWait");

		n_events = WaitEventSetWait(wes, 1000, events, lengthof(events),
									mux_we_wait);
		FreeWaitEventSet(wes);

		ResetLatch(&MuxState->mux_latch);

		/* Always check worker shm_mq responses */
		ConnMuxPollWorkerResponses();

		for (i = 0; i < n_events; i++)
		{
			WaitEvent *ev = &events[i];

			if (ev->events & WL_LATCH_SET)
			{
				ConnMuxProcessNewSessions();
				continue;
			}

			if (ev->events & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE))
			{
				if (ev->user_data == (void *)(intptr_t)-1)
				{
					ConnMuxAcceptPeer();
					continue;
				}

				switch (MUX_EVENT_KIND(ev->user_data))
				{
				case MUX_EVENT_PEER:
					ConnMuxReadPeerData(MUX_EVENT_IDX(ev->user_data));
					break;
				case MUX_EVENT_PENDING:
					ConnMuxClassifyPending(MUX_EVENT_IDX(ev->user_data));
					break;
				case MUX_EVENT_PROXY_CLIENT:
					ConnMuxProxyClientEvent(MUX_EVENT_IDX(ev->user_data),
											ev->events);
					break;
				case MUX_EVENT_PROXY_REMOTE:
					ConnMuxProxyRemoteEvent(MUX_EVENT_IDX(ev->user_data),
											ev->events);
					break;
				default:
					break;
				}
			}
		}

		/* Also check sessions (belt+suspenders after timeout) */
		ConnMuxProcessNewSessions();

		/* Clock-sweep idle peer connections */
		ConnMuxCloseIdle();

		/* Rebuild peer count */
		mux_n_peers = 0;
		for (i = 0; i < MUX_MAX_PEERS; i++)
		{
			if (mux_peers[i].sock != PGINVALID_SOCKET)
				mux_n_peers = i + 1;
		}
	}
}

static void
ConnMuxProcessNewSessions(void)
{
	int i;

	for (i = 0; i < MUX_MAX_SESSIONS; i++)
	{
		MuxQuerySlot *slot = &MuxState->sessions[i];
		MuxQueryStatus st;

		SpinLockAcquire(&MuxState->mutex);
		st = slot->status;
		SpinLockRelease(&MuxState->mutex);

		if (st != MQS_PENDING)
			continue;

		if (slot->server_oid == InvalidOid && slot->server_name[0] == '\0')
		{
			int wid = ConnMuxFindWorker(i);

			if (wid >= 0)
			{
				MUX_LOG("session %d assigning to local worker %d (xid %u, readonly=%d)",
						i, wid, slot->xid, slot->is_readonly);
				ConnMuxRouteToWorker(i, wid);
			}
			else
			{
				ConnMuxCompleteSession(i, NULL, 0, "no idle multiplexer worker");
				MUX_LOG("session %d could not find idle worker (xid %u, readonly=%d)",
						i, slot->xid, slot->is_readonly);
			}
		}
		else
		{
			MUX_LOG("session %d routing to remote %s:%d xid %u readonly=%d",
					i,
					slot->peer_host[0] ? slot->peer_host : "<unset>",
					slot->mux_port,
					slot->xid,
					slot->is_readonly);
			ConnMuxRouteToRemote(i);
		}
	}
}

static int
ConnMuxFindWorker(int session_id)
{
	MuxQuerySlot *slot = &MuxState->sessions[session_id];
	int i;

	/* Prefer worker already pinned to same transaction */
	if (TransactionIdIsValid(slot->xid))
	{
		for (i = 0; i < mux_worker_count; i++)
		{
			MuxWorkerSlot *ws = &MuxState->workers[i];
			bool ok = false;

			if (ws->worker_pid == 0 || !ws->mq_ready)
				continue;

			SpinLockAcquire(&ws->mutex);
			if (ws->phase == MWP_IDLE && ws->xid == slot->xid)
				ok = true;
			SpinLockRelease(&ws->mutex);

			if (ok)
				return i;
		}
	}

	/* Any idle worker with no active transaction */
	for (i = 0; i < mux_worker_count; i++)
	{
		MuxWorkerSlot *ws = &MuxState->workers[i];
		bool ok = false;

		if (ws->worker_pid == 0 || !ws->mq_ready)
			continue;

		SpinLockAcquire(&ws->mutex);
		if (ws->phase == MWP_IDLE && !TransactionIdIsValid(ws->xid))
			ok = true;
		SpinLockRelease(&ws->mutex);

		if (ok)
			return i;
	}

	/*
	 * Fallback: if all idle workers keep a transaction affinity xid from prior
	 * work, still allow routing to any idle worker.
	 */
	for (i = 0; i < mux_worker_count; i++)
	{
		MuxWorkerSlot *ws = &MuxState->workers[i];
		bool ok = false;

		if (ws->worker_pid == 0 || !ws->mq_ready)
			continue;

		SpinLockAcquire(&ws->mutex);
		if (ws->phase == MWP_IDLE)
			ok = true;
		SpinLockRelease(&ws->mutex);

		if (ok)
			return i;
	}

	return -1;
}

/*
 * Ensure the mux has shm_mq handles to worker i.
 * The worker must have initialized the mqs (mq_ready == true) before this.
 * Returns true on success.
 */
static bool
ConnMuxAttachWorkerHandles(int worker_id)
{
	MuxWorkerSlot *ws = &MuxState->workers[worker_id];
	shm_mq *mq;

	/* Attach to request mq (mux = sender) */
	if (mux_worker_req_handle[worker_id] == NULL)
	{
		PGPROC *wproc;

		mq = (shm_mq *)ws->mq_req_buf;
		/* Worker has already set itself as receiver; we set sender here */
		shm_mq_set_sender(mq, MyProc);
		mux_worker_req_handle[worker_id] = shm_mq_attach(mq, NULL, NULL);

		/* If we know worker's PGPROC, ensure receiver is set */
		if (ws->worker_pid != 0)
		{
			wproc = BackendPidGetProc(ws->worker_pid);
			if (wproc && shm_mq_get_receiver(mq) == NULL)
				shm_mq_set_receiver(mq, wproc);
		}
	}

	/* Attach to response mq (mux = receiver) */
	if (mux_worker_resp_handle[worker_id] == NULL)
	{
		mq = (shm_mq *)ws->mq_resp_buf;
		/* Worker has set itself as sender; we set receiver here */
		shm_mq_set_receiver(mq, MyProc);
		mux_worker_resp_handle[worker_id] = shm_mq_attach(mq, NULL, NULL);
	}

	return (mux_worker_req_handle[worker_id] != NULL &&
			mux_worker_resp_handle[worker_id] != NULL);
}

static void
ConnMuxRouteToWorker(int session_id, int worker_id)
{
	MuxQuerySlot *slot = &MuxState->sessions[session_id];
	MuxWorkerSlot *ws = &MuxState->workers[worker_id];
	shm_mq_result res;
	char hdr[13];
	shm_mq_iovec iov[2];

	if (!ConnMuxAttachWorkerHandles(worker_id))
	{
		ConnMuxCompleteSession(session_id, NULL, 0, "failed to attach worker queues");
		MUX_LOG("session %d failed to attach worker %d queues", session_id, worker_id);
		return;
	}

	/* Build request header: [session_id 4B][xid 4B][rdonly 1B][qlen 4B] */
	put_be32(hdr + 0, (uint32)session_id);
	put_be32(hdr + 4, (uint32)slot->xid);
	hdr[8] = slot->is_readonly ? 1 : 0;
	put_be32(hdr + 9, (uint32)slot->query_len);

	iov[0].data = hdr;
	iov[0].len = 13;
	iov[1].data = slot->query;
	iov[1].len = slot->query_len;

	SpinLockAcquire(&MuxState->mutex);
	slot->status = MQS_ASSIGNED;
	slot->assigned_worker = worker_id;
	SpinLockRelease(&MuxState->mutex);

	MUX_LOG("session %d dispatched to worker %d (qlen=%d rdonly=%d xid=%u)",
			session_id, worker_id, slot->query_len, slot->is_readonly, slot->xid);

	SpinLockAcquire(&ws->mutex);
	ws->phase = MWP_BUSY;
	ws->session_id = session_id;
	ws->xid = slot->xid;
	SpinLockRelease(&ws->mutex);

	res = shm_mq_sendv(mux_worker_req_handle[worker_id], iov, 2,
					   false /* nowait */, true);
	if (res != SHM_MQ_SUCCESS)
	{
		mux_worker_req_handle[worker_id] = NULL;
		mux_worker_resp_handle[worker_id] = NULL;

		SpinLockAcquire(&ws->mutex);
		ws->phase = MWP_IDLE;
		ws->session_id = -1;
		SpinLockRelease(&ws->mutex);

		ConnMuxCompleteSession(session_id, NULL, 0, "worker queue send failed");
		return;
	}

	ConnMuxSignalWorker(worker_id);
}

static void
ConnMuxRouteToRemote(int session_id)
{
	MuxQuerySlot *slot = &MuxState->sessions[session_id];
	int peer_idx;
	char hdr[14];
	bool ok;

	peer_idx = ConnMuxFindOrOpenPeer(slot->peer_host, slot->mux_port);
	if (peer_idx < 0)
	{
		ConnMuxCompleteSession(session_id, NULL, 0,
							   "could not connect to peer multiplexer");
		return;
	}

	SpinLockAcquire(&MuxState->mutex);
	slot->status = MQS_ASSIGNED;
	SpinLockRelease(&MuxState->mutex);

	mux_peers[peer_idx].pending_session = session_id;
	mux_peers[peer_idx].last_used = GetCurrentTimestamp();

	/* Wire: [type 'Q'][session_id 4B][xid 4B][rdonly 1B][qlen 4B] + query */
	hdr[0] = MUX_MSG_QUERY;
	put_be32(hdr + 1, (uint32)session_id);
	put_be32(hdr + 5, (uint32)slot->xid);
	hdr[9] = slot->is_readonly ? 1 : 0;
	put_be32(hdr + 10, (uint32)slot->query_len);

	ok = mux_write_all(mux_peers[peer_idx].sock, hdr, 14);
	if (ok)
		ok = mux_write_all(mux_peers[peer_idx].sock, slot->query, slot->query_len);

	if (!ok)
	{
		closesocket(mux_peers[peer_idx].sock);
		mux_peers[peer_idx].sock = PGINVALID_SOCKET;
		ConnMuxCompleteSession(session_id, NULL, 0,
							   "could not send query to peer multiplexer");
		MUX_LOG("session %d failed send to peer %s:%d",
				session_id, slot->peer_host, slot->mux_port);
	}
	else
	{
		MUX_LOG("session %d sent to peer %s:%d (qlen=%d rdonly=%d xid=%u)",
				session_id,
				slot->peer_host[0] ? slot->peer_host : "<unset>",
				slot->mux_port,
				slot->query_len,
				slot->is_readonly,
				slot->xid);
	}
}

static void
ConnMuxPollWorkerResponses(void)
{
	int i;

	for (i = 0; i < mux_worker_count; i++)
	{
		MuxWorkerSlot *ws = &MuxState->workers[i];

		if (ws->worker_pid == 0 || !ws->mq_ready)
			continue;

		if (mux_worker_resp_handle[i] == NULL)
		{
			if (!ConnMuxAttachWorkerHandles(i))
				continue;
		}

		for (;;)
		{
			Size nbytes;
			void *data;
			shm_mq_result res;

			res = shm_mq_receive(mux_worker_resp_handle[i],
								 &nbytes, &data, true /* nowait */);

			if (res == SHM_MQ_WOULD_BLOCK)
				break;

			if (res == SHM_MQ_DETACHED)
			{
				int sid;

				SpinLockAcquire(&ws->mutex);
				sid = ws->session_id;
				ws->phase = MWP_IDLE;
				ws->session_id = -1;
				ws->mq_ready = false;
				SpinLockRelease(&ws->mutex);

				mux_worker_req_handle[i] = NULL;
				mux_worker_resp_handle[i] = NULL;

				if (sid >= 0)
					ConnMuxCompleteSession(sid, NULL, 0, "worker process crashed");
				break;
			}

			if (res == SHM_MQ_SUCCESS)
			{
				/* [session_id 4B][type 1B][rlen 4B][data] */
				char *p = (char *)data;
				int sid;
				char type;
				int rlen;

				if (nbytes < 9)
					break;

				sid = (int)get_be32(p);
				type = p[4];
				rlen = (int)get_be32(p + 5);
				p += 9;

				if (type == 'D')
					ConnMuxCompleteSession(sid, p, rlen, NULL);
				else
					ConnMuxCompleteSession(sid, NULL, 0, (rlen > 0 ? p : "error"));

				SpinLockAcquire(&ws->mutex);
				ws->phase = MWP_IDLE;
				ws->session_id = -1;
				/* keep xid for transaction affinity */
				SpinLockRelease(&ws->mutex);

				break;
			}
		}
	}
}

static void
ConnMuxAcceptPeer(void)
{
	pgsocket new_sock;
	struct sockaddr_in client_addr;
	socklen_t client_len = sizeof(client_addr);

	new_sock = accept(mux_listen_sock,
					  (struct sockaddr *)&client_addr, &client_len);
	if (new_sock == PGINVALID_SOCKET)
	{
		if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
			elog(WARNING, "connection multiplexer: accept failed: %m");
		return;
	}

	if (!pg_set_noblock(new_sock))
	{
		closesocket(new_sock);
		return;
	}
	if (fcntl(new_sock, F_SETFD, FD_CLOEXEC) < 0)
	{
		closesocket(new_sock);
		return;
	}

	ConnMuxAddPendingSocket(new_sock, false);
	MUX_LOG("accepted socket %d into pending list (pending=%d)",
			(int)new_sock, mux_pending_count);
}

static void
ConnMuxAddPendingSocket(pgsocket sock, bool adopted)
{
	int i;

	for (i = 0; i < MUX_MAX_PEERS; i++)
	{
		if (mux_pending[i] == PGINVALID_SOCKET)
		{
			mux_pending[i] = sock;
			mux_pending_adopted[i] = adopted;
			mux_pending_count++;
			MUX_LOG("pending socket %d stored at idx %d (pending=%d)",
					(int)sock, i, mux_pending_count);
			return;
		}
	}

	elog(WARNING, "connection multiplexer: too many pending connections");
	closesocket(sock);
}

/*
 * Allow an already-accepted socket (e.g., forwarded by postmaster) to be
 * handled by the multiplexer. Caller is responsible for source validation.
 */
bool
ConnMuxAdoptSocket(pgsocket sock)
{
	if (sock == PGINVALID_SOCKET)
		return false;

	if (!pg_set_noblock(sock))
	{
		closesocket(sock);
		return false;
	}
	if (fcntl(sock, F_SETFD, FD_CLOEXEC) < 0)
	{
		closesocket(sock);
		return false;
	}

	ConnMuxAddPendingSocket(sock, true);
	return true;
}

static void
ConnMuxClassifyPending(int idx)
{
	pgsocket sock = mux_pending[idx];
	char peekbuf[8];
	int nread;
	uint32 msg_len;
	uint32 proto;
	int i;
	bool is_chain = false;
	char *startup_buf = NULL;

	if (sock == PGINVALID_SOCKET)
		return;

	nread = recv(sock, peekbuf, sizeof(peekbuf), MSG_PEEK);
	if (nread < 0)
	{
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			return;
		closesocket(sock);
		mux_pending[idx] = PGINVALID_SOCKET;
		mux_pending_adopted[idx] = false;
		mux_pending_count--;
		return;
	}
	if (nread < (int)sizeof(peekbuf))
		return;

	msg_len = get_be32(peekbuf);
	proto = get_be32(peekbuf + 4);

	/* Look for mux_chain flag to distinguish mux-to-mux links */
	if (msg_len >= 8 && msg_len <= MUX_STARTUP_MAX &&
		proto == MUX_PROTOCOL_VERSION)
	{
		startup_buf = palloc(msg_len);
		if (recv(sock, startup_buf, msg_len, MSG_PEEK) == (int) msg_len)
		{
			char *ptr = startup_buf + 8;
			char *end = startup_buf + msg_len;
			char *options = NULL;

			while (ptr < end && *ptr != '\0')
			{
				char *key = ptr;
				char *val;

				ptr += strlen(ptr) + 1;
				if (ptr >= end)
					break;
				val = ptr;
				ptr += strlen(ptr) + 1;

				if (strcmp(key, "options") == 0)
				{
					options = val;
					break;
				}
			}

			if (options != NULL)
			{
				char *opts_copy = pstrdup(options);
				char *saveptr = NULL;
				char *tok = strtok_r(opts_copy, " ", &saveptr);

				while (tok != NULL)
				{
					if (strcmp(tok, "-cmux_chain=1") == 0 ||
						strcmp(tok, "mux_chain=1") == 0)
					{
						is_chain = true;
					}
					else if (strcmp(tok, "-c") == 0)
					{
						char *next = strtok_r(NULL, " ", &saveptr);

						if (next == NULL)
							break;
						if (strncmp(next, "mux_chain=", 10) == 0)
							is_chain = true;
					}
					tok = strtok_r(NULL, " ", &saveptr);
				}
				pfree(opts_copy);
			}
		}

	}

	/*
	 * Any PostgreSQL startup packet is handled through proxy startup parsing.
	 * For mux_chain packets we still emit peer classification in logs, but
	 * route through proxy startup so forwarded options are preserved and the
	 * connection does not stall waiting for mux protocol bytes.
	 */
	if (msg_len >= 8 && msg_len <= MUX_STARTUP_MAX &&
		proto == MUX_PROTOCOL_VERSION)
	{
		/* Assign to proxy list */
		for (i = 0; i < MUX_PROXY_MAX; i++)
		{
			if (mux_proxies[i].state == MPX_EMPTY)
			{
				mux_proxies[i].client_sock = sock;
				mux_proxies[i].remote_sock = PGINVALID_SOCKET;
				mux_proxies[i].state = MPX_STARTUP;
				mux_proxies[i].startup_len = 0;
				mux_proxies[i].startup_off = 0;
				mux_proxies[i].c2r_len = 0;
				mux_proxies[i].c2r_off = 0;
				mux_proxies[i].r2c_len = 0;
				mux_proxies[i].r2c_off = 0;

				mux_pending[idx] = PGINVALID_SOCKET;
				mux_pending_count--;
				MUX_LOG("pending socket %d classified as proxy idx %d (pending=%d)",
						(int)sock, i, mux_pending_count);
				return;
			}
		}

		elog(WARNING, "connection multiplexer: too many proxy connections");
		closesocket(sock);
		mux_pending[idx] = PGINVALID_SOCKET;
		mux_pending_count--;
		return;
	}

	/*
	 * Treat as peer mux connection. If this connection came from a remote mux
	 * (is_chain=true), consume the startup packet so the peer read loop starts
	 * on the multiplexer protocol messages instead of the PostgreSQL startup.
	 */
	if (is_chain && startup_buf != NULL)
	{
		/* Drop the buffered startup bytes */
		if (recv(sock, startup_buf, msg_len, MSG_WAITALL) != (int) msg_len)
		{
			closesocket(sock);
			mux_pending[idx] = PGINVALID_SOCKET;
			mux_pending_count--;
			pfree(startup_buf);
			return;
		}
	}
	if (startup_buf != NULL)
		pfree(startup_buf);

	for (i = 0; i < MUX_MAX_PEERS; i++)
	{
		if (mux_peers[i].sock == PGINVALID_SOCKET)
		{
			mux_peers[i].sock = sock;
			mux_peers[i].pending_session = -1;
			mux_peers[i].last_used = GetCurrentTimestamp();
			mux_peers[i].clock_mark = 0;
			mux_pending_adopted[idx] = false;
			if (i >= mux_n_peers)
				mux_n_peers = i + 1;

			mux_pending[idx] = PGINVALID_SOCKET;
			mux_pending_count--;
			MUX_LOG("pending socket %d classified as peer idx %d (pending=%d)",
					(int)sock, i, mux_pending_count);
			return;
		}
	}

	elog(WARNING, "connection multiplexer: too many peer connections");
	closesocket(sock);
	mux_pending[idx] = PGINVALID_SOCKET;
	mux_pending_count--;
}

static void
ConnMuxProxyClientEvent(int idx, uint32 events)
{
	MuxProxyConn *proxy = &mux_proxies[idx];

	if (proxy->state == MPX_EMPTY)
		return;

	if (proxy->state == MPX_STARTUP)
	{
		if (events & WL_SOCKET_READABLE)
		{
			if (!ConnMuxProxyReadStartup(idx))
			{
				MUX_LOG("proxy %d startup failed (client=%d len=%d off=%d), closing",
						idx, (int)proxy->client_sock,
						proxy->startup_len, proxy->startup_off);
				ConnMuxProxyClose(idx);
			}
			else
				MUX_LOG("proxy %d read startup progress (len=%d off=%d)",
						idx, proxy->startup_len, proxy->startup_off);
		}
		return;
	}

	if (proxy->state != MPX_ACTIVE)
		return;

	if ((events & WL_SOCKET_WRITEABLE) && proxy->r2c_len > 0)
	{
		int written = send(proxy->client_sock,
						   proxy->r2c_buf + proxy->r2c_off,
						   proxy->r2c_len, 0);
		if (written > 0)
		{
			proxy->r2c_off += written;
			proxy->r2c_len -= written;
			if (proxy->r2c_len == 0)
				proxy->r2c_off = 0;
		}
		else if (written < 0)
		{
			int			save_errno = errno;

			/*
			 * When the remote socket is still completing a nonblocking connect,
			 * send() can transiently return EINPROGRESS/EALREADY/ENOTCONN; treat
			 * those like EAGAIN and keep the proxy alive.
			 */
			if (save_errno != EAGAIN &&
				save_errno != EWOULDBLOCK &&
				save_errno != EINPROGRESS &&
				save_errno != EALREADY &&
				save_errno != ENOTCONN)
				ConnMuxProxyClose(idx);
		}
		else if (written == 0)
			ConnMuxProxyClose(idx);
	}

	if ((events & WL_SOCKET_READABLE) &&
		proxy->c2r_len < MUX_PROXY_BUF)
	{
		int readn = recv(proxy->client_sock,
						 proxy->c2r_buf + proxy->c2r_len,
						 MUX_PROXY_BUF - proxy->c2r_len, 0);
		if (readn > 0)
		{
			proxy->c2r_len += readn;
		}
		else if (readn == 0)
		{
			ConnMuxProxyClose(idx);
		}
		else if (errno != EAGAIN && errno != EWOULDBLOCK)
		{
			ConnMuxProxyClose(idx);
		}
	}
}

static void
ConnMuxProxyRemoteEvent(int idx, uint32 events)
{
	MuxProxyConn *proxy = &mux_proxies[idx];

	if (proxy->state != MPX_ACTIVE)
		return;

	if ((events & WL_SOCKET_WRITEABLE) && proxy->c2r_len > 0)
	{
		int written = send(proxy->remote_sock,
						   proxy->c2r_buf + proxy->c2r_off,
						   proxy->c2r_len, 0);
		if (written > 0)
		{
			proxy->c2r_off += written;
			proxy->c2r_len -= written;
			if (proxy->c2r_len == 0)
				proxy->c2r_off = 0;
		}
		else if (written < 0)
		{
			int			save_errno = errno;

			/*
			 * When the remote socket is still completing a nonblocking connect,
			 * send() can transiently return EINPROGRESS/EALREADY/ENOTCONN; treat
			 * those like EAGAIN and keep the proxy alive.
			 */
			if (save_errno != EAGAIN &&
				save_errno != EWOULDBLOCK &&
				save_errno != EINPROGRESS &&
				save_errno != EALREADY &&
				save_errno != ENOTCONN)
				ConnMuxProxyClose(idx);
		}
		else if (written == 0)
			ConnMuxProxyClose(idx);
	}

	if ((events & WL_SOCKET_READABLE) &&
		proxy->r2c_len < MUX_PROXY_BUF)
	{
		int readn = recv(proxy->remote_sock,
						 proxy->r2c_buf + proxy->r2c_len,
						 MUX_PROXY_BUF - proxy->r2c_len, 0);
		if (readn > 0)
		{
			proxy->r2c_len += readn;
		}
		else if (readn == 0)
		{
			ConnMuxProxyClose(idx);
		}
		else if (errno != EAGAIN && errno != EWOULDBLOCK)
		{
			ConnMuxProxyClose(idx);
		}
	}
}

static void
ConnMuxProxyClose(int idx)
{
	MuxProxyConn *proxy = &mux_proxies[idx];

	MUX_LOG("proxy %d closing (client=%d remote=%d state=%d c2r=%d r2c=%d)",
			idx, (int)proxy->client_sock, (int)proxy->remote_sock,
			proxy->state, proxy->c2r_len, proxy->r2c_len);

	if (proxy->client_sock != PGINVALID_SOCKET)
		closesocket(proxy->client_sock);
	if (proxy->remote_sock != PGINVALID_SOCKET)
		closesocket(proxy->remote_sock);

	proxy->client_sock = PGINVALID_SOCKET;
	proxy->remote_sock = PGINVALID_SOCKET;
	proxy->state = MPX_EMPTY;
	proxy->startup_len = 0;
	proxy->startup_off = 0;
	proxy->c2r_len = 0;
	proxy->c2r_off = 0;
	proxy->r2c_len = 0;
	proxy->r2c_off = 0;
}

static bool
ConnMuxProxyReadStartup(int idx)
{
	MuxProxyConn *proxy = &mux_proxies[idx];
	int			actual_port;

	if (proxy->startup_len == 0)
	{
		int readn = recv(proxy->client_sock,
						 proxy->startup_buf + proxy->startup_off,
						 4 - proxy->startup_off, 0);
		if (readn <= 0)
		{
			if (readn < 0 &&
				(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
				return true;
			return false;
		}

		proxy->startup_off += readn;
		if (proxy->startup_off < 4)
			return true;

		proxy->startup_len = (int)get_be32(proxy->startup_buf);
		if (proxy->startup_len < 8 || proxy->startup_len > MUX_STARTUP_MAX)
			return false;
	}

	if (proxy->startup_off < proxy->startup_len)
	{
		int readn = recv(proxy->client_sock,
						 proxy->startup_buf + proxy->startup_off,
						 proxy->startup_len - proxy->startup_off, 0);
		if (readn <= 0)
		{
			if (readn < 0 &&
				(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
				return true;
			return false;
		}
		proxy->startup_off += readn;
	}

	if (proxy->startup_off < proxy->startup_len)
		return true;

	/* Parse startup packet and establish remote connection */
	{
		char target_host[256];
		int target_port = 0;
		char connect_host[256];
		int connect_port = 0;
		bool is_mux_chain = false;
		char *startup_forward = NULL;
		int startup_forward_len = 0;

		if (!ConnMuxProxyParseStartup(proxy,
									  target_host, sizeof(target_host),
									  &target_port,
									  connect_host, sizeof(connect_host),
									  &connect_port,
									  &is_mux_chain,
									  &startup_forward,
									  &startup_forward_len))
			return false;

		proxy->remote_sock = ConnMuxOpenPeerSocket(connect_host, connect_port);
		if (proxy->remote_sock == PGINVALID_SOCKET)
		{
			pfree(startup_forward);
			return false;
		}
		MUX_LOG("proxy %d connecting client %d -> %s:%d (target=%s:%d)%s (startup_len=%d)",
				idx, (int)proxy->client_sock,
				connect_host[0] ? connect_host : "<unset>",
				connect_port,
				target_host[0] ? target_host : "<unset>",
				target_port,
				is_mux_chain ? " [mux-to-mux]" : "",
				proxy->startup_len);

		/* Queue the rewritten startup packet to send to remote */
		if (startup_forward_len > MUX_PROXY_BUF)
		{
			pfree(startup_forward);
			return false;
		}
		memcpy(proxy->c2r_buf, startup_forward, startup_forward_len);
		proxy->c2r_len = startup_forward_len;
		proxy->c2r_off = 0;
		pfree(startup_forward);

		proxy->state = MPX_ACTIVE;
		MUX_LOG("proxy %d activated (client=%d remote=%d c2r_len=%d)",
				idx, (int)proxy->client_sock, (int)proxy->remote_sock,
				proxy->c2r_len);
		return true;
	}
}

static bool
ConnMuxProxyParseStartup(MuxProxyConn *proxy,
						 char *target_host, Size host_len,
						 int *target_port,
						 char *connect_host, Size connect_host_len,
						 int *connect_port,
						 bool *is_mux_chain,
						 char **startup_forward,
						 int *startup_forward_len)
{
	char *buf = proxy->startup_buf;
	uint32 proto = get_be32(buf + 4);
	char *ptr = buf + 8;
	char *end = buf + proxy->startup_len;
	StringInfoData clean;
	StringInfoData newmsg;
	char *options = NULL;
	char *clean_options = NULL;
	bool		has_chain_flag = false;
	bool		has_connect_opts = false;

	if (proto != MUX_PROTOCOL_VERSION)
		goto fail;

	initStringInfo(&newmsg);
	initStringInfo(&clean);

	/* Reserve space for length and protocol */
	appendBinaryStringInfo(&newmsg, buf, 8);

	target_host[0] = '\0';
	*target_port = 0;
	connect_host[0] = '\0';
	*connect_port = 0;

	while (ptr < end && *ptr != '\0')
	{
		char *key = ptr;
		char *val;

		ptr += strlen(ptr) + 1;
		if (ptr >= end)
			goto fail;
		val = ptr;
		ptr += strlen(ptr) + 1;

		if (strcmp(key, "options") == 0)
			options = val;
		else
		{
			appendStringInfoString(&newmsg, key);
			appendStringInfoChar(&newmsg, '\0');
			appendStringInfoString(&newmsg, val);
			appendStringInfoChar(&newmsg, '\0');
		}
	}

	if (options != NULL)
	{
		char *opts_copy = pstrdup(options);
		char *saveptr = NULL;
		char *token = strtok_r(opts_copy, " ", &saveptr);

		while (token != NULL)
		{
			if (strcmp(token, "-c") == 0)
			{
				char *next = strtok_r(NULL, " ", &saveptr);

				if (next == NULL)
				{
					token = strtok_r(NULL, " ", &saveptr);
					continue;
				}
				if (strncmp(next, "mux_target_host=", 16) == 0)
				{
					strlcpy(target_host, next + 16, host_len);
				}
				else if (strncmp(next, "mux_target_port=", 16) == 0)
				{
					*target_port = atoi(next + 16);
				}
				else if (strncmp(next, "mux_chain=", 10) == 0)
				{
					has_chain_flag = true;
				}
				else if (strncmp(next, "mux_connect_host=", 17) == 0)
				{
					has_connect_opts = true;
					strlcpy(connect_host, next + 17, connect_host_len);
				}
				else if (strncmp(next, "mux_connect_port=", 17) == 0)
				{
					has_connect_opts = true;
					*connect_port = atoi(next + 17);
				}
				else
				{
					if (clean.len > 0)
						appendStringInfoChar(&clean, ' ');
					appendStringInfoString(&clean, "-c");
					appendStringInfoChar(&clean, ' ');
					appendStringInfoString(&clean, next);
				}
			}
			else if (strncmp(token, "-cmux_target_host=", 19) == 0)
			{
				strlcpy(target_host, token + 19, host_len);
			}
			else if (strncmp(token, "-cmux_target_port=", 19) == 0)
			{
				*target_port = atoi(token + 19);
			}
			else if (strncmp(token, "-cmux_chain=", 12) == 0)
			{
				has_chain_flag = true;
			}
			else if (strncmp(token, "-cmux_connect_host=", 19) == 0)
			{
				has_connect_opts = true;
				strlcpy(connect_host, token + 19, connect_host_len);
			}
			else if (strncmp(token, "-cmux_connect_port=", 19) == 0)
			{
				has_connect_opts = true;
				*connect_port = atoi(token + 19);
			}
			else
			{
				if (clean.len > 0)
					appendStringInfoChar(&clean, ' ');
				appendStringInfoString(&clean, token);
			}

			token = strtok_r(NULL, " ", &saveptr);
		}

		pfree(opts_copy);
	}

	if (target_host[0] == '\0' || *target_port <= 0)
		goto fail;
	if (connect_host[0] == '\0')
		strlcpy(connect_host, target_host, connect_host_len);
	if (*connect_port <= 0)
		*connect_port = *target_port;

	/*
	 * For direct mux-to-mux forwarding, keep mux_target_* so the remote
	 * multiplexer can proxy onward to the real backend target.
	 */
	if (has_connect_opts &&
		((*connect_port != *target_port) ||
		 (strcmp(connect_host, target_host) != 0)))
	{
		if (clean.len > 0)
			appendStringInfoChar(&clean, ' ');
		appendStringInfo(&clean,
						 "-c mux_target_host=%s -c mux_target_port=%d %s",
						 target_host, *target_port, MUX_CHAIN_STARTUP_OPT);
	}

	*is_mux_chain = has_chain_flag;
	if (clean.len > 0)
		clean_options = clean.data;

	if (clean_options != NULL)
	{
		appendStringInfoString(&newmsg, "options");
		appendStringInfoChar(&newmsg, '\0');
		appendStringInfoString(&newmsg, clean_options);
		appendStringInfoChar(&newmsg, '\0');
	}

	appendStringInfoChar(&newmsg, '\0');

	/* Fix up length */
	put_be32(newmsg.data, (uint32)newmsg.len);

	*startup_forward = newmsg.data;
	*startup_forward_len = newmsg.len;

	if (clean.data != NULL)
		pfree(clean.data);

	return true;

fail:
	if (clean.data != NULL)
		pfree(clean.data);
	if (newmsg.data != NULL)
		pfree(newmsg.data);
	return false;
}

static void
ConnMuxReadPeerData(int peer_idx)
{
	MuxRemoteConn *peer = &mux_peers[peer_idx];
	for (;;)
	{
		char msgtype;
		int r;

		r = recv(peer->sock, &msgtype, 1, 0);
		if (r <= 0)
		{
			if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
				return;
			closesocket(peer->sock);
			peer->sock = PGINVALID_SOCKET;
			if (peer->pending_session >= 0)
				ConnMuxCompleteSession(peer->pending_session, NULL, 0,
									   "peer multiplexer disconnected");
			peer->pending_session = -1;
			return;
		}

		switch (msgtype)
		{
		case MUX_MSG_QUERY:
		{
		char fixed_hdr[13];
		uint32 session_id,
			xid,
			qlen;
		bool is_readonly;
		char query_buf[MUX_QUERY_MAX + 1];

		if (!mux_read_all(peer->sock, fixed_hdr, 13))
		{
			closesocket(peer->sock);
			peer->sock = PGINVALID_SOCKET;
			return;
		}
		session_id = get_be32(fixed_hdr + 0);
		xid = get_be32(fixed_hdr + 4);
		is_readonly = (fixed_hdr[8] != 0);
		qlen = get_be32(fixed_hdr + 9);

		if (qlen == 0 || qlen >= MUX_QUERY_MAX)
		{
			closesocket(peer->sock);
			peer->sock = PGINVALID_SOCKET;
			return;
		}
		if (!mux_read_all(peer->sock, query_buf, qlen))
		{
			closesocket(peer->sock);
			peer->sock = PGINVALID_SOCKET;
			return;
		}
		query_buf[qlen] = '\0';

		ConnMuxHandleRemoteQuery(peer->sock,
								 (int)session_id,
								 (TransactionId)xid,
								 is_readonly,
								 query_buf, (int)qlen);
			break;
		}

		case MUX_MSG_RESULT:
		case MUX_MSG_ERROR:
		{
		char rhdr[8];
		uint32 session_id,
			rlen;

		if (!mux_read_all(peer->sock, rhdr, 8))
		{
			closesocket(peer->sock);
			peer->sock = PGINVALID_SOCKET;
			return;
		}
		session_id = get_be32(rhdr + 0);
		rlen = get_be32(rhdr + 4);

		if (rlen > MUX_RESULT_MAX)
		{
			closesocket(peer->sock);
			peer->sock = PGINVALID_SOCKET;
			return;
		}

		{
			char *rbuf = NULL;

			if (rlen > 0)
			{
				rbuf = palloc(rlen + 1);
				if (!mux_read_all(peer->sock, rbuf, rlen))
				{
					pfree(rbuf);
					closesocket(peer->sock);
					peer->sock = PGINVALID_SOCKET;
					return;
				}
				rbuf[rlen] = '\0';
			}

			if (msgtype == MUX_MSG_RESULT)
				ConnMuxCompleteSession((int)session_id, rbuf,
									   (int)rlen, NULL);
			else
				ConnMuxCompleteSession((int)session_id, NULL, 0,
									   rbuf ? rbuf : "unknown peer error");

			if (rbuf)
				pfree(rbuf);
		}

		peer->pending_session = -1;
		peer->last_used = GetCurrentTimestamp();
			break;
		}

		case MUX_MSG_PING:
		{
		char pong = MUX_MSG_PONG;

		(void)mux_write_all(peer->sock, &pong, 1);
			break;
		}

		case MUX_MSG_PONG:
			break;

		default:
			elog(WARNING, "connection multiplexer: unknown message type %c from peer",
				 msgtype);
			closesocket(peer->sock);
			peer->sock = PGINVALID_SOCKET;
			return;
		}
	}
}

static void
ConnMuxPumpSocketsOnce(void)
{
	int i;

	ConnMuxAcceptPeer();

	for (i = 0; i < MUX_MAX_PEERS; i++)
	{
		if (mux_pending[i] != PGINVALID_SOCKET)
			ConnMuxClassifyPending(i);
	}

	for (i = 0; i < MUX_PROXY_MAX; i++)
	{
		MuxProxyConn *proxy = &mux_proxies[i];

		if (proxy->state == MPX_EMPTY)
			continue;

		ConnMuxProxyClientEvent(i, WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);
		if (proxy->state == MPX_EMPTY)
			continue;
		ConnMuxProxyRemoteEvent(i, WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);
	}

	for (i = 0; i < mux_n_peers; i++)
	{
		if (mux_peers[i].sock != PGINVALID_SOCKET)
			ConnMuxReadPeerData(i);
	}
}

/*
 * Handle an inbound remote query: allocate a local session slot, dispatch
 * to a local SPI worker, and send the result back over the TCP socket.
 *
 * We spin-wait for the worker result (bounded) to keep the protocol simple —
 * in a full production system this would be made fully async.
 */
static void
ConnMuxHandleRemoteQuery(pgsocket client_sock,
						 int remote_session_id,
						 TransactionId xid,
						 bool is_readonly,
						 const char *query, int qlen)
{
	int slot_id = -1;
	int i;

	SpinLockAcquire(&MuxState->mutex);
	for (i = 0; i < MUX_MAX_SESSIONS; i++)
	{
		if (MuxState->sessions[i].status == MQS_FREE)
		{
			slot_id = i;
			MuxState->sessions[i].status = MQS_PENDING;
			break;
		}
	}
	SpinLockRelease(&MuxState->mutex);

	if (slot_id < 0)
	{
		const char *errmsg = "no free session slots";
		int elen = strlen(errmsg);
		char rsp[9];

		rsp[0] = MUX_MSG_ERROR;
		put_be32(rsp + 1, (uint32)remote_session_id);
		put_be32(rsp + 5, (uint32)elen);
		(void)mux_write_all(client_sock, rsp, 9);
		(void)mux_write_all(client_sock, errmsg, elen);
		return;
	}

	{
		MuxQuerySlot *slot = &MuxState->sessions[slot_id];

		slot->backend_pid = MyProcPid;
		slot->backend_latch = &MuxState->mux_latch;
		slot->server_oid = InvalidOid;
		slot->server_name[0] = '\0';
		slot->peer_host[0] = '\0';
		slot->mux_port = 0;
		slot->xid = xid;
		slot->is_readonly = is_readonly;
		slot->assigned_worker = -1;
		slot->result_len = 0;
		slot->error_msg[0] = '\0';
		if (qlen >= MUX_QUERY_MAX)
			qlen = MUX_QUERY_MAX - 1;
		slot->query_len = qlen;
		memcpy(slot->query, query, qlen);
		slot->query[qlen] = '\0';

		i = ConnMuxFindWorker(slot_id);
		if (i < 0)
		{
			const char *errmsg = "no worker available";
			int elen = strlen(errmsg);
			char rsp[9];

			SpinLockAcquire(&MuxState->mutex);
			MuxState->sessions[slot_id].status = MQS_FREE;
			SpinLockRelease(&MuxState->mutex);

			rsp[0] = MUX_MSG_ERROR;
			put_be32(rsp + 1, (uint32)remote_session_id);
			put_be32(rsp + 5, (uint32)elen);
			(void)mux_write_all(client_sock, rsp, 9);
			(void)mux_write_all(client_sock, errmsg, elen);
			return;
		}

		ConnMuxRouteToWorker(slot_id, i);

		/* Bounded spin-wait for completion */
		for (int attempt = 0; attempt < MUX_REMOTE_QUERY_TIMEOUT_MS; attempt++)
		{
			MuxQueryStatus st;

			ConnMuxPollWorkerResponses();
			ConnMuxPumpSocketsOnce();

			SpinLockAcquire(&MuxState->mutex);
			st = MuxState->sessions[slot_id].status;
			SpinLockRelease(&MuxState->mutex);

			if (st == MQS_DONE || st == MQS_ERROR)
			{
				char rsp[9];
				MuxQuerySlot *s = &MuxState->sessions[slot_id];

				if (st == MQS_DONE)
				{
					rsp[0] = MUX_MSG_RESULT;
					put_be32(rsp + 1, (uint32)remote_session_id);
					put_be32(rsp + 5, (uint32)s->result_len);
					(void)mux_write_all(client_sock, rsp, 9);
					if (s->result_len > 0)
						(void)mux_write_all(client_sock, s->result_buf, s->result_len);
				}
				else
				{
					int elen = strlen(s->error_msg);

					rsp[0] = MUX_MSG_ERROR;
					put_be32(rsp + 1, (uint32)remote_session_id);
					put_be32(rsp + 5, (uint32)elen);
					(void)mux_write_all(client_sock, rsp, 9);
					if (elen > 0)
						(void)mux_write_all(client_sock, s->error_msg, elen);
				}

				SpinLockAcquire(&MuxState->mutex);
				s->status = MQS_FREE;
				SpinLockRelease(&MuxState->mutex);
				return;
			}

			pg_usleep(1000);
		}

		/* Timeout */
		{
			const char *errmsg = "worker timeout";
			int elen = strlen(errmsg);
			char rsp[9];

			SpinLockAcquire(&MuxState->mutex);
			MuxState->sessions[slot_id].status = MQS_FREE;
			SpinLockRelease(&MuxState->mutex);

			rsp[0] = MUX_MSG_ERROR;
			put_be32(rsp + 1, (uint32)remote_session_id);
			put_be32(rsp + 5, (uint32)elen);
			(void)mux_write_all(client_sock, rsp, 9);
			(void)mux_write_all(client_sock, errmsg, elen);
		}
	}
}

static pgsocket
ConnMuxOpenPeerSocket(const char *host, int port)
{
	pgsocket sock;
	struct addrinfo hint;
	struct addrinfo *addrs = NULL;
	char portstr[16];
	int ret;
	int one = 1;

	MemSet(&hint, 0, sizeof(hint));
	hint.ai_family = AF_UNSPEC;
	hint.ai_socktype = SOCK_STREAM;

	snprintf(portstr, sizeof(portstr), "%d", port);
	ret = getaddrinfo(host, portstr, &hint, &addrs);
	if (ret != 0 || addrs == NULL)
	{
		elog(WARNING, "connection multiplexer: could not resolve %s:%d: %s",
			 host, port, gai_strerror(ret));
		if (addrs)
			freeaddrinfo(addrs);
		return PGINVALID_SOCKET;
	}

	sock = socket(addrs->ai_family, addrs->ai_socktype, addrs->ai_protocol);
	if (sock == PGINVALID_SOCKET)
	{
		freeaddrinfo(addrs);
		return PGINVALID_SOCKET;
	}

	setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *)&one, sizeof(one));

	if (connect(sock, addrs->ai_addr, addrs->ai_addrlen) < 0)
	{
		if (errno != EINPROGRESS && errno != EWOULDBLOCK)
		{
			freeaddrinfo(addrs);
			closesocket(sock);
			return PGINVALID_SOCKET;
		}
	}

	freeaddrinfo(addrs);

	if (!pg_set_noblock(sock))
	{
		closesocket(sock);
		return PGINVALID_SOCKET;
	}

	if (fcntl(sock, F_SETFD, FD_CLOEXEC) < 0)
	{
		closesocket(sock);
		return PGINVALID_SOCKET;
	}

	return sock;
}

static int
ConnMuxFindOrOpenPeer(const char *host, int port)
{
	int i;
	int free_slot = -1;

	for (i = 0; i < MUX_MAX_PEERS; i++)
	{
		if (mux_peers[i].sock != PGINVALID_SOCKET &&
			mux_peers[i].peer_port == port &&
			strcmp(mux_peers[i].peer_host, host) == 0 &&
			mux_peers[i].pending_session < 0)
		{
			mux_peers[i].clock_mark = 0;
			return i;
		}
		if (mux_peers[i].sock == PGINVALID_SOCKET && free_slot < 0)
			free_slot = i;
	}

	if (free_slot < 0)
	{
		/* Clock-sweep eviction */
		for (i = 0; i < MUX_MAX_PEERS; i++)
		{
			if (mux_peers[i].sock != PGINVALID_SOCKET &&
				mux_peers[i].pending_session < 0 &&
				mux_peers[i].clock_mark >= MUX_CLOCK_SWEEP_MAX)
			{
				closesocket(mux_peers[i].sock);
				mux_peers[i].sock = PGINVALID_SOCKET;
				free_slot = i;
				break;
			}
			if (mux_peers[i].sock != PGINVALID_SOCKET &&
				mux_peers[i].pending_session < 0)
				mux_peers[i].clock_mark++;
		}
	}

	if (free_slot < 0)
	{
		elog(WARNING, "connection multiplexer: no free peer slot");
		return -1;
	}

	{
		pgsocket s = ConnMuxOpenPeerSocket(host, port);

		if (s == PGINVALID_SOCKET)
			return -1;

		mux_peers[free_slot].sock = s;
		strlcpy(mux_peers[free_slot].peer_host, host,
				sizeof(mux_peers[free_slot].peer_host));
		mux_peers[free_slot].peer_port = port;
		mux_peers[free_slot].pending_session = -1;
		mux_peers[free_slot].last_used = GetCurrentTimestamp();
		mux_peers[free_slot].clock_mark = 0;

		if (free_slot >= mux_n_peers)
			mux_n_peers = free_slot + 1;

		MUX_LOG("opened peer slot %d to %s:%d", free_slot, host, port);

		return free_slot;
	}
}

static void
ConnMuxCloseIdle(void)
{
	static TimestampTz last_sweep = 0;
	TimestampTz now = GetCurrentTimestamp();
	int i;

	if (last_sweep != 0 &&
		!TimestampDifferenceExceeds(last_sweep, now, 5000))
		return;
	last_sweep = now;

	for (i = 0; i < MUX_MAX_PEERS; i++)
	{
		if (mux_peers[i].sock == PGINVALID_SOCKET)
			continue;
		if (mux_peers[i].pending_session >= 0)
		{
			mux_peers[i].clock_mark = 0;
			continue;
		}
		mux_peers[i].clock_mark++;
		if (mux_peers[i].clock_mark > MUX_CLOCK_SWEEP_MAX)
		{
			closesocket(mux_peers[i].sock);
			mux_peers[i].sock = PGINVALID_SOCKET;
		}
	}
}

static void
ConnMuxCompleteSession(int session_id, const char *result, int result_len,
					   const char *error)
{
	MuxQuerySlot *slot;
	Latch *latch;

	if (session_id < 0 || session_id >= MUX_MAX_SESSIONS)
		return;

	slot = &MuxState->sessions[session_id];

	if (error)
	{
		strlcpy(slot->error_msg, error, sizeof(slot->error_msg));
		SpinLockAcquire(&MuxState->mutex);
		slot->status = MQS_ERROR;
		SpinLockRelease(&MuxState->mutex);
		MUX_LOG("session %d completed with error: %s", session_id, error);
	}
	else
	{
		int copy_len = Min(result_len, MUX_RESULT_MAX);

		slot->result_len = copy_len;
		if (copy_len > 0 && result)
			memcpy(slot->result_buf, result, copy_len);
		SpinLockAcquire(&MuxState->mutex);
		slot->status = MQS_DONE;
		SpinLockRelease(&MuxState->mutex);
		MUX_LOG("session %d completed ok (result_len=%d)", session_id, copy_len);
	}

	latch = slot->backend_latch;
	if (latch)
		SetLatch(latch);
}

static void
ConnMuxSignalWorker(int worker_id)
{
	MuxWorkerSlot *ws = &MuxState->workers[worker_id];
	pid_t pid;

	SpinLockAcquire(&ws->mutex);
	pid = ws->worker_pid;
	SpinLockRelease(&ws->mutex);

	if (pid != 0)
	{
		PGPROC *wproc = BackendPidGetProc(pid);

		if (wproc)
			SetLatch(&wproc->procLatch);
	}
}

static void
ConnMuxSpawnWorker(int worker_id)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *handle;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
					BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ConnMuxWorkerMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "connection multiplexer worker %d", worker_id);
	snprintf(bgw.bgw_type, BGW_MAXLEN, "connection multiplexer worker");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = Int32GetDatum(worker_id);

	if (!RegisterDynamicBackgroundWorker(&bgw, &handle))
		elog(WARNING, "connection multiplexer: could not start worker %d",
			 worker_id);
	else
		pfree(handle);
}

/* =========================================================================
 * ConnMuxWorkerMain — local SPI worker process
 * =========================================================================
 */

void ConnMuxWorkerMain(Datum main_arg)
{
	int worker_id = DatumGetInt32(main_arg);
	MuxWorkerSlot *ws;
	shm_mq *req_mq;
	shm_mq *resp_mq;
	shm_mq_handle *req_mqh;
	shm_mq_handle *resp_mqh;

	if (worker_id < 0 || worker_id >= MUX_MAX_WORKERS)
		ereport(FATAL,
				(errmsg("connection multiplexer worker: invalid worker_id %d",
						worker_id)));

	pqsignal(SIGTERM, die);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	Assert(MuxState != NULL);
	ws = &MuxState->workers[worker_id];

	/*
	 * Initialize the shm_mqs in our slot buffers.
	 * We are the receiver for request_mq and the sender for response_mq.
	 * The mux will attach as sender/receiver respectively.
	 */
	req_mq = shm_mq_create(ws->mq_req_buf, MUX_MQ_SIZE);
	shm_mq_set_receiver(req_mq, MyProc);
	req_mqh = shm_mq_attach(req_mq, NULL, NULL);

	resp_mq = shm_mq_create(ws->mq_resp_buf, MUX_MQ_SIZE);
	shm_mq_set_sender(resp_mq, MyProc);
	resp_mqh = shm_mq_attach(resp_mq, NULL, NULL);

	/* Register our PID and mark mqs ready AFTER initializing them */
	SpinLockAcquire(&ws->mutex);
	ws->worker_pid = MyProcPid;
	ws->phase = MWP_IDLE;
	ws->session_id = -1;
	ws->xid = InvalidTransactionId;
	ws->mq_ready = true;
	SpinLockRelease(&ws->mutex);

	/* Wake the mux so it learns we are ready */
	if (MuxState->mux_pid != 0)
		SetLatch(&MuxState->mux_latch);

	MUX_LOG("worker %d (pid=%d) ready for mux sessions",
			worker_id, (int) MyProcPid);

	set_ps_display("idle");

	for (;;)
	{
		Size nbytes;
		void *data;
		shm_mq_result res;
		char *p;
		int session_id;
		TransactionId xid;
		bool is_readonly;
		int qlen;
		char query[MUX_QUERY_MAX + 1];

		res = shm_mq_receive(req_mqh, &nbytes, &data, false);

		if (res == SHM_MQ_DETACHED)
		{
			/*
			 * Mux detached (e.g. it restarted). Recreate the mqs.
			 */
			req_mq = shm_mq_create(ws->mq_req_buf, MUX_MQ_SIZE);
			shm_mq_set_receiver(req_mq, MyProc);
			req_mqh = shm_mq_attach(req_mq, NULL, NULL);

			resp_mq = shm_mq_create(ws->mq_resp_buf, MUX_MQ_SIZE);
			shm_mq_set_sender(resp_mq, MyProc);
			resp_mqh = shm_mq_attach(resp_mq, NULL, NULL);

			SpinLockAcquire(&ws->mutex);
			ws->mq_ready = true;
			SpinLockRelease(&ws->mutex);
			continue;
		}

		if (res != SHM_MQ_SUCCESS)
			continue;

		/* Parse: [session_id 4B][xid 4B][rdonly 1B][qlen 4B][query] */
		if (nbytes < 13)
			continue;

		p = (char *)data;
		session_id = (int)get_be32(p);
		xid = (TransactionId)get_be32(p + 4);
		is_readonly = (p[8] != 0);
		qlen = (int)get_be32(p + 9);
		p += 13;

		if (qlen <= 0 || qlen >= MUX_QUERY_MAX ||
			nbytes < (Size)(13 + qlen))
			continue;

		memcpy(query, p, qlen);
		query[qlen] = '\0';

		set_ps_display("active");
		MUX_LOG("worker %d (pid=%d) executing session %d xid %u qlen=%d: %s",
				worker_id, (int) MyProcPid, session_id, xid, qlen, query);

		SpinLockAcquire(&ws->mutex);
		ws->phase = MWP_BUSY;
		ws->session_id = session_id;
		ws->xid = xid;
		SpinLockRelease(&ws->mutex);

		/* Execute via SPI */
		{
			StringInfoData result_buf;
			bool success = false;
			char error_msg[MUX_ERROR_MAX];

			initStringInfo(&result_buf);
			error_msg[0] = '\0';

			PG_TRY();
			{
				int spi_ret;

				SetCurrentStatementStartTimestamp();
				StartTransactionCommand();
				SPI_connect();
				PushActiveSnapshot(GetTransactionSnapshot());

				spi_ret = SPI_execute(query, is_readonly, 0);

				if (spi_ret < 0)
				{
					snprintf(error_msg, sizeof(error_msg),
							 "SPI_execute failed: %s",
							 SPI_result_code_string(spi_ret));
					MUX_LOG("worker %d (pid=%d) session %d SPI_execute error: %s",
							worker_id, (int) MyProcPid, session_id, error_msg);
				}
				else
				{
					char tmp4[4];
					char tmp2[2];

					if (spi_ret == SPI_OK_SELECT && SPI_tuptable != NULL)
					{
						TupleDesc tdesc = SPI_tuptable->tupdesc;
						int ncols = tdesc->natts;
						uint64 nrows = SPI_processed;
						int c;

						put_be32(tmp4, (uint32)ncols);
						appendBinaryStringInfo(&result_buf, tmp4, 4);
						put_be32(tmp4, (uint32)nrows);
						appendBinaryStringInfo(&result_buf, tmp4, 4);

						for (c = 0; c < ncols; c++)
						{
							Form_pg_attribute att = TupleDescAttr(tdesc, c);
							const char *name = NameStr(att->attname);
							int namelen = strlen(name);

							put_be16(tmp2, (uint16)namelen);
							appendBinaryStringInfo(&result_buf, tmp2, 2);
							appendBinaryStringInfo(&result_buf, name, namelen);
							put_be32(tmp4, (uint32)att->atttypid);
							appendBinaryStringInfo(&result_buf, tmp4, 4);
						}

						for (uint64 row = 0; row < nrows; row++)
						{
							HeapTuple tup = SPI_tuptable->vals[row];

							for (c = 0; c < ncols; c++)
							{
								bool isnull;
								Datum val;
								Oid typid;
								Oid typoutput;
								bool typisvarlena;
								char *outstr;
								int outlen;

								val = SPI_getbinval(tup, tdesc, c + 1, &isnull);
								if (isnull)
								{
									put_be32(tmp4, (uint32)UINT32_MAX);
									appendBinaryStringInfo(&result_buf, tmp4, 4);
									continue;
								}

								typid = TupleDescAttr(tdesc, c)->atttypid;
								getTypeOutputInfo(typid, &typoutput, &typisvarlena);
								outstr = OidOutputFunctionCall(typoutput, val);
								outlen = strlen(outstr);
								put_be32(tmp4, (uint32)outlen);
								appendBinaryStringInfo(&result_buf, tmp4, 4);
								appendBinaryStringInfo(&result_buf, outstr, outlen);
								pfree(outstr);
							}
						}
					}
					else
					{
						/* Non-SELECT */
						put_be32(tmp4, 0);
						appendBinaryStringInfo(&result_buf, tmp4, 4);
						put_be32(tmp4, (uint32)SPI_processed);
						appendBinaryStringInfo(&result_buf, tmp4, 4);
					}
					success = true;
					MUX_LOG("worker %d (pid=%d) session %d completed (spi_ret=%d processed=%llu)",
							worker_id, (int) MyProcPid, session_id, spi_ret,
							(unsigned long long) SPI_processed);
				}

				PopActiveSnapshot();
				SPI_finish();
				CommitTransactionCommand();
			}
			PG_CATCH();
			{
				ErrorData *edata;

				HOLD_INTERRUPTS();
				edata = CopyErrorData();
				FlushErrorState();
				RESUME_INTERRUPTS();

				snprintf(error_msg, sizeof(error_msg), "%s", edata->message);
				FreeErrorData(edata);
				success = false;
				MUX_LOG("worker %d (pid=%d) session %d exception: %s",
						worker_id, (int) MyProcPid, session_id, error_msg);

				AbortCurrentTransaction();
			}
			PG_END_TRY();

			/* Send response: [session_id 4B][type 1B][rlen 4B][data] */
			{
				char rsp_hdr[9];
				shm_mq_iovec iov[2];

				put_be32(rsp_hdr + 0, (uint32)session_id);
				rsp_hdr[4] = success ? 'D' : 'E';

				if (success)
				{
					put_be32(rsp_hdr + 5, (uint32)result_buf.len);
					iov[0].data = rsp_hdr;
					iov[0].len = 9;
					iov[1].data = result_buf.data;
					iov[1].len = result_buf.len;
					(void)shm_mq_sendv(resp_mqh, iov, 2, false, true);
				}
				else
				{
					int elen = strlen(error_msg);

					put_be32(rsp_hdr + 5, (uint32)elen);
					iov[0].data = rsp_hdr;
					iov[0].len = 9;
					iov[1].data = error_msg;
					iov[1].len = elen;
					(void)shm_mq_sendv(resp_mqh, iov, 2, false, true);
				}
			}

			pfree(result_buf.data);
		}

		/* Wake the mux to pick up the response */
		if (MuxState->mux_pid != 0)
			SetLatch(&MuxState->mux_latch);

		SpinLockAcquire(&ws->mutex);
		ws->phase = MWP_IDLE;
		ws->session_id = -1;
		SpinLockRelease(&ws->mutex);

		set_ps_display("idle");
	}

	proc_exit(0);
}
