/*-------------------------------------------------------------------------
 *
 * conn_multiplexer.c
 *	  Connection multiplexer for distributed PostgreSQL transport
 *
 * Architecture:
 *
 *   Each node runs ONE multiplexer process and N strictly-local workers.
 *   There is NO separate networker process.
 *
 *   - Local workers execute sub-statement queries on the CURRENT node via SPI.
 *     They receive the query text and transaction state from the multiplexer
 *     via shm_mq and return serialised results through shm_mq.
 *     Workers NEVER make outbound TCP connections.
 *
 *   - The multiplexer manages ALL TCP sockets to peer multiplexers directly
 *     inside ConnMuxMain.  It listens on mux_tcp_port for incoming peer
 *     connections and connects outbound to peers on demand (lazy).
 *
 *   Multiplexer main loop
 *   ---------------------
 *   On each iteration the multiplexer:
 *     1. Accepts any new inbound peer connections.
 *     2. Drains all local worker result queues and routes replies
 *        (either back to the local backend or via TCP to the requesting peer).
 *     3. Dispatches pending local requests to idle workers.
 *     4. Dispatches pending remote query slots to peer multiplexers via TCP.
 *     5. Re-spawns any dead local workers.
 *
 * Shared memory layout
 * --------------------
 * One MuxSharedState struct is allocated at startup via ShmemInitStruct.
 * Each MuxWorkerSlot within it contains two embedded shm_mq ring buffers
 * (mux_to_worker and worker_to_mux) large enough for MUX_QUEUE_SIZE bytes
 * of payload each.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/conn_multiplexer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <errno.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/conn_multiplexer.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "storage/shm_mq.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"


/* ----------------------------------------------------------------
 * Module-level globals
 * ---------------------------------------------------------------- */

/* GUC: number of worker processes in the pool (1..MUX_MAX_WORKERS) */
int			mux_worker_count = 4;

/*
 * GUC: maximum number of persistent peer connections.
 * When the pool is full and a new remote server needs a slot, the clock-sweep
 * eviction algorithm releases the least-recently-used idle connection.
 */
int			max_mux_connections = 64;

/* GUC: TCP port for inter-multiplexer communication */
int			mux_tcp_port = 7432;

/* Pointer to the shared-memory state; set by ConnMuxShmemInit(). */
static MuxSharedState *MuxState = NULL;

/*
 * Simple pending-request queue for requests that cannot be dispatched
 * immediately because all workers are busy.  Lives in local memory of
 * the multiplexer process.
 */
#define MUX_PENDING_QUEUE_MAX	256

typedef struct MuxPendingRequest
{
	char		data[sizeof(MuxMsgHeader) + 4096];	/* header + payload */
	Size		len;
} MuxPendingRequest;

static MuxPendingRequest pending_requests[MUX_PENDING_QUEUE_MAX];
static int	pending_head = 0;
static int	pending_tail = 0;

/* ----------------------------------------------------------------
 * TCP socket state (process-local, non-shared)
 * ---------------------------------------------------------------- */

/* Listening socket for inbound peer connections */
static int	listen_sock = PGINVALID_SOCKET;

/* Outbound peer sockets, indexed by MuxRemoteConn slot */
static int	outbound_socks[MUX_MAX_REMOTE_CONNS];

/* Inbound peer sockets (peers that connected to us) */
/* 16 peers is sufficient for a small cluster; increase if needed */
#define MUX_MAX_INBOUND_PEERS	16
static int	inbound_socks[MUX_MAX_INBOUND_PEERS];
static int	n_inbound = 0;

/*
 * Tracking for in-flight inbound requests dispatched to local workers.
 * conn_id values >= MUX_INBOUND_CONN_BASE identify inbound (remote-originated)
 * requests; values below that are local backend requests (query slot indices).
 */
#define MUX_INBOUND_CONN_BASE	MUX_MAX_QUERY_SLOTS

typedef struct MuxInboundReq
{
	bool		in_use;
	int			peer_idx;		/* inbound_socks[] index */
	uint32		remote_slot_id; /* slot_id on the sending multiplexer side */
} MuxInboundReq;

static MuxInboundReq inbound_reqs[MUX_MAX_QUERY_SLOTS];


/* ----------------------------------------------------------------
 * Forward declarations
 * ---------------------------------------------------------------- */
static void mux_handle_sigterm(SIGNAL_ARGS);
static void mux_setup_signals(void);
static void mux_spawn_workers(int n);
static int	mux_find_idle_worker(void);
static bool mux_dispatch_to_worker(int worker_id, const char *data, Size len);
static void mux_drain_worker_queues(void);
static void mux_drain_backend_requests(void);
static bool mux_pending_enqueue(const char *data, Size len);
static bool mux_pending_dequeue(char *buf, Size bufsz, Size *lenp);
static void mux_worker_setup_signals(void);

/* TCP helper forward declarations */
static void mux_set_nonblocking(int sock);
static int	mux_init_listen_socket(void);
static int	mux_accept_peer(void);
static int	mux_connect_to_peer(int rc_idx);
static bool mux_send_all(int sock, const void *buf, size_t len);
static bool mux_recv_available(int sock, void *buf, size_t len, size_t *got);
static bool mux_dispatch_remote_slots(void);
static void mux_process_inbound_message(int peer_idx);
static void mux_process_outbound_result(int rc_idx);


/* ----------------------------------------------------------------
 * Shared-memory sizing and initialisation
 * ---------------------------------------------------------------- */

/*
 * ConnMuxShmemSize
 *		Return the amount of shared memory required by the multiplexer.
 */
Size
ConnMuxShmemSize(void)
{
	return sizeof(MuxSharedState);
}

/*
 * ConnMuxShmemInit
 *		Allocate (or attach to) the multiplexer shared-memory region and
 *		initialise it on first call.
 */
void
ConnMuxShmemInit(void)
{
	bool		found;

	MuxState = (MuxSharedState *)
		ShmemInitStruct("Connection Multiplexer State",
						sizeof(MuxSharedState),
						&found);

	if (!found)
	{
		int			i;

		/* First call: initialise everything. */
		MemSet(MuxState, 0, sizeof(MuxSharedState));
		SpinLockInit(&MuxState->mutex);
		MuxState->mux_pid = 0;
		MuxState->mux_latch = NULL;
		MuxState->num_workers = mux_worker_count;

		for (i = 0; i < MUX_MAX_WORKERS; i++)
		{
			MuxWorkerSlot *slot = &MuxState->workers[i];

			SpinLockInit(&slot->mutex);
			slot->worker_id = i;
			slot->pid = 0;
			slot->phase = MUX_WORKER_DEAD;
			slot->worker_latch = NULL;

			/* Initialise the two embedded message queues. */
			shm_mq_create(slot->mux_to_worker_buf, MUX_QUEUE_SIZE);
			shm_mq_create(slot->worker_to_mux_buf, MUX_QUEUE_SIZE);
		}

		for (i = 0; i < MUX_MAX_REMOTE_CONNS; i++)
		{
			MuxRemoteConn *rc = &MuxState->remote_conns[i];

			SpinLockInit(&rc->mutex);
			rc->phase = MUX_CONN_UNUSED;
			rc->conn_id = (uint32) i;
			rc->server_oid = InvalidOid;
			rc->server_name[0] = '\0';
			rc->connstr[0] = '\0';
			rc->peer_host[0] = '\0';
			rc->use_count = 0;	/* 0 means "unused slot, no connection" */
		}

		for (i = 0; i < MUX_MAX_QUERY_SLOTS; i++)
		{
			MuxQuerySlot *qs = &MuxState->query_slots[i];

			SpinLockInit(&qs->mutex);
			qs->in_use = false;
			qs->completed = false;
		}
	}
}


/* ----------------------------------------------------------------
 * GUC definition
 * ---------------------------------------------------------------- */

/*
 * ConnMuxRegister
 *		Register the multiplexer main process as a background worker.
 *		This must be called during postmaster initialisation.
 */
void
ConnMuxRegister(void)
{
	BackgroundWorker bgw;

	MemSet(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_PostmasterStart;
	bgw.bgw_restart_time = 10;	/* restart after 10 s on crash */
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ConnMuxMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "connection multiplexer");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "connection multiplexer");
	bgw.bgw_main_arg = Int32GetDatum(0);
	bgw.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&bgw);
}


/* ----------------------------------------------------------------
 * Signal handling helpers
 * ---------------------------------------------------------------- */

static volatile sig_atomic_t mux_got_sigterm = false;

static void
mux_handle_sigterm(SIGNAL_ARGS)
{
	mux_got_sigterm = true;
	SetLatch(MyLatch);
}

static void
mux_setup_signals(void)
{
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, mux_handle_sigterm);
	/* SIGQUIT default (quick exit) is fine */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);
}


/* ----------------------------------------------------------------
 * Worker spawn / management
 * ---------------------------------------------------------------- */

/*
 * mux_spawn_workers
 *		Launch 'n' pool-worker background workers, claiming slots in the
 *		shared MuxState->workers[] array.
 */
static void
mux_spawn_workers(int n)
{
	int			i;

	for (i = 0; i < n && i < MUX_MAX_WORKERS; i++)
	{
		BackgroundWorker bgw;
		BackgroundWorkerHandle *handle;
		MuxWorkerSlot *slot = &MuxState->workers[i];

		SpinLockAcquire(&slot->mutex);
		if (slot->phase != MUX_WORKER_DEAD)
		{
			SpinLockRelease(&slot->mutex);
			continue;
		}
		slot->phase = MUX_WORKER_STARTING;
		SpinLockRelease(&slot->mutex);

		MemSet(&bgw, 0, sizeof(bgw));
		bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
		bgw.bgw_start_time = BgWorkerStart_PostmasterStart;
		bgw.bgw_restart_time = BGW_NEVER_RESTART;	/* multiplexer re-spawns */
		snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
		snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ConnMuxWorkerMain");
		snprintf(bgw.bgw_name, BGW_MAXLEN, "mux worker %d", i);
		snprintf(bgw.bgw_type, BGW_MAXLEN, "mux worker");
		bgw.bgw_main_arg = Int32GetDatum(i);
		bgw.bgw_notify_pid = MyProcPid;

		if (!RegisterDynamicBackgroundWorker(&bgw, &handle))
		{
			/* Failed to register – mark slot dead again */
			SpinLockAcquire(&slot->mutex);
			slot->phase = MUX_WORKER_DEAD;
			SpinLockRelease(&slot->mutex);
			ereport(WARNING,
					(errmsg("connection multiplexer: could not register worker %d", i)));
		}
		/* We intentionally do not wait for the worker to start here;
		 * the worker will announce itself via its latch. */
	}
}


/* ----------------------------------------------------------------
 * TCP socket helper functions
 * ---------------------------------------------------------------- */

/*
 * mux_set_nonblocking
 *		Set O_NONBLOCK on a socket.
 */
static void
mux_set_nonblocking(int sock)
{
	int			flags;

	flags = fcntl(sock, F_GETFL, 0);
	if (flags < 0)
	{
		ereport(WARNING, (errmsg("mux: fcntl F_GETFL failed: %m")));
		return;
	}
	if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0)
		ereport(WARNING, (errmsg("mux: fcntl F_SETFL failed: %m")));
}

/*
 * mux_init_listen_socket
 *		Create a TCP listening socket on 0.0.0.0:mux_tcp_port.
 *		Returns the socket fd on success, PGINVALID_SOCKET on failure.
 */
static int
mux_init_listen_socket(void)
{
	int			sock;
	int			on = 1;
	struct sockaddr_in addr;

	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0)
	{
		ereport(WARNING, (errmsg("mux: could not create listen socket: %m")));
		return PGINVALID_SOCKET;
	}

	if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0)
		ereport(WARNING, (errmsg("mux: setsockopt SO_REUSEADDR failed: %m")));

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_ANY);
	addr.sin_port = htons((uint16) mux_tcp_port);

	if (bind(sock, (struct sockaddr *) &addr, sizeof(addr)) < 0)
	{
		ereport(WARNING, (errmsg("mux: could not bind listen socket on port %d: %m",
								 mux_tcp_port)));
		close(sock);
		return PGINVALID_SOCKET;
	}

	if (listen(sock, 8) < 0)
	{
		ereport(WARNING, (errmsg("mux: listen() failed: %m")));
		close(sock);
		return PGINVALID_SOCKET;
	}

	mux_set_nonblocking(sock);

	ereport(LOG, (errmsg("mux: listening on port %d", mux_tcp_port)));
	return sock;
}

/*
 * mux_accept_peer
 *		Accept one inbound peer connection and store it in inbound_socks[].
 *		Returns the new fd, or PGINVALID_SOCKET if nothing to accept.
 */
static int
mux_accept_peer(void)
{
	int			fd;
	struct sockaddr_in peer_addr;
	socklen_t	peer_len = sizeof(peer_addr);

	if (n_inbound >= MUX_MAX_INBOUND_PEERS)
	{
		ereport(WARNING, (errmsg("mux: too many inbound peers, ignoring accept")));
		return PGINVALID_SOCKET;
	}

	fd = accept(listen_sock, (struct sockaddr *) &peer_addr, &peer_len);
	if (fd < 0)
	{
		if (errno != EAGAIN && errno != EWOULDBLOCK)
			ereport(WARNING, (errmsg("mux: accept() failed: %m")));
		return PGINVALID_SOCKET;
	}

	mux_set_nonblocking(fd);
	inbound_socks[n_inbound] = fd;
	n_inbound++;

	ereport(DEBUG1, (errmsg("mux: accepted peer connection from %s",
							inet_ntoa(peer_addr.sin_addr))));
	return fd;
}

/*
 * mux_connect_to_peer
 *		Lazily connect to the peer multiplexer for remote_conns[rc_idx].
 *		Returns the socket fd on success, PGINVALID_SOCKET on failure.
 *		Stores the fd in outbound_socks[rc_idx].
 */
static int
mux_connect_to_peer(int rc_idx)
{
	MuxRemoteConn *rc = &MuxState->remote_conns[rc_idx];
	char		host[MUX_PEER_HOST_MAXLEN];
	struct addrinfo hints;
	struct addrinfo *res,
			   *rp;
	char		portstr[16];
	int			sock = PGINVALID_SOCKET;
	int			ret;

	/* Already connected */
	if (outbound_socks[rc_idx] != PGINVALID_SOCKET)
		return outbound_socks[rc_idx];

	SpinLockAcquire(&rc->mutex);
	strlcpy(host, rc->peer_host, sizeof(host));
	SpinLockRelease(&rc->mutex);

	if (host[0] == '\0')
		strlcpy(host, MUX_DEFAULT_PEER_HOST, sizeof(host));

	snprintf(portstr, sizeof(portstr), "%d", mux_tcp_port);

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	ret = getaddrinfo(host, portstr, &hints, &res);
	if (ret != 0)
	{
		ereport(WARNING, (errmsg("mux: getaddrinfo(%s): %s", host,
								 gai_strerror(ret))));
		return PGINVALID_SOCKET;
	}

	for (rp = res; rp != NULL; rp = rp->ai_next)
	{
		sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sock < 0)
			continue;

		if (connect(sock, rp->ai_addr, rp->ai_addrlen) == 0)
			break;				/* connected */

		close(sock);
		sock = PGINVALID_SOCKET;
	}
	freeaddrinfo(res);

	if (sock == PGINVALID_SOCKET)
	{
		ereport(WARNING, (errmsg("mux: could not connect to peer %s:%d: %m",
								 host, mux_tcp_port)));
		return PGINVALID_SOCKET;
	}

	mux_set_nonblocking(sock);
	outbound_socks[rc_idx] = sock;

	ereport(DEBUG1, (errmsg("mux: connected to peer %s:%d (rc_idx=%d)",
							host, mux_tcp_port, rc_idx)));
	return sock;
}

/*
 * mux_send_all
 *		Send exactly 'len' bytes to 'sock'.  Retries on EAGAIN.
 *		Returns true on success, false on error.
 */
static bool
mux_send_all(int sock, const void *buf, size_t len)
{
	const char *p = (const char *) buf;
	size_t		remaining = len;

	while (remaining > 0)
	{
		ssize_t		sent;
		fd_set		wfds;
		struct timeval tv;

		sent = send(sock, p, remaining, 0);
		if (sent > 0)
		{
			p += sent;
			remaining -= sent;
			continue;
		}
		if (sent == 0)
			return false;		/* connection closed */

		if (errno != EAGAIN && errno != EWOULDBLOCK)
		{
			ereport(WARNING, (errmsg("mux: send() failed: %m")));
			return false;
		}

		/* Wait briefly for socket to become writable */
		FD_ZERO(&wfds);
		FD_SET(sock, &wfds);
		tv.tv_sec = 1;
		tv.tv_usec = 0;
		if (select(sock + 1, NULL, &wfds, NULL, &tv) <= 0)
			return false;
	}
	return true;
}

/*
 * mux_recv_available
 *		Non-blocking receive: try to read exactly 'len' bytes from 'sock'.
 *		Sets *got to the number of bytes actually read.
 *		Returns true if all 'len' bytes were read (or if len == 0),
 *		false if the socket would block (partial or zero read) or on error.
 */
static bool
mux_recv_available(int sock, void *buf, size_t len, size_t *got)
{
	char	   *p = (char *) buf;
	size_t		remaining = len;

	*got = 0;
	while (remaining > 0)
	{
		ssize_t		n = recv(sock, p, remaining, 0);

		if (n > 0)
		{
			p += n;
			remaining -= n;
			*got += n;
			continue;
		}
		if (n == 0)
			return false;		/* connection closed */

		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return false;		/* would block */

		ereport(WARNING, (errmsg("mux: recv() failed: %m")));
		return false;
	}
	return true;
}

/*
 * mux_find_remote_conn_idx
 *		Find the MuxRemoteConn slot index for the given server_oid.
 *		Returns -1 if not found.
 */
static int
mux_find_remote_conn_idx(Oid server_oid)
{
	for (int i = 0; i < MUX_MAX_REMOTE_CONNS; i++)
	{
		MuxRemoteConn *rc = &MuxState->remote_conns[i];

		if (rc->phase != MUX_CONN_UNUSED && rc->server_oid == server_oid)
			return i;
	}
	return -1;
}

/*
 * mux_dispatch_remote_slots
 *		Scan MuxQuerySlot[] for pending remote queries and send each one to
 *		the appropriate peer multiplexer via TCP.
 *		Returns true if any slot was dispatched.
 */
static bool
mux_dispatch_remote_slots(void)
{
	bool		any = false;

	for (int q = 0; q < MUX_MAX_QUERY_SLOTS; q++)
	{
		MuxQuerySlot *qs = &MuxState->query_slots[q];
		int			rc_idx;
		int			sock;
		MuxNetMsgHeader nethdr;
		size_t		sql_len;

		SpinLockAcquire(&qs->mutex);
		if (!qs->in_use || qs->completed || qs->server_oid == InvalidOid)
		{
			SpinLockRelease(&qs->mutex);
			continue;
		}
		SpinLockRelease(&qs->mutex);

		/* Find the remote conn slot for this server */
		rc_idx = mux_find_remote_conn_idx(qs->server_oid);
		if (rc_idx < 0)
		{
			/* No remote conn registered yet – skip for now */
			continue;
		}

		sock = mux_connect_to_peer(rc_idx);
		if (sock == PGINVALID_SOCKET)
			continue;

		sql_len = strlen(qs->sql) + 1;	/* include NUL terminator */

		nethdr.magic = MUXNET_MAGIC;
		nethdr.msg_type = MUXNET_MSG_QUERY;
		nethdr.slot_id = (uint32) q;
		nethdr.server_oid = (uint32) qs->server_oid;
		nethdr.payload_len = (uint32) sql_len;
		nethdr.is_error = 0;

		if (!mux_send_all(sock, &nethdr, sizeof(nethdr)) ||
			!mux_send_all(sock, qs->sql, sql_len))
		{
			ereport(WARNING, (errmsg("mux: failed to send query to peer (rc_idx=%d)",
									 rc_idx)));
			close(sock);
			outbound_socks[rc_idx] = PGINVALID_SOCKET;
			continue;
		}

		/*
		 * Mark slot as "dispatched" by setting server_oid to InvalidOid
		 * so we do not re-send it on the next loop iteration.
		 * The slot stays in_use=true until the result arrives.
		 */
		SpinLockAcquire(&qs->mutex);
		qs->server_oid = InvalidOid;	/* prevents re-dispatch */
		SpinLockRelease(&qs->mutex);

		any = true;
	}
	return any;
}

/*
 * mux_find_inbound_req_slot
 *		Find a free slot in inbound_reqs[].  Returns index or -1 if full.
 */
static int
mux_find_inbound_req_slot(void)
{
	for (int i = 0; i < MUX_MAX_QUERY_SLOTS; i++)
	{
		if (!inbound_reqs[i].in_use)
			return i;
	}
	return -1;
}

/*
 * mux_process_inbound_message
 *		Read and process one message from inbound_socks[peer_idx].
 *		For MUXNET_MSG_QUERY: allocate an inbound_req slot, find an idle
 *		local worker, and dispatch the query to it.
 */
static void
mux_process_inbound_message(int peer_idx)
{
	MuxNetMsgHeader nethdr;
	size_t		got;
	char		sql_buf[MUX_SQL_MAXLEN];
	int			req_idx;
	int			worker_id;

	if (!mux_recv_available(inbound_socks[peer_idx], &nethdr, sizeof(nethdr), &got))
		return;					/* nothing available or error */

	if (nethdr.magic != MUXNET_MAGIC)
	{
		ereport(WARNING, (errmsg("mux: bad magic from peer %d", peer_idx)));
		return;
	}

	if (nethdr.msg_type != MUXNET_MSG_QUERY)
	{
		ereport(WARNING, (errmsg("mux: unexpected msg_type %u from peer %d",
								 nethdr.msg_type, peer_idx)));
		return;
	}

	if (nethdr.payload_len == 0 || nethdr.payload_len > MUX_SQL_MAXLEN)
	{
		ereport(WARNING, (errmsg("mux: bad payload_len %u from peer %d",
								 nethdr.payload_len, peer_idx)));
		return;
	}

	/* Read the SQL payload (blocking loop since we got the header) */
	{
		char	   *p = sql_buf;
		size_t		remaining = nethdr.payload_len;

		while (remaining > 0)
		{
			ssize_t		n = recv(inbound_socks[peer_idx], p, remaining, 0);

			if (n <= 0)
			{
				if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK))
				{
					ereport(WARNING, (errmsg("mux: short read of payload from peer %d", peer_idx)));
					return;
				}
				/* Wait briefly for more data rather than busy-spinning */
				{
					fd_set		rfds;
					struct timeval tv;

					FD_ZERO(&rfds);
					FD_SET(inbound_socks[peer_idx], &rfds);
					tv.tv_sec = 1;
					tv.tv_usec = 0;
					if (select(inbound_socks[peer_idx] + 1, &rfds, NULL, NULL, &tv) <= 0)
					{
						ereport(WARNING, (errmsg("mux: timeout reading payload from peer %d", peer_idx)));
						return;
					}
				}
				continue;
			}
			p += n;
			remaining -= n;
		}
	}
	/* Sender already includes the NUL terminator in payload_len */

	/* Allocate an inbound request slot */
	req_idx = mux_find_inbound_req_slot();
	if (req_idx < 0)
	{
		ereport(WARNING, (errmsg("mux: no free inbound request slots")));
		return;
	}

	/* Find an idle local worker */
	worker_id = mux_find_idle_worker();
	if (worker_id < 0)
	{
		ereport(WARNING, (errmsg("mux: no idle worker for inbound query from peer %d", peer_idx)));
		return;
	}

	/* Set up the inbound request tracking */
	inbound_reqs[req_idx].in_use = true;
	inbound_reqs[req_idx].peer_idx = peer_idx;
	inbound_reqs[req_idx].remote_slot_id = nethdr.slot_id;

	/* Build a MuxMsgHeader + SQL payload and dispatch to the worker */
	{
		char		dispatch_buf[sizeof(MuxMsgHeader) + MUX_SQL_MAXLEN];
		MuxMsgHeader *hdr = (MuxMsgHeader *) dispatch_buf;
		char	   *payload = dispatch_buf + sizeof(MuxMsgHeader);
		Size		total_len;

		hdr->msg_type = MUX_MSG_QUERY;
		hdr->conn_id = (uint32)(MUX_INBOUND_CONN_BASE + req_idx);
		hdr->payload_len = nethdr.payload_len;
		hdr->requester_pid = 0;	/* no local backend pid for remote requests */

		memcpy(payload, sql_buf, nethdr.payload_len);
		total_len = sizeof(MuxMsgHeader) + nethdr.payload_len;

		if (!mux_dispatch_to_worker(worker_id, dispatch_buf, total_len))
		{
			ereport(WARNING, (errmsg("mux: dispatch to worker %d failed", worker_id)));
			inbound_reqs[req_idx].in_use = false;
		}
	}
}

/*
 * mux_process_outbound_result
 *		Read and process one result message from outbound_socks[rc_idx].
 *		Look up the query slot, fill the result, and signal the requester.
 */
static void
mux_process_outbound_result(int rc_idx)
{
	MuxNetMsgHeader nethdr;
	size_t		got;
	int			slot_id;
	MuxQuerySlot *qs;

	if (!mux_recv_available(outbound_socks[rc_idx], &nethdr, sizeof(nethdr), &got))
		return;

	if (nethdr.magic != MUXNET_MAGIC)
	{
		ereport(WARNING, (errmsg("mux: bad magic from outbound peer rc_idx=%d", rc_idx)));
		return;
	}

	if (nethdr.msg_type != MUXNET_MSG_RESULT && nethdr.msg_type != MUXNET_MSG_ERROR)
	{
		ereport(WARNING, (errmsg("mux: unexpected result msg_type %u from rc_idx=%d",
								 nethdr.msg_type, rc_idx)));
		return;
	}

	slot_id = (int) nethdr.slot_id;
	if (slot_id < 0 || slot_id >= MUX_MAX_QUERY_SLOTS)
	{
		ereport(WARNING, (errmsg("mux: invalid slot_id %u in result", nethdr.slot_id)));
		return;
	}

	qs = &MuxState->query_slots[slot_id];

	if (nethdr.is_error || nethdr.msg_type == MUXNET_MSG_ERROR)
	{
		qs->is_error = true;
		if (nethdr.payload_len > 0 && nethdr.payload_len < sizeof(qs->error_msg))
		{
			char	   *p = qs->error_msg;
			size_t		remaining = nethdr.payload_len;

			while (remaining > 0)
			{
				ssize_t		n = recv(outbound_socks[rc_idx], p, remaining, 0);

				if (n <= 0)
					break;
				p += n;
				remaining -= n;
			}
			/* NUL-terminate at the actual bytes received, not payload_len */
			*p = '\0';
		}
		else
		{
			strlcpy(qs->error_msg, "remote query failed", sizeof(qs->error_msg));
		}
	}
	else
	{
		/* Read result payload into result_data */
		uint32		plen = nethdr.payload_len;

		qs->is_error = false;
		if (plen > 0 && plen <= MUX_RESULT_MAXLEN)
		{
			char	   *p = qs->result_data;
			size_t		remaining = plen;

			while (remaining > 0)
			{
				ssize_t		n = recv(outbound_socks[rc_idx], p, remaining, 0);

				if (n <= 0)
					break;
				p += n;
				remaining -= n;
			}
			qs->result_len = (int) plen;
		}
	}

	SpinLockAcquire(&qs->mutex);
	qs->completed = true;
	SpinLockRelease(&qs->mutex);

	if (qs->requester_latch)
		SetLatch(qs->requester_latch);
}


/*
 *		Return the index of an IDLE worker slot, or -1 if none are free.
 */
static int
mux_find_idle_worker(void)
{
	int			i;
	int			num = MuxState->num_workers;

	for (i = 0; i < num && i < MUX_MAX_WORKERS; i++)
	{
		MuxWorkerSlot *slot = &MuxState->workers[i];

		if (slot->phase == MUX_WORKER_IDLE)
			return i;
	}
	return -1;
}


/* ----------------------------------------------------------------
 * Message dispatch
 * ---------------------------------------------------------------- */

/*
 * mux_dispatch_to_worker
 *		Write 'len' bytes starting at 'data' into the mux_to_worker queue
 *		of worker[worker_id] and wake the worker.
 *		Returns true on success, false if the queue is full.
 */
static bool
mux_dispatch_to_worker(int worker_id, const char *data, Size len)
{
	MuxWorkerSlot *slot = &MuxState->workers[worker_id];
	shm_mq	   *mq = (shm_mq *) slot->mux_to_worker_buf;
	shm_mq_handle *mqh;
	shm_mq_result result;

	/*
	 * Attach to the queue in the sender role.  We pass NULL for the
	 * BackgroundWorkerHandle since the worker is already running.
	 */
	shm_mq_set_sender(mq, MyProc);
	mqh = shm_mq_attach(mq, NULL, NULL);

	result = shm_mq_send(mqh, len, data, true /* nowait */, true /* flush */);
	shm_mq_detach(mqh);

	if (result != SHM_MQ_SUCCESS)
		return false;

	/* Mark slot as busy */
	SpinLockAcquire(&slot->mutex);
	slot->phase = MUX_WORKER_BUSY;
	slot->last_active = GetCurrentTimestamp();
	SpinLockRelease(&slot->mutex);

	/* Wake the worker */
	if (slot->worker_latch)
		SetLatch(slot->worker_latch);

	return true;
}


/* ----------------------------------------------------------------
 * Per-iteration drain functions
 * ---------------------------------------------------------------- */

/*
 * mux_drain_worker_queues
 *		Poll every active worker's worker_to_mux queue for completed results.
 *		For each complete message, update statistics and (in a full
 *		implementation) route the reply to the requesting backend or remote
 *		socket.
 */
static void
mux_drain_worker_queues(void)
{
	int			i;
	int			num = MuxState->num_workers;

	for (i = 0; i < num && i < MUX_MAX_WORKERS; i++)
	{
		MuxWorkerSlot *slot = &MuxState->workers[i];
		shm_mq	   *mq;
		shm_mq_handle *mqh;
		shm_mq_result result;
		Size		nbytes;
		void	   *data;

		if (slot->phase != MUX_WORKER_BUSY && slot->phase != MUX_WORKER_IDLE)
			continue;

		mq = (shm_mq *) slot->worker_to_mux_buf;
		shm_mq_set_receiver(mq, MyProc);
		mqh = shm_mq_attach(mq, NULL, NULL);

		for (;;)
		{
			result = shm_mq_receive(mqh, &nbytes, &data, true /* nowait */);
			if (result == SHM_MQ_WOULD_BLOCK)
				break;
			if (result == SHM_MQ_DETACHED)
				break;

			/* We have a complete message. */
			if (nbytes >= sizeof(MuxMsgHeader))
			{
				MuxMsgHeader *hdr = (MuxMsgHeader *) data;

				SpinLockAcquire(&slot->mutex);
				slot->requests_completed++;
				if (hdr->msg_type == MUX_MSG_ERROR)
					slot->count_errors++;
				slot->phase = MUX_WORKER_IDLE;
				SpinLockRelease(&slot->mutex);

				SpinLockAcquire(&MuxState->mutex);
				MuxState->total_requests++;
				SpinLockRelease(&MuxState->mutex);

				if (hdr->conn_id >= MUX_INBOUND_CONN_BASE)
				{
					/* This result is for a remote-originated (inbound) request */
					int			req_idx = (int)(hdr->conn_id - MUX_INBOUND_CONN_BASE);

					if (req_idx >= 0 && req_idx < MUX_MAX_QUERY_SLOTS &&
						inbound_reqs[req_idx].in_use)
					{
						int			peer_idx = inbound_reqs[req_idx].peer_idx;
						int			peer_sock;
						MuxNetMsgHeader nethdr;
						const char *payload = (const char *) data + sizeof(MuxMsgHeader);
						uint32		payload_len = (nbytes > sizeof(MuxMsgHeader)) ?
							(uint32)(nbytes - sizeof(MuxMsgHeader)) : 0;

						nethdr.magic = MUXNET_MAGIC;
						nethdr.msg_type = (hdr->msg_type == MUX_MSG_ERROR) ?
							MUXNET_MSG_ERROR : MUXNET_MSG_RESULT;
						nethdr.slot_id = inbound_reqs[req_idx].remote_slot_id;
						nethdr.server_oid = 0;
						nethdr.payload_len = payload_len;
						nethdr.is_error = (hdr->msg_type == MUX_MSG_ERROR) ? 1 : 0;

						if (peer_idx >= 0 && peer_idx < n_inbound)
						{
							peer_sock = inbound_socks[peer_idx];
							if (peer_sock != PGINVALID_SOCKET)
							{
								if (!mux_send_all(peer_sock, &nethdr, sizeof(nethdr)) ||
									(payload_len > 0 &&
									 !mux_send_all(peer_sock, payload, payload_len)))
								{
									ereport(WARNING, (errmsg("mux: failed to send result to peer %d", peer_idx)));
									close(peer_sock);
									inbound_socks[peer_idx] = PGINVALID_SOCKET;
								}
							}
						}

						inbound_reqs[req_idx].in_use = false;
					}
				}
				else
				{
					/* Local backend request: fill the query slot and signal */
					uint32		conn_id = hdr->conn_id;

					if (conn_id < MUX_MAX_QUERY_SLOTS)
					{
						MuxQuerySlot *qs = &MuxState->query_slots[conn_id];

						qs->is_error = (hdr->msg_type == MUX_MSG_ERROR);
						if (nbytes > sizeof(MuxMsgHeader))
						{
							uint32		plen = (uint32)(nbytes - sizeof(MuxMsgHeader));
							const char *payload = (const char *) data + sizeof(MuxMsgHeader);

							if (qs->is_error)
							{
								Size		copy_len = Min(plen, sizeof(qs->error_msg) - 1);

								memcpy(qs->error_msg, payload, copy_len);
								qs->error_msg[copy_len] = '\0';
							}
							else
							{
								Size		copy_len = Min(plen, MUX_RESULT_MAXLEN);

								memcpy(qs->result_data, payload, copy_len);
								qs->result_len = (int) copy_len;
							}
						}

						SpinLockAcquire(&qs->mutex);
						qs->completed = true;
						SpinLockRelease(&qs->mutex);

						if (qs->requester_latch)
							SetLatch(qs->requester_latch);
					}
				}

				ereport(DEBUG2,
						(errmsg("mux: result from worker %d conn_id=%u type=%d",
								i, hdr->conn_id, (int) hdr->msg_type)));

				/* Try to dispatch a pending request to the now-idle worker */
				{
					char		buf[sizeof(MuxMsgHeader) + 4096];
					Size		len;

					if (mux_pending_dequeue(buf, sizeof(buf), &len))
					{
						(void) mux_dispatch_to_worker(i, buf, len);
					}
				}
			}
		}

		shm_mq_detach(mqh);
	}
}

/*
 * mux_drain_backend_requests
 *		In a complete implementation this function would read from the
 *		per-backend request queues.  For simplicity the current version
 *		handles requests submitted via the pending_requests array, which
 *		an external caller fills through ConnMuxSubmitRequest().
 *
 *		It dispatches each pending request to an idle worker (if available).
 */
static void
mux_drain_backend_requests(void)
{
	int			worker_id;
	char		buf[sizeof(MuxMsgHeader) + 4096];
	Size		len;

	while (mux_pending_dequeue(buf, sizeof(buf), &len))
	{
		worker_id = mux_find_idle_worker();
		if (worker_id < 0)
		{
			/* No idle worker – re-queue at the front */
			mux_pending_enqueue(buf, len);
			break;
		}
		(void) mux_dispatch_to_worker(worker_id, buf, len);
	}
}


/* ----------------------------------------------------------------
 * Pending-request queue helpers
 * ---------------------------------------------------------------- */

static bool
mux_pending_enqueue(const char *data, Size len)
{
	int			next_tail;

	if (len > sizeof(pending_requests[0].data))
		return false;

	next_tail = (pending_tail + 1) % MUX_PENDING_QUEUE_MAX;
	if (next_tail == pending_head)
		return false;			/* queue full */

	memcpy(pending_requests[pending_tail].data, data, len);
	pending_requests[pending_tail].len = len;
	pending_tail = next_tail;
	return true;
}

static bool
mux_pending_dequeue(char *buf, Size bufsz, Size *lenp)
{
	Size		len;

	if (pending_head == pending_tail)
		return false;			/* queue empty */

	len = pending_requests[pending_head].len;
	if (len > bufsz)
		return false;

	memcpy(buf, pending_requests[pending_head].data, len);
	*lenp = len;
	pending_head = (pending_head + 1) % MUX_PENDING_QUEUE_MAX;
	return true;
}


/* ----------------------------------------------------------------
 * Multiplexer main entry point
 * ---------------------------------------------------------------- */

/*
 * ConnMuxMain
 *		Entry point for the multiplexer background worker.
 *
 * This function:
 *   1. Initialises shared memory state.
 *   2. Spawns the worker pool.
 *   3. Opens the TCP listening socket.
 *   4. Runs the event loop.
 */
void
ConnMuxMain(Datum main_arg)
{
	MemoryContext mux_context;
	WaitEventSet *wes;
	int			n_events;
	sigjmp_buf	local_sigjmp_buf;
	int			i;

	/* Basic process setup */
	mux_setup_signals();
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	/*
	 * Create a memory context for this process so that we can reset it on
	 * error without affecting TopMemoryContext.
	 */
	mux_context = AllocSetContextCreate(TopMemoryContext,
										"Connection Multiplexer",
										ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(mux_context);

	/* Publish our PID and latch in shared memory */
	SpinLockAcquire(&MuxState->mutex);
	MuxState->mux_pid = MyProcPid;
	MuxState->mux_latch = &MyProc->procLatch;
	MuxState->num_workers = mux_worker_count;
	SpinLockRelease(&MuxState->mutex);

	ereport(LOG,
			(errmsg("connection multiplexer started, worker pool size = %d",
					mux_worker_count)));

	/* Set up the error-recovery jump point */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		error_context_stack = NULL;
		HOLD_INTERRUPTS();
		EmitErrorReport();
		LWLockReleaseAll();
		pgstat_report_wait_end();
		MemoryContextSwitchTo(mux_context);
		FlushErrorState();
		MemoryContextReset(mux_context);
		RESUME_INTERRUPTS();
		pg_usleep(1000000L);	/* 1 s back-off after error */
	}
	PG_exception_stack = &local_sigjmp_buf;

	/* Initialise private TCP socket arrays */
	for (i = 0; i < MUX_MAX_REMOTE_CONNS; i++)
		outbound_socks[i] = PGINVALID_SOCKET;
	for (i = 0; i < MUX_MAX_INBOUND_PEERS; i++)
		inbound_socks[i] = PGINVALID_SOCKET;
	n_inbound = 0;
	memset(inbound_reqs, 0, sizeof(inbound_reqs));

	/* Spawn the initial local worker pool */
	mux_spawn_workers(mux_worker_count);

	/* Open the TCP listening socket */
	listen_sock = mux_init_listen_socket();

	/*
	 * Build the WaitEventSet large enough for:
	 *   2 fixed slots (latch + PM death)
	 *   1 listen socket
	 *   MUX_MAX_REMOTE_CONNS outbound sockets
	 *   MUX_MAX_INBOUND_PEERS inbound sockets
	 */
	wes = CreateWaitEventSet(CurrentResourceOwner,
							 2 + 1 + MUX_MAX_REMOTE_CONNS + MUX_MAX_INBOUND_PEERS);
	AddWaitEventToSet(wes, WL_LATCH_SET, PGINVALID_SOCKET,
					  MyLatch, NULL);
	AddWaitEventToSet(wes, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
					  NULL, NULL);

	/*
	 * Listen socket uses user_data = (void *)(intptr_t)0.
	 * Inbound sockets use user_data = (void *)(intptr_t)(1 + peer_idx).
	 * Outbound sockets use user_data = (void *)(intptr_t)(1 + MUX_MAX_INBOUND_PEERS + rc_idx).
	 * This lets us identify which socket type fired without scanning arrays.
	 */
	if (listen_sock != PGINVALID_SOCKET)
		AddWaitEventToSet(wes, WL_SOCKET_READABLE, listen_sock,
						  NULL, (void *) (intptr_t) 0);

	/* Track which outbound sockets have been added to the WES */
	{
		bool		outbound_in_wes[MUX_MAX_REMOTE_CONNS];

		memset(outbound_in_wes, 0, sizeof(outbound_in_wes));

	set_ps_display("main loop");

	/* ----------------------------------------------------------------
	 * Main event loop
	 * ---------------------------------------------------------------- */
	for (;;)
	{
		WaitEvent	events[32];
		int			j;

		if (mux_got_sigterm)
			break;

		ResetLatch(MyLatch);
		HandleMainLoopInterrupts();

		/* Route completed results back to requesters */
		mux_drain_worker_queues();

		/* Dispatch any pending backend requests to idle local workers */
		mux_drain_backend_requests();

		/* Send pending remote query slots to peer multiplexers via TCP */
		mux_dispatch_remote_slots();

		/*
		 * Register any newly-connected outbound sockets with the WES so
		 * we get notified when results arrive.
		 */
		for (i = 0; i < MUX_MAX_REMOTE_CONNS; i++)
		{
			if (outbound_socks[i] != PGINVALID_SOCKET && !outbound_in_wes[i])
			{
				AddWaitEventToSet(wes, WL_SOCKET_READABLE, outbound_socks[i],
								  NULL, (void *) (intptr_t) (1 + MUX_MAX_INBOUND_PEERS + i));
				outbound_in_wes[i] = true;
			}
			else if (outbound_socks[i] == PGINVALID_SOCKET && outbound_in_wes[i])
			{
				/* Socket was closed — clear the flag (WES entry stays but fd is gone) */
				outbound_in_wes[i] = false;
			}
		}

		/* Re-spawn any dead local workers */
		for (i = 0; i < MuxState->num_workers && i < MUX_MAX_WORKERS; i++)
		{
			MuxWorkerSlot *slot = &MuxState->workers[i];

			if (slot->phase == MUX_WORKER_DEAD)
			{
				ereport(DEBUG1,
						(errmsg("mux: re-spawning worker %d", i)));
				mux_spawn_workers(1);
			}
		}

		/* Sleep until our latch is set, a socket is readable, or 100 ms elapses */
		n_events = WaitEventSetWait(wes, 100 /* ms */, events,
									lengthof(events),
									WAIT_EVENT_CONN_MUX_MAIN);

		for (j = 0; j < n_events; j++)
		{
			WaitEvent  *ev = &events[j];
			intptr_t	ud = (intptr_t) ev->user_data;

			if (ev->events & WL_LATCH_SET)
				continue;		/* handled by ResetLatch above */

			if (!(ev->events & WL_SOCKET_READABLE))
				continue;

			if (ud == 0)
			{
				/* New inbound peer connection on the listen socket */
				int			new_fd = mux_accept_peer();

				if (new_fd != PGINVALID_SOCKET)
				{
					int			peer_idx = n_inbound - 1;

					AddWaitEventToSet(wes, WL_SOCKET_READABLE, new_fd,
									  NULL,
									  (void *) (intptr_t) (1 + peer_idx));
				}
			}
			else if (ud <= MUX_MAX_INBOUND_PEERS)
			{
				/* Inbound peer socket — process incoming query */
				int			peer_idx = (int) (ud - 1);

				if (peer_idx >= 0 && peer_idx < n_inbound)
					mux_process_inbound_message(peer_idx);
			}
			else
			{
				/* Outbound peer socket — process incoming result */
				int			rc_idx = (int) (ud - 1 - MUX_MAX_INBOUND_PEERS);

				if (rc_idx >= 0 && rc_idx < MUX_MAX_REMOTE_CONNS)
					mux_process_outbound_result(rc_idx);
			}
		}
	}

	} /* end outbound_in_wes scope */

	FreeWaitEventSet(wes);

	/* Shutdown: close all TCP sockets and signal workers */
	if (listen_sock != PGINVALID_SOCKET)
	{
		close(listen_sock);
		listen_sock = PGINVALID_SOCKET;
	}
	for (i = 0; i < MUX_MAX_REMOTE_CONNS; i++)
	{
		if (outbound_socks[i] != PGINVALID_SOCKET)
		{
			close(outbound_socks[i]);
			outbound_socks[i] = PGINVALID_SOCKET;
		}
	}
	for (i = 0; i < n_inbound; i++)
	{
		if (inbound_socks[i] != PGINVALID_SOCKET)
		{
			close(inbound_socks[i]);
			inbound_socks[i] = PGINVALID_SOCKET;
		}
	}

	SpinLockAcquire(&MuxState->mutex);
	for (i = 0; i < MUX_MAX_WORKERS; i++)
	{
		MuxWorkerSlot *slot = &MuxState->workers[i];

		if (slot->worker_latch)
			SetLatch(slot->worker_latch);
	}
	MuxState->mux_pid = 0;
	MuxState->mux_latch = NULL;
	SpinLockRelease(&MuxState->mutex);

	ereport(LOG,
			(errmsg("connection multiplexer shutting down")));
	proc_exit(0);
}


/* ----------------------------------------------------------------
 * Worker main entry point
 * ---------------------------------------------------------------- */

static void
mux_worker_setup_signals(void)
{
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, mux_handle_sigterm);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);
}

/*
 * ConnMuxWorkerMain
 *		Entry point for each pool worker background worker.
 *
 * The worker:
 *   1. Claims a slot in MuxState->workers[].
 *   2. Sets up the mux↔worker shared-memory queues.
 *   3. Loops: wait for a request, execute it, send back the result.
 *
 * Workers are stateless – no transaction state is kept between requests.
 * The coordinator transfers any required state via the request payload.
 */
void
ConnMuxWorkerMain(Datum main_arg)
{
	int			worker_id = DatumGetInt32(main_arg);
	MuxWorkerSlot *slot;
	shm_mq	   *req_mq,
			   *res_mq;
	shm_mq_handle *req_mqh,
			   *res_mqh;
	MemoryContext worker_context;
	sigjmp_buf	local_sigjmp_buf;

	if (worker_id < 0 || worker_id >= MUX_MAX_WORKERS)
		ereport(ERROR,
				(errmsg("mux worker started with invalid id %d", worker_id)));

	slot = &MuxState->workers[worker_id];

	mux_worker_setup_signals();
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	worker_context = AllocSetContextCreate(TopMemoryContext,
										   "Mux Worker",
										   ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(worker_context);

	/* Announce ourselves in the shared slot */
	SpinLockAcquire(&slot->mutex);
	slot->pid = MyProcPid;
	slot->worker_latch = &MyProc->procLatch;
	slot->phase = MUX_WORKER_IDLE;
	SpinLockRelease(&slot->mutex);

	/* Wake the multiplexer so it sees the new idle worker */
	ConnMuxWakeup();

	ereport(DEBUG1,
			(errmsg("mux worker %d started (pid %d)", worker_id, MyProcPid)));

	set_ps_display("idle");

	/* Error-recovery entry point */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		error_context_stack = NULL;
		HOLD_INTERRUPTS();
		EmitErrorReport();
		LWLockReleaseAll();
		pgstat_report_wait_end();

		SpinLockAcquire(&slot->mutex);
		slot->count_errors++;
		slot->phase = MUX_WORKER_IDLE;
		SpinLockRelease(&slot->mutex);

		MemoryContextSwitchTo(worker_context);
		FlushErrorState();
		MemoryContextReset(worker_context);
		RESUME_INTERRUPTS();
	}
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Re-initialise the queues for this worker invocation.  We call
	 * shm_mq_create() to reset the ring buffers, then set the sender and
	 * receiver roles.
	 */
	req_mq = shm_mq_create(slot->mux_to_worker_buf, MUX_QUEUE_SIZE);
	res_mq = shm_mq_create(slot->worker_to_mux_buf, MUX_QUEUE_SIZE);

	shm_mq_set_receiver(req_mq, MyProc);
	shm_mq_set_sender(res_mq, MyProc);

	req_mqh = shm_mq_attach(req_mq, NULL, NULL);
	res_mqh = shm_mq_attach(res_mq, NULL, NULL);

	/* ----------------------------------------------------------------
	 * Worker request loop
	 * ---------------------------------------------------------------- */
	for (;;)
	{
		shm_mq_result result;
		Size		nbytes;
		void	   *data;
		MuxMsgHeader *hdr;
		MuxMsgHeader reply_hdr;

		if (mux_got_sigterm)
			break;

		/*
		 * Try to receive a message without blocking first.  If no message is
		 * available, wait on the latch (which the multiplexer will set when
		 * it dispatches a request) before trying again.  Using nowait + latch
		 * ensures SIGTERM is processed promptly.
		 */
		set_ps_display("idle");
		result = shm_mq_receive(req_mqh, &nbytes, &data, true /* nowait */);

		if (result == SHM_MQ_WOULD_BLOCK)
		{
			/* Nothing to do; wait for the mux to wake us up */
			ResetLatch(MyLatch);
			if (mux_got_sigterm)
				break;
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 100 /* ms */,
							 WAIT_EVENT_CONN_MUX_WORKER_MAIN);
			continue;
		}

		if (result == SHM_MQ_DETACHED)
			break;				/* multiplexer shut down */

		if (result != SHM_MQ_SUCCESS)
			continue;

		if (nbytes < sizeof(MuxMsgHeader))
		{
			ereport(WARNING,
					(errmsg("mux worker %d: short message (%zu bytes)", worker_id,
							nbytes)));
			continue;
		}

		hdr = (MuxMsgHeader *) data;

		/* Mark slot as busy */
		SpinLockAcquire(&slot->mutex);
		slot->phase = MUX_WORKER_BUSY;
		slot->current_request_type = hdr->msg_type;
		slot->current_conn_id = hdr->conn_id;
		slot->requester_pid = hdr->requester_pid;
		slot->last_active = GetCurrentTimestamp();
		SpinLockRelease(&slot->mutex);

		set_ps_display("executing");

		/*
		 * Execute the request.
		 *
		 * In the current implementation we perform the minimal processing
		 * to validate the infrastructure.  A complete implementation would
		 * deserialise the payload, set up the transaction context (using the
		 * CSN transferred by the coordinator), execute the sub-statement, and
		 * serialise the result.
		 *
		 * Request types:
		 *   MUX_MSG_QUERY   – execute a query sub-statement
		 *   MUX_MSG_TXSTATE – apply transaction state from coordinator
		 *   MUX_MSG_CLOSE   – release any held resources and become idle
		 */
		switch (hdr->msg_type)
		{
			case MUX_MSG_QUERY:
				SpinLockAcquire(&slot->mutex);
				slot->count_queries++;
				SpinLockRelease(&slot->mutex);
				/* Full impl: deserialise and execute query here */
				break;

			case MUX_MSG_TXSTATE:
				/* Full impl: import CSN snapshot and transaction state */
				break;

			case MUX_MSG_CLOSE:
				SpinLockAcquire(&slot->mutex);
				slot->count_closes++;
				SpinLockRelease(&slot->mutex);
				break;

			default:
				ereport(WARNING,
						(errmsg("mux worker %d: unknown message type %d",
								worker_id, (int) hdr->msg_type)));
				break;
		}

		/*
		 * Send the result back to the multiplexer.
		 * The reply header echoes the conn_id so the mux can route it.
		 */
		reply_hdr.msg_type = MUX_MSG_RESULT;
		reply_hdr.conn_id = hdr->conn_id;
		reply_hdr.payload_len = 0;
		reply_hdr.requester_pid = hdr->requester_pid;

		result = shm_mq_send(res_mqh,
							 sizeof(reply_hdr), &reply_hdr,
							 true /* nowait */,
							 true /* flush */);

		SpinLockAcquire(&slot->mutex);
		slot->requests_completed++;
		slot->phase = MUX_WORKER_IDLE;
		SpinLockRelease(&slot->mutex);

		/* Wake the multiplexer to pick up the result */
		ConnMuxWakeup();

		if (result == SHM_MQ_DETACHED)
			break;
	}

	/* Clean up */
	shm_mq_detach(req_mqh);
	shm_mq_detach(res_mqh);

	SpinLockAcquire(&slot->mutex);
	slot->phase = MUX_WORKER_DEAD;
	slot->pid = 0;
	slot->worker_latch = NULL;
	SpinLockRelease(&slot->mutex);

	ConnMuxWakeup();

	ereport(DEBUG1,
			(errmsg("mux worker %d exiting", worker_id)));
	proc_exit(0);
}


/* ----------------------------------------------------------------
 * Utility functions
 * ---------------------------------------------------------------- */

/*
 * ConnMuxWakeup
 *		Signal the multiplexer process that there is work to do.
 *		Safe to call from any backend or worker process.
 */
void
ConnMuxWakeup(void)
{
	Latch	   *latch;

	SpinLockAcquire(&MuxState->mutex);
	latch = MuxState->mux_latch;
	SpinLockRelease(&MuxState->mutex);

	if (latch)
		SetLatch(latch);
}

/*
 * ConnMuxGetWorkerStats
 *		Copy the current state of up to 'max_slots' worker slots into the
 *		caller-supplied array 'slots'.  Returns the number of slots copied.
 *
 *		Called by the pg_stat_conn_multiplexer view function.
 */
int
ConnMuxGetWorkerStats(MuxWorkerSlot *slots, int max_slots)
{
	int			i;
	int			n = 0;
	int			num;

	if (MuxState == NULL)
		return 0;

	num = Min(MuxState->num_workers, MUX_MAX_WORKERS);
	num = Min(num, max_slots);

	for (i = 0; i < num; i++)
	{
		MuxWorkerSlot *src = &MuxState->workers[i];

		SpinLockAcquire(&src->mutex);
		memcpy(&slots[n], src, sizeof(MuxWorkerSlot));
		SpinLockRelease(&src->mutex);
		/* Clear the spinlock in the copy so callers see a plain struct */
		SpinLockInit(&slots[n].mutex);
		n++;
	}

	return n;
}

/* ----------------------------------------------------------------
 * Remote connection metadata management
 *
 * MuxState->remote_conns[] stores metadata for each registered foreign
 * server (OID, name, connstr, peer_host, use_count).  The actual TCP socket
 * to each peer multiplexer lives in the multiplexer's private process memory.
 * ---------------------------------------------------------------- */

/*
 * mux_find_or_alloc_remote_slot
 *Find an existing remote-conn slot for serverOid, or allocate a new
 *free slot.  Returns the slot index, or -1 if the table is full.
 *Caller must hold MuxState->mutex.
 */
static int
mux_find_or_alloc_remote_slot(Oid serverOid)
{
	int			free_slot = -1;

	for (int i = 0; i < MUX_MAX_REMOTE_CONNS; i++)
	{
		MuxRemoteConn *rc = &MuxState->remote_conns[i];

		if (rc->phase != MUX_CONN_UNUSED && rc->server_oid == serverOid)
			return i;			/* already registered */

		if (rc->phase == MUX_CONN_UNUSED && free_slot < 0)
			free_slot = i;
	}

	return free_slot;
}


/* ----------------------------------------------------------------
 * Public API: foreign server routing
 * ---------------------------------------------------------------- */

/*
 * ConnMuxIsAvailable
 *		Return true if the multiplexer is running and can accept query
 *		routing requests for the given foreign server.
 */
bool
ConnMuxIsAvailable(Oid serverOid)
{
	pid_t		mux_pid;

	if (MuxState == NULL)
		return false;

	SpinLockAcquire(&MuxState->mutex);
	mux_pid = MuxState->mux_pid;
	SpinLockRelease(&MuxState->mutex);

	return mux_pid != 0;
}

/*
 * ConnMuxRegisterServer
 *		Register a foreign server with the multiplexer.  Records the server
 *		metadata in MuxState->remote_conns so the multiplexer can find it
 *		when the first query arrives.
 *
 *		Returns the conn_id (remote-conn slot index) on success, or
 *		(uint32) -1 if no slot is available.
 */
uint32
ConnMuxRegisterServer(Oid serverOid, const char *serverName,
					  const char *connstr, const char *peer_host)
{
	int			slot_idx;

	if (MuxState == NULL)
		return (uint32) -1;

	SpinLockAcquire(&MuxState->mutex);
	slot_idx = mux_find_or_alloc_remote_slot(serverOid);

	if (slot_idx >= 0)
	{
		MuxRemoteConn *rc = &MuxState->remote_conns[slot_idx];

		if (rc->phase == MUX_CONN_UNUSED)
		{
			/* Fresh slot: initialise it */
			rc->server_oid = serverOid;
			strlcpy(rc->server_name, serverName, NAMEDATALEN);
			strlcpy(rc->connstr, connstr, MUX_CONNSTR_MAXLEN);
			strlcpy(rc->peer_host,
					(peer_host && peer_host[0]) ? peer_host : MUX_DEFAULT_PEER_HOST,
					sizeof(rc->peer_host));
			rc->phase = MUX_CONN_CONNECTING;
			rc->use_count = MUX_USE_COUNT_MAX;
		}
		else if (peer_host && peer_host[0])
		{
			/* Update peer_host even if already registered */
			strlcpy(rc->peer_host, peer_host, sizeof(rc->peer_host));
		}
	}
	SpinLockRelease(&MuxState->mutex);

	/* Wake the multiplexer so it notices the new server */
	if (slot_idx >= 0)
		ConnMuxWakeup();

	return slot_idx >= 0 ? (uint32) slot_idx : (uint32) -1;
}

/*
 * mux_acquire_query_slot
 *		Grab a free MuxQuerySlot and mark it in_use.
 *		Returns the slot index, or -1 if all slots are busy.
 */
static int
mux_acquire_query_slot(void)
{
	for (int i = 0; i < MUX_MAX_QUERY_SLOTS; i++)
	{
		MuxQuerySlot *slot = &MuxState->query_slots[i];

		SpinLockAcquire(&slot->mutex);
		if (!slot->in_use)
		{
			slot->in_use = true;
			slot->completed = false;
			slot->is_error = false;
			SpinLockRelease(&slot->mutex);
			return i;
		}
		SpinLockRelease(&slot->mutex);
	}
	return -1;
}

/*
 * mux_wait_for_slot
 *		Wait until MuxState->query_slots[slot_idx].completed is true,
 *		or until a postmaster death / query-cancel signal is received.
 */
static void
mux_wait_for_slot(int slot_idx)
{
	MuxQuerySlot *slot = &MuxState->query_slots[slot_idx];

	while (!slot->completed)
	{
		int			rc;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   1000,	/* 1 s timeout */
					   PG_WAIT_IPC);

		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			ereport(ERROR,
					(errmsg("postmaster died while waiting for multiplexer")));

		CHECK_FOR_INTERRUPTS();
	}
}

/*
 * ConnMuxSubmitQuery
 *		Execute sql on the remote server identified by serverOid by posting
 *		a request to a MuxQuerySlot in shared memory and waking the
 *		multiplexer.  The multiplexer sends the query to the peer node's
 *		multiplexer via a raw TCP socket, which dispatches it to a local
 *		worker, then sends the result back via TCP and signals this backend.
 *
 *		On success returns true and fills result_data / nfields_out /
 *		ntuples_out / truncated_out.  On failure returns false and fills
 *		error_msg.
 */
bool
ConnMuxSubmitQuery(Oid serverOid, const char *sql,
				   char *result_data, int result_data_size,
				   int *nfields_out, int *ntuples_out,
				   bool *truncated_out,
				   char *error_msg, int error_msg_size)
{
	int			slot_idx;
	MuxQuerySlot *slot;

	if (MuxState == NULL)
	{
		snprintf(error_msg, error_msg_size,
				 "connection multiplexer not initialised");
		return false;
	}

	/* Grab a free request slot */
	slot_idx = mux_acquire_query_slot();
	if (slot_idx < 0)
	{
		snprintf(error_msg, error_msg_size,
				 "connection multiplexer: all query slots are busy");
		return false;
	}

	slot = &MuxState->query_slots[slot_idx];

	/* Fill in the request */
	slot->server_oid = serverOid;
	strlcpy(slot->sql, sql, MUX_SQL_MAXLEN);
	slot->requester_pid = MyProcPid;
	slot->requester_latch = MyLatch;

	/* Wake the multiplexer so it dispatches the remote query via TCP */
	ConnMuxWakeup();

	/* Wait for the result to arrive */
	mux_wait_for_slot(slot_idx);

	/* Copy result out */
	if (slot->is_error)
	{
		strlcpy(error_msg, slot->error_msg, error_msg_size);

		SpinLockAcquire(&slot->mutex);
		slot->in_use = false;
		SpinLockRelease(&slot->mutex);

		return false;
	}

	if (result_data && result_data_size > 0)
	{
		int			copy_len = Min(slot->result_len, result_data_size);

		memcpy(result_data, slot->result_data, copy_len);
	}
	if (nfields_out)
		*nfields_out = slot->result_nfields;
	if (ntuples_out)
		*ntuples_out = slot->result_ntuples;
	if (truncated_out)
		*truncated_out = slot->result_truncated;

	/* Release the slot */
	SpinLockAcquire(&slot->mutex);
	slot->in_use = false;
	SpinLockRelease(&slot->mutex);

	return true;
}

/*
 * ConnMuxSendCommand
 *		Send a no-result SQL command (BEGIN, COMMIT, ROLLBACK, etc.) to a
 *		remote server via the multiplexer's peer worker.
 */
bool
ConnMuxSendCommand(Oid serverOid, const char *sql,
				   char *error_msg, int error_msg_size)
{
	int			nfields,
				ntuples;
	bool		truncated;
	char		dummy_buf[1];

	return ConnMuxSubmitQuery(serverOid, sql,
							  dummy_buf, sizeof(dummy_buf),
							  &nfields, &ntuples, &truncated,
							  error_msg, error_msg_size);
}
