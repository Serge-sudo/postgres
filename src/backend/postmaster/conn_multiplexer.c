/*-------------------------------------------------------------------------
 *
 * conn_multiplexer.c
 * Connection multiplexer: transaction-level PG connection pool.
 *
 * Reduces total cluster TCP connections from O(M×N) to O(M+N):
 *   M  = external client connections to this node
 *   N  = number of peer nodes
 *   W  = mux_worker_count (max PG sessions per (db, user) pool)
 *
 * Architecture:
 *   - Local backends connect to mux_tcp_port with a standard PG startup packet
 *     that embeds mux_target_host / mux_target_port in the options string.
 *   - The local mux opens a TCP connection to the remote mux (mux_host:mux_port
 *     from ForeignServer catalog, defaulting to target_host:mux_tcp_port).
 *   - A control protocol (MUX_MSG_CONNECT / TX_BEGIN / TX_END / DISCONNECT)
 *     runs on each per-backend TCP socket between the two mux processes.
 *   - Once TX_BEGIN_OK is received, both sides enter tunnel mode and forward
 *     raw PG wire-protocol bytes between backend and worker.
 *   - TX_END is detected by scanning the remote→local stream for the
 *     ReadyForQuery 'I' pattern (6 bytes: 'Z' 0x00 0x00 0x00 0x05 'I').
 *
 * Control message wire format (all integers big-endian):
 *   [type 1B][channel_id 4B][payload_len 4B][payload payload_len B]
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *  src/backend/postmaster/conn_multiplexer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <unistd.h>

#include "lib/stringinfo.h"
#include "libpq/pqcomm.h"
#include "miscadmin.h"
#include "tcop/tcopprot.h"	
#include "port.h"
#include "postmaster/bgworker.h"
#include "postmaster/conn_multiplexer.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#ifndef MULTIPLEXER_LOG_LEVEL
#define MULTIPLEXER_LOG_LEVEL DEBUG1
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

static const char *
mux_select_pg_host(char *buf, size_t buflen)
{
	if (Unix_socket_directories && Unix_socket_directories[0] != '\0')
	{
		const char *start = Unix_socket_directories;
		const char *end;
		size_t len;

		while (*start && isspace((unsigned char) *start))
			start++;
		end = strchr(start, ',');
		len = end ? (size_t)(end - start) : strlen(start);
		while (len > 0 && isspace((unsigned char) start[len - 1]))
			len--;
		if (len > 0)
		{
			if (len >= buflen)
				len = buflen - 1;
			memcpy(buf, start, len);
			buf[len] = '\0';
			return buf;
		}
	}

	if (ListenAddresses && ListenAddresses[0] != '\0')
	{
		const char *start = ListenAddresses;
		const char *end;
		size_t len;

		while (*start && isspace((unsigned char) *start))
			start++;
		end = strchr(start, ',');
		len = end ? (size_t)(end - start) : strlen(start);
		while (len > 0 && isspace((unsigned char) start[len - 1]))
			len--;
		if (len > 0)
		{
			if (len >= buflen)
				len = buflen - 1;
			memcpy(buf, start, len);
			buf[len] = '\0';
			if (strcmp(buf, "*") == 0 || strcmp(buf, "0.0.0.0") == 0)
				return "127.0.0.1";
			if (strcmp(buf, "::") == 0)
				return "::1";
			return buf;
		}
	}

	return "127.0.0.1";
}

/* -------------------------------------------------------------------------
 * Shared state pointer (set once at shmem init)
 * -------------------------------------------------------------------------
 */
MuxSharedState *MuxState = NULL;
static uint32 mux_we_wait = 0;

/* -------------------------------------------------------------------------
 * Process-local state for ConnMuxMain
 * -------------------------------------------------------------------------
 */
static pgsocket mux_listen_sock = PGINVALID_SOCKET;

/* Channels: local-mux side, one per backend */
static MuxChannelSlot mux_channels[MUX_MAX_CHANNELS];
static int32 mux_next_channel_id = 1;

/* Pending newly-accepted sockets not yet classified */
#define MUX_MAX_PENDING MUX_MAX_CHANNELS
static pgsocket mux_pending[MUX_MAX_PENDING];
static int mux_pending_count = 0;

/* Incoming control connections: remote-mux side, one per peer backend */
static MuxCtrlConn mux_ctrl[MUX_MAX_CTRL_CONNS];

/* Worker pool: remote-mux side, one per PG session */
static MuxWorkerSlot mux_workers[MUX_MAX_WORKERS];
static int mux_n_workers = 0; /* live worker count */

static void ConnMuxPublishStats(void);

/* WaitEventSet user_data encoding */
#define MUX_EVENT_KIND_SHIFT 16
#define MUX_EVENT(kind, idx) \
	((void *)(intptr_t)(((kind) << MUX_EVENT_KIND_SHIFT) | (idx)))
#define MUX_EVENT_KIND(val) \
	(((int)(intptr_t)(val)) >> MUX_EVENT_KIND_SHIFT)
#define MUX_EVENT_IDX(val) \
	(((int)(intptr_t)(val)) & 0xFFFF)

enum
{
	MUX_EV_CHAN_BACKEND = 1, /* channel backend_sock (client side) */
	MUX_EV_CHAN_CTRL = 2,	 /* channel ctrl_sock (to remote mux) */
	MUX_EV_CTRL_SOCK = 3,	 /* incoming ctrl conn (remote mux side) */
	MUX_EV_CTRL_WORKER = 4,	 /* worker_sock (remote mux side) */
};

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
		int n;

		CHECK_FOR_INTERRUPTS();
		n = send(sock, p, remaining, 0);
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
		int n;

		CHECK_FOR_INTERRUPTS();
		n = recv(sock, p, remaining, 0);
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

/*
 * Send a control message on a socket (blocking, for simplicity).
 * Returns false on write error.
 */
static bool
mux_send_ctrl(pgsocket sock, char type, int32 channel_id,
			  const void *payload, int payload_len)
{
	char hdr[MUX_CTRL_HDR_LEN];

	hdr[0] = type;
	put_be32(hdr + 1, (uint32)channel_id);
	put_be32(hdr + 5, (uint32)payload_len);

	if (!mux_write_all(sock, hdr, MUX_CTRL_HDR_LEN))
		return false;
	if (payload_len > 0 && payload != NULL)
		if (!mux_write_all(sock, payload, payload_len))
			return false;
	return true;
}

/*
 * Scan a buffer for the ReadyForQuery 'I' pattern: 'Z' 0x00 0x00 0x00 0x05 'I'
 * Updates *scan_pos_inout with the partial-match state.
 * Returns true if the pattern was found in this buffer.
 */
static bool
mux_scan_rfq_idle(const char *buf, int len, int *scan_pos_inout)
{
	static const unsigned char pattern[6] = {
		'Z', 0x00, 0x00, 0x00, 0x05, 'I'};
	int pos = *scan_pos_inout;

	for (int i = 0; i < len; i++)
	{
		unsigned char c = (unsigned char)buf[i];

		if (c == pattern[pos])
		{
			pos++;
			if (pos == 6)
			{
				*scan_pos_inout = 0;
				return true;
			}
		}
		else if (c == pattern[0])
			pos = 1;
		else
			pos = 0;
	}
	*scan_pos_inout = pos;
	return false;
}

/* =========================================================================
 * Shared memory
 * ========================================================================= */

Size ConnMuxShmemSize(void)
{
	if (mux_worker_count <= 0)
		return 0;
	return MAXALIGN(sizeof(MuxSharedState));
}

void ConnMuxShmemInit(void)
{
	bool found;

	if (mux_worker_count <= 0)
		return;

	MuxState = (MuxSharedState *)
		ShmemInitStruct("Connection Multiplexer Data",
						ConnMuxShmemSize(),
						&found);
	if (!found)
	{
		MemSet(MuxState, 0, sizeof(MuxSharedState));
		InitSharedLatch(&MuxState->mux_latch);
		SpinLockInit(&MuxState->mutex);
		MuxState->mux_pid = 0;
		MuxState->mux_ready = false;
		MemSet(&MuxState->stats, 0, sizeof(MuxState->stats));
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
 * ConnMuxIsAvailable / ConnMuxIsWorkerProcess
 * ========================================================================= */

bool ConnMuxIsAvailable(void)
{
	if (mux_worker_count <= 0)
		return false;
	if (MuxState == NULL)
		return false;
	return MuxState->mux_ready;
}

/*
 * In the new architecture workers are regular PG backends, not background
 * workers tracked in shared memory, so this always returns false.
 */
bool ConnMuxIsWorkerProcess(void)
{
	return false;
}

bool
ConnMuxGetStatsSnapshot(MuxStatsSnapshot *snapshot)
{
	if (snapshot == NULL)
		return false;
	if (mux_worker_count <= 0 || MuxState == NULL)
		return false;

	SpinLockAcquire(&MuxState->mutex);
	memcpy(snapshot, &MuxState->stats, sizeof(*snapshot));
	SpinLockRelease(&MuxState->mutex);

	return true;
}

/* =========================================================================
 * TCP helpers
 * ========================================================================= */

static pgsocket
mux_open_tcp(const char *host, int port)
{
	pgsocket sock;
	struct addrinfo hint;
	struct addrinfo *addrs = NULL;
	char portstr[16];
	int ret;
	int one = 1;

	if (host == NULL || host[0] == '\0')
	{
		elog(WARNING, "connection multiplexer: empty host for connection");
		return PGINVALID_SOCKET;
	}

	if (is_unixsock_path(host))
	{
		struct sockaddr_un addr;
		char unix_socket_path[MAXPGPATH];
		int pathlen;

		pathlen = UNIXSOCK_PATH(unix_socket_path, port, host);
		if (pathlen < 0 || pathlen >= (int) sizeof(addr.sun_path))
		{
			elog(WARNING, "connection multiplexer: unix socket path too long: %s",
				 host);
			return PGINVALID_SOCKET;
		}

		sock = socket(AF_UNIX, SOCK_STREAM, 0);
		if (sock == PGINVALID_SOCKET)
			return PGINVALID_SOCKET;

		MemSet(&addr, 0, sizeof(addr));
		addr.sun_family = AF_UNIX;
		strlcpy(addr.sun_path, unix_socket_path, sizeof(addr.sun_path));

		if (connect(sock, (struct sockaddr *) &addr, sizeof(addr)) < 0)
		{
			closesocket(sock);
			return PGINVALID_SOCKET;
		}

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

/* =========================================================================
 * Channel management (local-mux side)
 * ========================================================================= */

static void
mux_channel_init(MuxChannelSlot *ch)
{
	ch->channel_id = 0;
	ch->state = MCH_EMPTY;
	ch->database[0] = '\0';
	ch->username[0] = '\0';
	ch->target_host[0] = '\0';
	ch->target_port = 0;
	ch->target_mux_port = 0;
	ch->backend_sock = PGINVALID_SOCKET;
	ch->ctrl_sock = PGINVALID_SOCKET;
	ch->startup_len = 0;
	ch->startup_off = 0;
	ch->ctrl_hdr_off = 0;
	ch->ctrl_payload_buf = NULL;
	ch->ctrl_payload_len = 0;
	ch->ctrl_payload_off = 0;
	ch->c2r_len = 0;
	ch->c2r_off = 0;
	ch->r2c_len = 0;
	ch->r2c_off = 0;
	ch->rfq_scan_pos = 0;
	ch->pending_tx_end = false;
}

static void
mux_channel_close(int idx)
{
	MuxChannelSlot *ch = &mux_channels[idx];

	MUX_LOG("channel %d closing (state=%d backend=%d ctrl=%d)",
			ch->channel_id, ch->state,
			(int)ch->backend_sock, (int)ch->ctrl_sock);

	if (ch->backend_sock != PGINVALID_SOCKET)
	{
		closesocket(ch->backend_sock);
		ch->backend_sock = PGINVALID_SOCKET;
	}
	if (ch->ctrl_sock != PGINVALID_SOCKET)
	{
		closesocket(ch->ctrl_sock);
		ch->ctrl_sock = PGINVALID_SOCKET;
	}
	if (ch->ctrl_payload_buf != NULL)
	{
		pfree(ch->ctrl_payload_buf);
		ch->ctrl_payload_buf = NULL;
	}
	mux_channel_init(ch);
}

/*
 * Allocate a new channel slot for a freshly accepted backend socket.
 * Returns the index, or -1 if no free slot.
 */
static int
mux_channel_alloc(pgsocket backend_sock)
{
	for (int i = 0; i < MUX_MAX_CHANNELS; i++)
	{
		if (mux_channels[i].state == MCH_EMPTY)
		{
			mux_channel_init(&mux_channels[i]);
			mux_channels[i].backend_sock = backend_sock;
			mux_channels[i].channel_id = mux_next_channel_id++;
			if (mux_next_channel_id <= 0)
				mux_next_channel_id = 1; /* wrap, skip 0 */
			mux_channels[i].state = MCH_STARTUP;
			return i;
		}
	}
	return -1;
}

/* =========================================================================
 * Startup packet parsing (local-mux side)
 * ========================================================================= */

/*
 * Parse the startup packet in ch->startup_buf and extract:
 *   - target_host, target_port  (from mux_target_host / mux_target_port options)
 *   - database, username
 *   - a "cleaned" startup packet to forward to the remote mux's worker
 *     (mux_target_* stripped, rest kept)
 *
 * Returns false if the packet is malformed.
 * On success, *fwd_buf is a palloc'd buffer and *fwd_len its length.
 */
static bool
mux_parse_startup(MuxChannelSlot *ch,
				  char **fwd_buf, int *fwd_len)
{
	char *buf = ch->startup_buf;
	uint32 proto;
	char *ptr;
	char *end;
	StringInfoData clean_opts;
	StringInfoData newmsg;
	char *options_val = NULL;

	if (ch->startup_len < 8)
		return false;

	proto = get_be32(buf + 4);
	if (proto != MUX_PG_PROTOCOL_V3)
		return false;

	ptr = buf + 8;
	end = buf + ch->startup_len;

	initStringInfo(&newmsg);
	initStringInfo(&clean_opts);

	/* Reserve space for length + protocol version (written at the end) */
	appendBinaryStringInfo(&newmsg, buf, 8);

	ch->target_host[0] = '\0';
	ch->target_port = 0;
	ch->database[0] = '\0';
	ch->username[0] = '\0';

	while (ptr < end && *ptr != '\0')
	{
		char *key = ptr;
		char *val;

		ptr += strlen(ptr) + 1;
		if (ptr >= end)
			goto fail;
		val = ptr;
		ptr += strlen(ptr) + 1;

		if (strcmp(key, "database") == 0)
			strlcpy(ch->database, val, sizeof(ch->database));
		else if (strcmp(key, "user") == 0)
			strlcpy(ch->username, val, sizeof(ch->username));
		else if (strcmp(key, "options") == 0)
			options_val = val;
		else
		{
			appendStringInfoString(&newmsg, key);
			appendStringInfoChar(&newmsg, '\0');
			appendStringInfoString(&newmsg, val);
			appendStringInfoChar(&newmsg, '\0');
		}
	}

	/* Parse the options string to extract mux_target_* */
	if (options_val != NULL)
	{
		char *opts_copy = pstrdup(options_val);
		char *saveptr = NULL;
		char *tok = strtok_r(opts_copy, " ", &saveptr);

		while (tok != NULL)
		{
			if (strcmp(tok, "-c") == 0)
			{
				char *next = strtok_r(NULL, " ", &saveptr);

				if (next == NULL)
					break;
				if (strncmp(next, "mux_target_host=", 16) == 0)
					strlcpy(ch->target_host, next + 16, sizeof(ch->target_host));
				else if (strncmp(next, "mux_target_port=", 16) == 0)
					ch->target_port = atoi(next + 16);
				else if (strncmp(next, "mux_target_mux_port=", 20) == 0)
					ch->target_mux_port = atoi(next + 20);
				else
				{
					/* keep other -c options */
					if (clean_opts.len > 0)
						appendStringInfoChar(&clean_opts, ' ');
					appendStringInfoString(&clean_opts, "-c ");
					appendStringInfoString(&clean_opts, next);
				}
			}
			else if (strncmp(tok, "-cmux_target_host=", 18) == 0)
				strlcpy(ch->target_host, tok + 18, sizeof(ch->target_host));
			else if (strncmp(tok, "-cmux_target_port=", 18) == 0)
				ch->target_port = atoi(tok + 18);
			else if (strncmp(tok, "-cmux_target_mux_port=", 22) == 0)
				ch->target_mux_port = atoi(tok + 22);
			else
			{
				if (clean_opts.len > 0)
					appendStringInfoChar(&clean_opts, ' ');
				appendStringInfoString(&clean_opts, tok);
			}
			tok = strtok_r(NULL, " ", &saveptr);
		}
		pfree(opts_copy);
	}

	if (ch->target_host[0] == '\0' || ch->target_port <= 0)
		goto fail;

	if (clean_opts.len > 0)
	{
		appendStringInfoString(&newmsg, "options");
		appendStringInfoChar(&newmsg, '\0');
		appendStringInfoString(&newmsg, clean_opts.data);
		appendStringInfoChar(&newmsg, '\0');
	}

	appendStringInfoChar(&newmsg, '\0'); /* terminating NUL */

	/* Fix the length field */
	put_be32(newmsg.data, (uint32)newmsg.len);

	*fwd_buf = newmsg.data;
	*fwd_len = newmsg.len;

	if (clean_opts.data)
		pfree(clean_opts.data);
	return true;

fail:
	if (newmsg.data)
		pfree(newmsg.data);
	if (clean_opts.data)
		pfree(clean_opts.data);
	return false;
}

/*
 * Read (more of) the PG startup packet from the backend socket.
 * Returns true while still in progress (no error yet), false on error.
 * When the full packet is available, proceeds to CONNECTING state.
 */
static bool
mux_channel_read_startup(int idx)
{
	MuxChannelSlot *ch = &mux_channels[idx];

	/* First read: get the 4-byte length field */
	if (ch->startup_len == 0)
	{
		int readn = recv(ch->backend_sock,
						 ch->startup_buf + ch->startup_off,
						 4 - ch->startup_off, 0);

		if (readn < 0)
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
				return true;
			return false;
		}
		if (readn == 0)
			return false;
		ch->startup_off += readn;
		if (ch->startup_off < 4)
			return true;
		ch->startup_len = (int)get_be32(ch->startup_buf);
		if (ch->startup_len < 8 || ch->startup_len > MUX_STARTUP_MAX)
			return false;
	}

	/* Read remaining bytes */
	if (ch->startup_off < ch->startup_len)
	{
		int readn = recv(ch->backend_sock,
						 ch->startup_buf + ch->startup_off,
						 ch->startup_len - ch->startup_off, 0);

		if (readn < 0)
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
				return true;
			return false;
		}
		if (readn == 0)
			return false;
		ch->startup_off += readn;
	}

	if (ch->startup_off < ch->startup_len)
		return true; /* not done yet */

	/* Full startup packet received — parse it */
	{
		char *fwd_buf = NULL;
		int fwd_len = 0;
		pgsocket ctrl_sock;
		char connect_payload[NAMEDATALEN * 2 + 4];
		int cp_len = 0;
		uint16 dblen, userlen;
		int remote_mux_port;

		if (!mux_parse_startup(ch, &fwd_buf, &fwd_len))
		{
			MUX_LOG("channel %d startup parse failed", ch->channel_id);
			return false;
		}
		pfree(fwd_buf); /* only needed the parse side-effects */

		/*
		 * Open a TCP connection to the remote mux.
		 * Use target_mux_port if provided via mux_target_mux_port startup
		 * option; fall back to the local mux_tcp_port GUC otherwise.
		 */
		remote_mux_port = (ch->target_mux_port > 0) ? ch->target_mux_port : mux_tcp_port;
		ctrl_sock = mux_open_tcp(ch->target_host, remote_mux_port);
		if (ctrl_sock == PGINVALID_SOCKET)
		{
			MUX_LOG("channel %d: cannot connect to remote mux at %s:%d",
					ch->channel_id, ch->target_host, remote_mux_port);
			return false;
		}
		ch->ctrl_sock = ctrl_sock;

		/*
		 * Send MUX_MSG_CONNECT with payload: [db_len 2B][db][user_len 2B][user]
		 */
		dblen = (uint16)strlen(ch->database);
		userlen = (uint16)strlen(ch->username);
		put_be16(connect_payload + cp_len, dblen);
		cp_len += 2;
		memcpy(connect_payload + cp_len, ch->database, dblen);
		cp_len += dblen;
		put_be16(connect_payload + cp_len, userlen);
		cp_len += 2;
		memcpy(connect_payload + cp_len, ch->username, userlen);
		cp_len += userlen;

		if (!mux_send_ctrl(ctrl_sock, MUX_MSG_CONNECT, ch->channel_id,
						   connect_payload, cp_len))
		{
			MUX_LOG("channel %d: failed to send CONNECT to remote mux", ch->channel_id);
			closesocket(ctrl_sock);
			ch->ctrl_sock = PGINVALID_SOCKET;
			return false;
		}

		ch->ctrl_hdr_off = 0;
		ch->state = MCH_CONNECTING;
		MUX_LOG("channel %d: sent CONNECT for db=%s user=%s to %s:%d",
				ch->channel_id, ch->database, ch->username,
				ch->target_host, remote_mux_port);
	}
	return true;
}

/*
 * Send an ErrorResponse to a backend socket (to reject the connection).
 */
static void
mux_send_error_to_backend(pgsocket sock, const char *sqlstate, const char *msg)
{
	StringInfoData buf;

	initStringInfo(&buf);

	/* ErrorResponse body */
	appendStringInfoChar(&buf, 'S');
	appendStringInfoString(&buf, "FATAL");
	appendStringInfoChar(&buf, '\0');
	appendStringInfoChar(&buf, 'C');
	appendStringInfoString(&buf, sqlstate);
	appendStringInfoChar(&buf, '\0');
	appendStringInfoChar(&buf, 'M');
	appendStringInfoString(&buf, msg);
	appendStringInfoChar(&buf, '\0');
	appendStringInfoChar(&buf, '\0'); /* terminator */

	/* PG message frame: 'E' + 4-byte length (includes itself) + body */
	{
		char hdr[5];
		uint32 msglen = (uint32)buf.len + 4;

		hdr[0] = 'E';
		put_be32(hdr + 1, msglen);
		mux_write_all(sock, hdr, 5);
		mux_write_all(sock, buf.data, buf.len);
	}
	pfree(buf.data);
}

/*
 * Handle readable event on channel ctrl_sock while in MCH_CONNECTING.
 * Reads the control response (CONNECT_OK or CONNECT_FAIL) and acts on it.
 */
static bool
mux_channel_read_ctrl_response(int idx)
{
	MuxChannelSlot *ch = &mux_channels[idx];

	/* Read header */
	if (ch->ctrl_hdr_off < MUX_CTRL_HDR_LEN)
	{
		int readn = recv(ch->ctrl_sock,
						 ch->ctrl_hdr + ch->ctrl_hdr_off,
						 MUX_CTRL_HDR_LEN - ch->ctrl_hdr_off, 0);

		if (readn < 0)
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
				return true;
			return false;
		}
		if (readn == 0)
			return false;
		ch->ctrl_hdr_off += readn;
	}

	if (ch->ctrl_hdr_off < MUX_CTRL_HDR_LEN)
		return true;

	/* Parse header */
	{
		char msg_type = ch->ctrl_hdr[0];
		/* int32 channel_id = (int32) get_be32(ch->ctrl_hdr + 1); */
		int payload_len = (int)get_be32(ch->ctrl_hdr + 5);

		if (payload_len < 0 || payload_len > MUX_CTRL_PAYLOAD_MAX)
			return false;

		/* Allocate payload buffer if needed */
		if (payload_len > 0 && ch->ctrl_payload_buf == NULL)
		{
			ch->ctrl_payload_buf = (char *)palloc(payload_len);
			ch->ctrl_payload_len = payload_len;
			ch->ctrl_payload_off = 0;
		}

		/* Read payload */
		if (ch->ctrl_payload_len > 0 && ch->ctrl_payload_off < ch->ctrl_payload_len)
		{
			int readn = recv(ch->ctrl_sock,
							 ch->ctrl_payload_buf + ch->ctrl_payload_off,
							 ch->ctrl_payload_len - ch->ctrl_payload_off, 0);

			if (readn < 0)
			{
				if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
					return true;
				return false;
			}
			if (readn == 0)
				return false;
			ch->ctrl_payload_off += readn;
		}

		if (ch->ctrl_payload_off < ch->ctrl_payload_len)
			return true; /* still reading payload */

		/* Full control response received */
		if (msg_type == MUX_MSG_CONNECT_OK)
		{
			/*
			 * Payload is the PG startup response bytes.
			 * Forward them verbatim to the backend.
			 */
			if (ch->ctrl_payload_len > 0)
			{
				if (!mux_write_all(ch->backend_sock,
								   ch->ctrl_payload_buf,
								   ch->ctrl_payload_len))
				{
					MUX_LOG("channel %d: failed to forward startup response to backend",
							ch->channel_id);
					return false;
				}
			}
			if (ch->ctrl_payload_buf)
			{
				pfree(ch->ctrl_payload_buf);
				ch->ctrl_payload_buf = NULL;
			}
			ch->ctrl_payload_len = 0;
			ch->ctrl_payload_off = 0;
			ch->ctrl_hdr_off = 0;
			ch->state = MCH_READY;
			ch->rfq_scan_pos = 0;
			ch->pending_tx_end = false;
			MUX_LOG("channel %d: CONNECT_OK, entering READY state", ch->channel_id);
		}
		else if (msg_type == MUX_MSG_CONNECT_FAIL)
		{
			/*
			 * No free worker slot.  Send an error to the backend so that
			 * connection.c can detect it and fall back to a direct connection.
			 */
			MUX_LOG("channel %d: CONNECT_FAIL, sending fallback error to backend",
					ch->channel_id);
			mux_send_error_to_backend(ch->backend_sock,
									  "08006", /* connection_failure */
									  "mux_fallback");
			return false;
		}
		else
		{
			MUX_LOG("channel %d: unexpected ctrl msg type %c in CONNECTING state",
					ch->channel_id, msg_type);
			return false;
		}
	}
	return true;
}

/*
 * Handle TX_BEGIN_OK or TX_BEGIN_WAIT response from the remote mux.
 */
static bool
mux_channel_read_tx_begin_resp(int idx)
{
	MuxChannelSlot *ch = &mux_channels[idx];

	/* Read header */
	if (ch->ctrl_hdr_off < MUX_CTRL_HDR_LEN)
	{
		int readn = recv(ch->ctrl_sock,
						 ch->ctrl_hdr + ch->ctrl_hdr_off,
						 MUX_CTRL_HDR_LEN - ch->ctrl_hdr_off, 0);

		if (readn < 0)
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
				return true;
			return false;
		}
		if (readn == 0)
			return false;
		ch->ctrl_hdr_off += readn;
	}

	if (ch->ctrl_hdr_off < MUX_CTRL_HDR_LEN)
		return true;

	{
		char msg_type = ch->ctrl_hdr[0];
		int payload_len = (int)get_be32(ch->ctrl_hdr + 5);

		/* consume any (unexpected) payload */
		if (payload_len > 0 && payload_len <= MUX_CTRL_PAYLOAD_MAX)
		{
			char dummy[MUX_CTRL_PAYLOAD_MAX];

			mux_read_all(ch->ctrl_sock, dummy, payload_len);
		}

		ch->ctrl_hdr_off = 0;

		if (msg_type == MUX_MSG_TX_BEGIN_OK)
		{
			ch->state = MCH_IN_TX;
			ch->rfq_scan_pos = 0;
			ch->pending_tx_end = false;
			MUX_LOG("channel %d: TX_BEGIN_OK, entering tunnel", ch->channel_id);
		}
		else if (msg_type == MUX_MSG_TX_BEGIN_WAIT)
		{
			/*
			 * No idle worker right now.  Go back to READY and retry when
			 * the remote mux sends TX_BEGIN_OK later (after a TX_END on
			 * another channel frees a worker).
			 */
			ch->state = MCH_READY;
			MUX_LOG("channel %d: TX_BEGIN_WAIT, retrying later", ch->channel_id);
		}
		else
		{
			MUX_LOG("channel %d: unexpected msg %c in TX_PENDING", ch->channel_id, msg_type);
			return false;
		}
	}
	return true;
}

/* =========================================================================
 * Channel event handlers
 * ========================================================================= */

static void
mux_channel_backend_event(int idx, uint32 events)
{
	MuxChannelSlot *ch = &mux_channels[idx];

	if (ch->state == MCH_STARTUP)
	{
		if (events & WL_SOCKET_READABLE)
		{
			if (!mux_channel_read_startup(idx))
			{
				MUX_LOG("channel %d startup failed, closing", ch->channel_id);
				mux_channel_close(idx);
			}
		}
		return;
	}

	if (ch->state == MCH_CONNECTING || ch->state == MCH_TX_PENDING)
		return; /* waiting for ctrl response, not reading backend now */

	if (ch->state != MCH_READY && ch->state != MCH_IN_TX)
		return;

	/* Write buffered r2c data to backend */
	if ((events & WL_SOCKET_WRITEABLE) && ch->r2c_len > 0)
	{
		int written = send(ch->backend_sock,
						   ch->r2c_buf + ch->r2c_off,
						   ch->r2c_len, 0);

		if (written > 0)
		{
			ch->r2c_off += written;
			ch->r2c_len -= written;
			if (ch->r2c_len == 0)
				ch->r2c_off = 0;
		}
		else if (written == 0)
		{
			mux_channel_close(idx);
			return;
		}
		else if (errno != EAGAIN && errno != EWOULDBLOCK &&
				 errno != EINPROGRESS && errno != EALREADY && errno != ENOTCONN)
		{
			mux_channel_close(idx);
			return;
		}

		/*
		 * If TX_END is pending and the r2c buffer just became empty, send
		 * TX_END immediately here without waiting for a ctrl_sock event.
		 * This prevents a stall when the remote mux has already exited
		 * tunnel mode and won't send any more data on ctrl_sock.
		 */
		if (ch->state == MCH_IN_TX && ch->pending_tx_end && ch->r2c_len == 0)
		{
			if (!mux_send_ctrl(ch->ctrl_sock, MUX_MSG_TX_END,
							   ch->channel_id, NULL, 0))
			{
				MUX_LOG("channel %d: failed to send TX_END", ch->channel_id);
				mux_channel_close(idx);
				return;
			}
			ch->pending_tx_end = false;
			ch->state = MCH_READY;
			ch->ctrl_hdr_off = 0;
			ch->c2r_len = 0;
			ch->c2r_off = 0;
			MUX_LOG("channel %d: TX_END sent, back to READY", ch->channel_id);
			return;
		}
	}

	/*
	 * Read new data from backend into c2r buffer.
	 * Skip when TX_END is pending — we must not read (and later forward)
	 * the next query as tunnel data while the remote is in control mode.
	 */
	if ((events & WL_SOCKET_READABLE) && ch->c2r_len < MUX_PROXY_BUF &&
		!ch->pending_tx_end)
	{
		int readn = recv(ch->backend_sock,
						 ch->c2r_buf + ch->c2r_len,
						 MUX_PROXY_BUF - ch->c2r_len, 0);

		if (readn > 0)
		{
			ch->c2r_len += readn;

			if (ch->state == MCH_READY)
			{
				/*
				 * First data from backend after session setup — initiate a
				 * transaction reservation before forwarding.
				 */
				if (!mux_send_ctrl(ch->ctrl_sock, MUX_MSG_TX_BEGIN,
								   ch->channel_id, NULL, 0))
				{
					MUX_LOG("channel %d: failed to send TX_BEGIN", ch->channel_id);
					mux_channel_close(idx);
					return;
				}
				ch->ctrl_hdr_off = 0;
				ch->state = MCH_TX_PENDING;
				MUX_LOG("channel %d: sent TX_BEGIN", ch->channel_id);
			}
		}
		else if (readn == 0)
		{
			/* Backend disconnected */
			if (ch->ctrl_sock != PGINVALID_SOCKET)
				mux_send_ctrl(ch->ctrl_sock, MUX_MSG_DISCONNECT,
							  ch->channel_id, NULL, 0);
			mux_channel_close(idx);
		}
		else if (errno != EAGAIN && errno != EWOULDBLOCK)
		{
			if (ch->ctrl_sock != PGINVALID_SOCKET)
				mux_send_ctrl(ch->ctrl_sock, MUX_MSG_DISCONNECT,
							  ch->channel_id, NULL, 0);
			mux_channel_close(idx);
		}
	}
}

static void
mux_channel_ctrl_event(int idx, uint32 events)
{
	MuxChannelSlot *ch = &mux_channels[idx];

	if (ch->state == MCH_CONNECTING)
	{
		if (events & WL_SOCKET_READABLE)
		{
			if (!mux_channel_read_ctrl_response(idx))
			{
				MUX_LOG("channel %d: ctrl response failed, closing", ch->channel_id);
				mux_channel_close(idx);
			}
		}
		return;
	}

	if (ch->state == MCH_TX_PENDING)
	{
		if (events & WL_SOCKET_READABLE)
		{
			if (!mux_channel_read_tx_begin_resp(idx))
			{
				MUX_LOG("channel %d: TX_BEGIN resp failed, closing", ch->channel_id);
				mux_channel_close(idx);
			}
		}
		return;
	}

	if (ch->state == MCH_IN_TX)
	{
		/*
		 * Write buffered c2r data to remote worker (via ctrl_sock).
		 * Stop forwarding once TX_END is pending — the remote has already
		 * exited tunnel mode and would misinterpret any further bytes as a
		 * control message.
		 */
		if ((events & WL_SOCKET_WRITEABLE) && ch->c2r_len > 0 &&
			!ch->pending_tx_end)
		{
			int written = send(ch->ctrl_sock,
							   ch->c2r_buf + ch->c2r_off,
							   ch->c2r_len, 0);

			if (written > 0)
			{
				ch->c2r_off += written;
				ch->c2r_len -= written;
				if (ch->c2r_len == 0)
					ch->c2r_off = 0;
			}
			else if (written == 0)
			{
				mux_channel_close(idx);
				return;
			}
			else if (errno != EAGAIN && errno != EWOULDBLOCK &&
					 errno != EINPROGRESS && errno != EALREADY && errno != ENOTCONN)
			{
				mux_channel_close(idx);
				return;
			}
		}

		/* Read data from remote (ctrl_sock) into r2c buffer */
		if ((events & WL_SOCKET_READABLE) && ch->r2c_len < MUX_PROXY_BUF)
		{
			int readn = recv(ch->ctrl_sock,
							 ch->r2c_buf + ch->r2c_len,
							 MUX_PROXY_BUF - ch->r2c_len, 0);

			if (readn > 0)
			{
				bool found_rfq;

				/* Scan new bytes for ReadyForQuery 'I' */
				found_rfq = mux_scan_rfq_idle(ch->r2c_buf + ch->r2c_len,
											  readn,
											  &ch->rfq_scan_pos);
				ch->r2c_len += readn;

				if (found_rfq)
				{
					/*
					 * Transaction ended.  Send TX_END to remote mux after we
					 * have flushed the r2c buffer to the backend.
					 */
					ch->pending_tx_end = true;
					MUX_LOG("channel %d: detected RFQ 'I', pending TX_END",
							ch->channel_id);
				}
			}
			else if (readn == 0)
			{
				mux_channel_close(idx);
				return;
			}
			else if (errno != EAGAIN && errno != EWOULDBLOCK)
			{
				mux_channel_close(idx);
				return;
			}
		}

		/*
		 * If TX_END is pending and the r2c buffer has been flushed, send it.
		 */
		if (ch->pending_tx_end && ch->r2c_len == 0)
		{
			if (!mux_send_ctrl(ch->ctrl_sock, MUX_MSG_TX_END,
							   ch->channel_id, NULL, 0))
			{
				MUX_LOG("channel %d: failed to send TX_END", ch->channel_id);
				mux_channel_close(idx);
				return;
			}
			ch->pending_tx_end = false;
			ch->state = MCH_READY;
			ch->ctrl_hdr_off = 0;
			ch->c2r_len = 0;
			ch->c2r_off = 0;
			MUX_LOG("channel %d: TX_END sent, back to READY", ch->channel_id);
		}
	}
}

/* =========================================================================
 * Worker management (remote-mux side)
 * ========================================================================= */

static void
mux_worker_init(MuxWorkerSlot *w)
{
	w->worker_sock = PGINVALID_SOCKET;
	w->worker_pid = 0;
	w->database[0] = '\0';
	w->username[0] = '\0';
	w->connect_cnt = 0;
	w->in_tx = false;
	w->active_channel = -1;
	w->startup_resp_len = 0;
}

/*
 * Blocking connect to the local PG server and perform PG startup protocol.
 * Returns true on success; startup response bytes stored in w->startup_resp.
 */
static bool
mux_spawn_worker(MuxWorkerSlot *w, const char *database, const char *username,
				 const char *pg_host, int pg_port)
{
	pgsocket sock;
	StringInfoData startup;
	uint32 proto = MUX_PG_PROTOCOL_V3;
	char lenbuf[4];
	char msgtype;
	char hdr[4];
	int msglen;
	StringInfoData resp_buf;

	sock = mux_open_tcp(pg_host, pg_port);
	if (sock == PGINVALID_SOCKET)
	{
		elog(WARNING, "connection multiplexer: worker: cannot connect to %s:%d",
			 pg_host, pg_port);
		return false;
	}

	/* Make it blocking for the handshake */
	{
		int flags = fcntl(sock, F_GETFL, 0);
		if (flags >= 0)
			fcntl(sock, F_SETFL, flags & ~O_NONBLOCK);
	}

	/* Build PG v3 startup packet */
	initStringInfo(&startup);
	appendBinaryStringInfo(&startup, "\x00\x00\x00\x00", 4); /* length placeholder */
	{
		char proto_bytes[4];
		put_be32(proto_bytes, proto);
		appendBinaryStringInfo(&startup, proto_bytes, 4);
	}
	appendStringInfoString(&startup, "user");
	appendStringInfoChar(&startup, '\0');
	appendStringInfoString(&startup, username);
	appendStringInfoChar(&startup, '\0');
	appendStringInfoString(&startup, "database");
	appendStringInfoChar(&startup, '\0');
	appendStringInfoString(&startup, database);
	appendStringInfoChar(&startup, '\0');
	appendStringInfoString(&startup, "application_name");
	appendStringInfoChar(&startup, '\0');
	appendStringInfoString(&startup, "mux_worker");
	appendStringInfoChar(&startup, '\0');
	appendStringInfoChar(&startup, '\0'); /* terminator */
	put_be32(startup.data, (uint32)startup.len);

	if (!mux_write_all(sock, startup.data, startup.len))
	{
		pfree(startup.data);
		closesocket(sock);
		return false;
	}
	pfree(startup.data);

	/*
	 * Read startup response messages until ReadyForQuery.
	 * We capture all bytes so we can replay them for future connections.
	 */
	initStringInfo(&resp_buf);

	for (;;)
	{
		char payloadbuf[8192];

		if (!mux_read_all(sock, &msgtype, 1))
			goto fail;

		if (!mux_read_all(sock, hdr, 4))
			goto fail;

		msglen = (int)get_be32(hdr) - 4; /* payload length after the 4-byte len field */
		if (msglen < 0)
			goto fail;
		if (msglen > (int)sizeof(payloadbuf))
			goto fail;

		if (msglen > 0 && !mux_read_all(sock, payloadbuf, msglen))
			goto fail;

		/* Append full message to resp_buf for replay */
		appendStringInfoChar(&resp_buf, msgtype);
		appendBinaryStringInfo(&resp_buf, hdr, 4);
		if (msglen > 0)
			appendBinaryStringInfo(&resp_buf, payloadbuf, msglen);

		if (msgtype == 'R')
		{
			/* Authentication */
			if (msglen >= 4)
			{
				uint32 auth_type = get_be32(payloadbuf);

				if (auth_type == 0)
				{
					/* AuthenticationOK — continue */
				}
				else
				{
					/*
					 * Auth challenge received.  For now we only support
					 * trust authentication.  More complex auth would require
					 * passwords passed in the CONNECT payload.
					 */
					elog(WARNING,
						 "connection multiplexer: worker: unsupported auth type %u for %s@%s",
						 auth_type, username, database);
					goto fail;
				}
			}
		}
		else if (msgtype == 'E')
		{
			/* ErrorResponse */
			elog(WARNING, "connection multiplexer: worker: PG error during startup");
			goto fail;
		}
		else if (msgtype == 'Z')
		{
			/* ReadyForQuery — startup complete */
			break;
		}
		else if (msgtype == 'K')
		{
			/* BackendKeyData: pid + cancel key */
			if (msglen >= 8)
				w->worker_pid = (pid_t) get_be32(payloadbuf);
		}
		/* Skip S (ParameterStatus) and N (NoticeResponse) */
	}

	/* Store the startup response for replay */
	if (resp_buf.len > MUX_STARTUP_RESP_MAX)
		resp_buf.len = MUX_STARTUP_RESP_MAX; /* truncate if too big */
	memcpy(w->startup_resp, resp_buf.data, resp_buf.len);
	w->startup_resp_len = resp_buf.len;
	pfree(resp_buf.data);

	strlcpy(w->database, database, sizeof(w->database));
	strlcpy(w->username, username, sizeof(w->username));
	w->worker_sock = sock;
	w->connect_cnt = 1;
	w->in_tx = false;
	w->active_channel = -1;

	/* Make it non-blocking again for event-loop use */
	if (!pg_set_noblock(sock))
	{
		closesocket(sock);
		w->worker_sock = PGINVALID_SOCKET;
		return false;
	}

	mux_n_workers++;
	MUX_LOG("worker spawned for db=%s user=%s (total=%d)", database, username, mux_n_workers);
	return true;

fail:
	if (resp_buf.data)
		pfree(resp_buf.data);
	closesocket(sock);
	return false;
}

/*
 * Find or create a worker for (database, username).
 * Returns the worker index on success, -1 if at capacity.
 * Increments connect_cnt of the assigned worker.
 */
static int
mux_find_or_spawn_worker(const char *database, const char *username)
{
	int i;
	int free_slot = -1;
	char pg_host_buf[MAXPGPATH];
	const char *pg_host;

	/* Find an existing worker for this (db, user) */
	for (i = 0; i < MUX_MAX_WORKERS; i++)
	{
		MuxWorkerSlot *w = &mux_workers[i];

		if (w->worker_sock == PGINVALID_SOCKET)
		{
			if (free_slot < 0)
				free_slot = i;
			continue;
		}
		if (strcmp(w->database, database) == 0 &&
			strcmp(w->username, username) == 0)
		{
			w->connect_cnt++;
			MUX_LOG("reusing worker %d for db=%s user=%s (cnt=%d)",
					i, database, username, w->connect_cnt);
			return i;
		}
	}

	/* No existing worker — need to spawn one */
	if (mux_n_workers >= mux_worker_count || free_slot < 0)
		return -1;
	mux_worker_init(&mux_workers[free_slot]);
	pg_host = mux_select_pg_host(pg_host_buf, sizeof(pg_host_buf));
	if (!mux_spawn_worker(&mux_workers[free_slot],
						  database, username,
						  pg_host, PostPortNumber))
		return -1;

	return free_slot;
}

/* =========================================================================
 * Ctrl connection management (remote-mux side)
 * ========================================================================= */

static void
mux_ctrl_init(MuxCtrlConn *cc)
{
	cc->ctrl_sock = PGINVALID_SOCKET;
	cc->state = MCC_EMPTY;
	cc->hdr_off = 0;
	cc->msg_type = 0;
	cc->channel_id = -1;
	cc->payload_len = 0;
	cc->payload_off = 0;
	cc->worker_idx = -1;
	cc->database[0] = '\0';
	cc->username[0] = '\0';
	cc->c2w_len = 0;
	cc->c2w_off = 0;
	cc->w2c_len = 0;
	cc->w2c_off = 0;
	cc->rfq_scan_pos = 0;
	cc->rfq_detected = false;
	cc->send_len = 0;
	cc->send_off = 0;
}

static void
mux_ctrl_close(int idx)
{
	MuxCtrlConn *cc = &mux_ctrl[idx];
	int widx = cc->worker_idx;

	MUX_LOG("ctrl conn %d closing (channel=%d worker=%d)", idx, cc->channel_id, widx);

	if (cc->ctrl_sock != PGINVALID_SOCKET)
	{
		closesocket(cc->ctrl_sock);
		cc->ctrl_sock = PGINVALID_SOCKET;
	}

	/* Release worker if one was assigned */
	if (widx >= 0 && widx < MUX_MAX_WORKERS &&
		mux_workers[widx].worker_sock != PGINVALID_SOCKET)
	{
		MuxWorkerSlot *w = &mux_workers[widx];

		if (w->in_tx && w->active_channel == cc->channel_id)
		{
			w->in_tx = false;
			w->active_channel = -1;
		}
		w->connect_cnt--;
		if (w->connect_cnt <= 0)
		{
			MUX_LOG("closing worker %d (db=%s user=%s)", widx, w->database, w->username);
			closesocket(w->worker_sock);
			mux_worker_init(w);
			mux_n_workers--;
		}
	}

	mux_ctrl_init(cc);
}

/*
 * Allocate a ctrl conn slot for a newly accepted ctrl socket.
 * Returns the index or -1.
 */
static int
mux_ctrl_alloc(pgsocket ctrl_sock)
{
	for (int i = 0; i < MUX_MAX_CTRL_CONNS; i++)
	{
		if (mux_ctrl[i].state == MCC_EMPTY)
		{
			mux_ctrl_init(&mux_ctrl[i]);
			mux_ctrl[i].ctrl_sock = ctrl_sock;
			mux_ctrl[i].state = MCC_READING_HDR;
			return i;
		}
	}
	return -1;
}

/*
 * Queue a control message to send on a ctrl conn.
 */
static void
mux_ctrl_queue_send(MuxCtrlConn *cc, char type, int32 channel_id,
					const void *payload, int payload_len)
{
	char *p;

	/* If there's already pending data, we can't queue more safely.
	 * For simplicity in this prototype, just send synchronously. */
	if (cc->send_len > 0)
	{
		/* Flush existing */
		mux_write_all(cc->ctrl_sock, cc->send_buf + cc->send_off, cc->send_len);
		cc->send_len = 0;
		cc->send_off = 0;
	}

	if (MUX_CTRL_HDR_LEN + payload_len > (int)sizeof(cc->send_buf))
		return; /* shouldn't happen */

	p = cc->send_buf;
	p[0] = type;
	put_be32(p + 1, (uint32)channel_id);
	put_be32(p + 5, (uint32)payload_len);
	if (payload_len > 0 && payload != NULL)
		memcpy(p + MUX_CTRL_HDR_LEN, payload, payload_len);

	cc->send_len = MUX_CTRL_HDR_LEN + payload_len;
	cc->send_off = 0;
}

/*
 * Process a fully received control message on a ctrl conn (remote-mux side).
 */
static void
mux_ctrl_handle_msg(int idx)
{
	MuxCtrlConn *cc = &mux_ctrl[idx];
	char type = cc->msg_type;
	int32 channel_id = cc->channel_id;

	MUX_LOG("ctrl %d: msg type='%c' channel=%d payload_len=%d",
			idx, type, channel_id, cc->payload_len);

	switch (type)
	{
	case MUX_MSG_CONNECT:
	{
		const char *p = cc->payload_buf;
		const char *end = p + cc->payload_len;
		uint16 dblen,
			userlen;
		char database[NAMEDATALEN];
		char username[NAMEDATALEN];
		int widx;

		if (end - p < 4)
		{
			mux_ctrl_queue_send(cc, MUX_MSG_CONNECT_FAIL, channel_id, NULL, 0);
			break;
		}
		dblen = get_be16(p);
		p += 2;
		if (end - p < dblen + 2 || dblen >= NAMEDATALEN)
		{
			mux_ctrl_queue_send(cc, MUX_MSG_CONNECT_FAIL, channel_id, NULL, 0);
			break;
		}
		memcpy(database, p, dblen);
		database[dblen] = '\0';
		p += dblen;

		userlen = get_be16(p);
		p += 2;
		if (end - p < userlen || userlen >= NAMEDATALEN)
		{
			mux_ctrl_queue_send(cc, MUX_MSG_CONNECT_FAIL, channel_id, NULL, 0);
			break;
		}
		memcpy(username, p, userlen);
		username[userlen] = '\0';

		strlcpy(cc->database, database, sizeof(cc->database));
		strlcpy(cc->username, username, sizeof(cc->username));
		cc->channel_id = channel_id;

		widx = mux_find_or_spawn_worker(database, username);
		if (widx < 0)
		{
			MUX_LOG("ctrl %d: no free worker slot for db=%s user=%s",
					idx, database, username);
			mux_ctrl_queue_send(cc, MUX_MSG_CONNECT_FAIL, channel_id, NULL, 0);
			break;
		}

		cc->worker_idx = widx;

		/* Send CONNECT_OK with stored startup response as payload */
		mux_ctrl_queue_send(cc, MUX_MSG_CONNECT_OK, channel_id,
							mux_workers[widx].startup_resp,
							mux_workers[widx].startup_resp_len);
		break;
	}

	case MUX_MSG_TX_BEGIN:
	{
		int widx = cc->worker_idx;

		if (widx < 0 || widx >= MUX_MAX_WORKERS ||
			mux_workers[widx].worker_sock == PGINVALID_SOCKET)
		{
			MUX_LOG("ctrl %d: TX_BEGIN with no worker assigned", idx);
			mux_ctrl_close(idx);
			return;
		}

		if (mux_workers[widx].in_tx)
		{
			/* Worker is busy — tell local mux to wait */
			mux_ctrl_queue_send(cc, MUX_MSG_TX_BEGIN_WAIT, channel_id, NULL, 0);
		}
		else
		{
			mux_workers[widx].in_tx = true;
			mux_workers[widx].active_channel = channel_id;
			cc->state = MCC_IN_TX;
			cc->rfq_scan_pos = 0;
			cc->c2w_len = 0;
			cc->c2w_off = 0;
			cc->w2c_len = 0;
			cc->w2c_off = 0;
			mux_ctrl_queue_send(cc, MUX_MSG_TX_BEGIN_OK, channel_id, NULL, 0);
			MUX_LOG("ctrl %d: TX_BEGIN_OK, tunnel open (worker %d)", idx, widx);
		}
		break;
	}

	case MUX_MSG_TX_END:
	{
		int widx = cc->worker_idx;

		if (widx >= 0 && widx < MUX_MAX_WORKERS &&
			mux_workers[widx].in_tx &&
			mux_workers[widx].active_channel == channel_id)
		{
			mux_workers[widx].in_tx = false;
			mux_workers[widx].active_channel = -1;
			MUX_LOG("ctrl %d: TX_END, worker %d released", idx, widx);
		}
		cc->state = MCC_READING_HDR;
		cc->hdr_off = 0;
		break;
	}

	case MUX_MSG_DISCONNECT:
	{
		mux_ctrl_close(idx);
		return;
	}

	case MUX_MSG_PING:
		mux_ctrl_queue_send(cc, MUX_MSG_PONG, channel_id, NULL, 0);
		break;

	case MUX_MSG_PONG:
		break;

	default:
		MUX_LOG("ctrl %d: unknown msg type '%c'", idx, type);
		mux_ctrl_close(idx);
		return;
	}

	/* Flush any queued send */
	if (cc->ctrl_sock != PGINVALID_SOCKET && cc->send_len > 0)
	{
		mux_write_all(cc->ctrl_sock,
					  cc->send_buf + cc->send_off,
					  cc->send_len);
		cc->send_len = 0;
		cc->send_off = 0;
	}
}

/*
 * Event on the ctrl_sock of a ctrl connection (remote-mux side).
 * Reads control messages or forwards tunnel data.
 */
static void
mux_ctrl_event(int idx, uint32 events)
{
	MuxCtrlConn *cc = &mux_ctrl[idx];

	if (cc->state == MCC_IN_TX)
	{
		/* Write w2c buffer to ctrl_sock (worker→frontend direction) */
		if ((events & WL_SOCKET_WRITEABLE) && cc->w2c_len > 0)
		{
			int written = send(cc->ctrl_sock,
							   cc->w2c_buf + cc->w2c_off,
							   cc->w2c_len, 0);

			if (written > 0)
			{
				cc->w2c_off += written;
				cc->w2c_len -= written;
				if (cc->w2c_len == 0)
					cc->w2c_off = 0;
			}
			else if (written == 0 || (errno != EAGAIN && errno != EWOULDBLOCK &&
									  errno != EINPROGRESS && errno != EALREADY &&
									  errno != ENOTCONN))
			{
				mux_ctrl_close(idx);
				return;
			}
		}

		/*
		 * If RFQ was detected and w2c_buf is now empty, exit tunnel mode.
		 * The local mux will have received the RFQ and will send TX_END as
		 * the next control message on ctrl_sock.
		 */
		if (cc->rfq_detected && cc->w2c_len == 0)
		{
			MUX_LOG("ctrl %d: w2c flushed after RFQ, exiting tunnel", idx);
			cc->rfq_detected = false;
			cc->rfq_scan_pos = 0;
			cc->state = MCC_READING_HDR;
			cc->hdr_off = 0;
			/* Fall through to control-message reading below */
		}
		else
		{
			/* Still in tunnel mode: read frontend data into c2w buffer */
			if (!cc->rfq_detected &&
				(events & WL_SOCKET_READABLE) &&
				cc->c2w_len < MUX_PROXY_BUF)
			{
				int readn = recv(cc->ctrl_sock,
								 cc->c2w_buf + cc->c2w_len,
								 MUX_PROXY_BUF - cc->c2w_len, 0);

				if (readn > 0)
					cc->c2w_len += readn;
				else if (readn == 0)
				{
					mux_ctrl_close(idx);
					return;
				}
				else if (errno != EAGAIN && errno != EWOULDBLOCK)
				{
					mux_ctrl_close(idx);
					return;
				}
			}
			return;
		}
	}

	/* Control mode: read messages */
	if (cc->state == MCC_READING_HDR)
	{
		if (!(events & WL_SOCKET_READABLE))
			return;

		if (cc->hdr_off < MUX_CTRL_HDR_LEN)
		{
			int readn = recv(cc->ctrl_sock,
							 cc->hdr_buf + cc->hdr_off,
							 MUX_CTRL_HDR_LEN - cc->hdr_off, 0);

			if (readn < 0)
			{
				if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
					return;
				mux_ctrl_close(idx);
				return;
			}
			if (readn == 0)
			{
				mux_ctrl_close(idx);
				return;
			}
			cc->hdr_off += readn;
		}

		if (cc->hdr_off < MUX_CTRL_HDR_LEN)
			return;

		cc->msg_type = cc->hdr_buf[0];
		cc->channel_id = (int32)get_be32(cc->hdr_buf + 1);
		cc->payload_len = (int)get_be32(cc->hdr_buf + 5);

		if (cc->payload_len < 0 || cc->payload_len > MUX_CTRL_PAYLOAD_MAX)
		{
			mux_ctrl_close(idx);
			return;
		}

		if (cc->payload_len == 0)
		{
			/* No payload — process immediately */
			cc->hdr_off = 0;
			mux_ctrl_handle_msg(idx);
			return;
		}

		cc->payload_off = 0;
		cc->state = MCC_READING_PAYLOAD;
	}

	if (cc->state == MCC_READING_PAYLOAD)
	{
		if (!(events & WL_SOCKET_READABLE))
			return;

		while (cc->payload_off < cc->payload_len)
		{
			int readn = recv(cc->ctrl_sock,
							 cc->payload_buf + cc->payload_off,
							 cc->payload_len - cc->payload_off, 0);

			if (readn < 0)
			{
				if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
					return;
				mux_ctrl_close(idx);
				return;
			}
			if (readn == 0)
			{
				mux_ctrl_close(idx);
				return;
			}
			cc->payload_off += readn;
		}

		/* Full payload received */
		cc->state = MCC_READING_HDR;
		cc->hdr_off = 0;
		mux_ctrl_handle_msg(idx);
	}
}

/*
 * Event on the worker_sock of a ctrl connection (remote-mux side).
 * Only relevant when in MCC_IN_TX (tunnel mode).
 */
static void
mux_ctrl_worker_event(int idx, uint32 events)
{
	MuxCtrlConn *cc = &mux_ctrl[idx];
	int widx = cc->worker_idx;
	MuxWorkerSlot *w;

	if (cc->state != MCC_IN_TX || widx < 0 || widx >= MUX_MAX_WORKERS)
		return;

	w = &mux_workers[widx];
	if (w->worker_sock == PGINVALID_SOCKET)
		return;

	/* Write c2w buffer to worker_sock (frontend→worker direction) */
	if ((events & WL_SOCKET_WRITEABLE) && cc->c2w_len > 0)
	{
		int written = send(w->worker_sock,
						   cc->c2w_buf + cc->c2w_off,
						   cc->c2w_len, 0);

		if (written > 0)
		{
			cc->c2w_off += written;
			cc->c2w_len -= written;
			if (cc->c2w_len == 0)
				cc->c2w_off = 0;
		}
		else if (written == 0 || (errno != EAGAIN && errno != EWOULDBLOCK &&
								  errno != EINPROGRESS && errno != EALREADY &&
								  errno != ENOTCONN))
		{
			mux_ctrl_close(idx);
			return;
		}
	}

	/* Read from worker_sock into w2c buffer */
	if ((events & WL_SOCKET_READABLE) && cc->w2c_len < MUX_PROXY_BUF)
	{
		int readn = recv(w->worker_sock,
						 cc->w2c_buf + cc->w2c_len,
						 MUX_PROXY_BUF - cc->w2c_len, 0);

		if (readn > 0)
		{
			bool found_rfq;

			found_rfq = mux_scan_rfq_idle(cc->w2c_buf + cc->w2c_len,
										  readn,
										  &cc->rfq_scan_pos);
			cc->w2c_len += readn;

			if (found_rfq)
			{
				/*
				 * Transaction ended on the worker side.  Set rfq_detected
				 * and release the worker's in_tx flag, but do NOT change
				 * state yet — we must first flush w2c_buf (including the
				 * RFQ bytes) to ctrl_sock so the local mux can detect the
				 * transaction end and send TX_END.  mux_ctrl_event will
				 * transition to MCC_READING_HDR once w2c_buf is empty.
				 */
				MUX_LOG("ctrl %d: worker RFQ 'I' detected, pending tunnel exit", idx);
				if (w->in_tx && w->active_channel == cc->channel_id)
				{
					w->in_tx = false;
					w->active_channel = -1;
				}
				cc->rfq_detected = true;
			}
		}
		else if (readn == 0)
		{
			mux_ctrl_close(idx);
			return;
		}
		else if (errno != EAGAIN && errno != EWOULDBLOCK)
		{
			mux_ctrl_close(idx);
			return;
		}
	}
}

/* =========================================================================
 * Accept and classify new connections
 * ========================================================================= */

static void
ConnMuxAcceptConn(void)
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

	/* Store in pending list for classification */
	for (int i = 0; i < MUX_MAX_PENDING; i++)
	{
		if (mux_pending[i] == PGINVALID_SOCKET)
		{
			mux_pending[i] = new_sock;
			mux_pending_count++;
			return;
		}
	}

	elog(WARNING, "connection multiplexer: too many pending connections");
	closesocket(new_sock);
}

/*
 * Classify a pending socket: PG startup → channel; control msg → ctrl conn.
 *
 * A PG startup packet has its first byte == 0x00 (because the 4-byte big-endian
 * length is small and the high byte is always 0).  A control message has its
 * first byte == the message type (e.g., 'C' = 0x43), which is never 0.
 */
static void
ConnMuxClassifyPending(int idx)
{
	pgsocket sock = mux_pending[idx];
	unsigned char peek;
	int nread;

	if (sock == PGINVALID_SOCKET)
		return;

	nread = recv(sock, (char *)&peek, 1, MSG_PEEK);
	if (nread < 0)
	{
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			return;
		closesocket(sock);
		mux_pending[idx] = PGINVALID_SOCKET;
		mux_pending_count--;
		return;
	}
	if (nread == 0)
	{
		closesocket(sock);
		mux_pending[idx] = PGINVALID_SOCKET;
		mux_pending_count--;
		return;
	}

	mux_pending[idx] = PGINVALID_SOCKET;
	mux_pending_count--;

	if (peek == 0x00)
	{
		/* PG startup packet — allocate a channel slot */
		int chi = mux_channel_alloc(sock);

		if (chi < 0)
		{
			elog(WARNING, "connection multiplexer: too many channel slots");
			closesocket(sock);
			return;
		}
		MUX_LOG("pending socket %d classified as channel %d (id=%d)",
				(int)sock, chi, mux_channels[chi].channel_id);
	}
	else
	{
		/* Control message — allocate a ctrl conn slot */
		int ci = mux_ctrl_alloc(sock);

		if (ci < 0)
		{
			elog(WARNING, "connection multiplexer: too many ctrl connections");
			closesocket(sock);
			return;
		}
		MUX_LOG("pending socket %d classified as ctrl conn %d", (int)sock, ci);
	}
}

/* =========================================================================
 * Main event loop
 * ========================================================================= */

static void
ConnMuxEventLoop(void)
{
	ConnMuxPublishStats();

	for (;;)
	{
		WaitEventSet *wes;
		WaitEvent events[256];
		int n_events;
		int i;
		int wes_size;

		CHECK_FOR_INTERRUPTS();
		/* Estimate event set size */
		wes_size = 4 +
				   MUX_MAX_PENDING +
				   MUX_MAX_CHANNELS * 2 +
				   MUX_MAX_CTRL_CONNS * 2;

		wes = CreateWaitEventSet(CurrentResourceOwner, wes_size);
		AddWaitEventToSet(wes, WL_LATCH_SET, PGINVALID_SOCKET,
						  &MuxState->mux_latch, NULL);
		AddWaitEventToSet(wes, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
						  NULL, NULL);
		AddWaitEventToSet(wes, WL_SOCKET_READABLE, mux_listen_sock,
						  NULL, (void *)(intptr_t)-1);

		/* Pending sockets */
		for (i = 0; i < MUX_MAX_PENDING; i++)
		{
			if (mux_pending[i] != PGINVALID_SOCKET)
				AddWaitEventToSet(wes, WL_SOCKET_READABLE, mux_pending[i],
								  NULL, MUX_EVENT(0, i)); /* kind=0 means pending */
		}

		/* Channel sockets */
		for (i = 0; i < MUX_MAX_CHANNELS; i++)
		{
			MuxChannelSlot *ch = &mux_channels[i];
			uint32 be_mask = 0,
				   ctrl_mask = 0;

			if (ch->state == MCH_EMPTY)
				continue;

			if (ch->backend_sock != PGINVALID_SOCKET)
			{
				if (ch->state == MCH_STARTUP)
					be_mask = WL_SOCKET_READABLE;
				else if (ch->state == MCH_IN_TX || ch->state == MCH_READY)
				{
					/*
					 * Don't read from the backend while TX_END is pending.
					 * The next query must not be buffered until we finish the
					 * current transaction handshake with the remote mux.
					 */
					if (ch->c2r_len < MUX_PROXY_BUF &&
						!(ch->state == MCH_IN_TX && ch->pending_tx_end))
						be_mask |= WL_SOCKET_READABLE;
					if (ch->r2c_len > 0)
						be_mask |= WL_SOCKET_WRITEABLE;
				}
				if (be_mask)
					AddWaitEventToSet(wes, be_mask, ch->backend_sock,
									  NULL, MUX_EVENT(MUX_EV_CHAN_BACKEND, i));
			}

			if (ch->ctrl_sock != PGINVALID_SOCKET)
			{
				if (ch->state == MCH_CONNECTING || ch->state == MCH_TX_PENDING)
					ctrl_mask = WL_SOCKET_READABLE;
				else if (ch->state == MCH_IN_TX)
				{
					if (ch->r2c_len < MUX_PROXY_BUF)
						ctrl_mask |= WL_SOCKET_READABLE;
					/* Don't request writable for c2r when tx_end is pending */
					if (ch->c2r_len > 0 && !ch->pending_tx_end)
						ctrl_mask |= WL_SOCKET_WRITEABLE;
				}
				if (ctrl_mask)
					AddWaitEventToSet(wes, ctrl_mask, ch->ctrl_sock,
									  NULL, MUX_EVENT(MUX_EV_CHAN_CTRL, i));
			}
		}

		/* Ctrl conn sockets */
		for (i = 0; i < MUX_MAX_CTRL_CONNS; i++)
		{
			MuxCtrlConn *cc = &mux_ctrl[i];
			uint32 ctrl_mask = 0,
				   worker_mask = 0;
			int widx;

			if (cc->state == MCC_EMPTY)
				continue;

			if (cc->ctrl_sock != PGINVALID_SOCKET)
			{
				ctrl_mask = WL_SOCKET_READABLE;
				if (cc->w2c_len > 0)
					ctrl_mask |= WL_SOCKET_WRITEABLE;
				if (ctrl_mask)
					AddWaitEventToSet(wes, ctrl_mask, cc->ctrl_sock,
									  NULL, MUX_EVENT(MUX_EV_CTRL_SOCK, i));
			}

			widx = cc->worker_idx;
			if (cc->state == MCC_IN_TX &&
				widx >= 0 && widx < MUX_MAX_WORKERS &&
				mux_workers[widx].worker_sock != PGINVALID_SOCKET)
			{
				if (cc->w2c_len < MUX_PROXY_BUF && !cc->rfq_detected)
					worker_mask |= WL_SOCKET_READABLE;
				if (cc->c2w_len > 0)
					worker_mask |= WL_SOCKET_WRITEABLE;
				if (worker_mask)
					AddWaitEventToSet(wes, worker_mask,
									  mux_workers[widx].worker_sock,
									  NULL, MUX_EVENT(MUX_EV_CTRL_WORKER, i));
			}
		}

		if (mux_we_wait == 0)
			mux_we_wait = WaitEventExtensionNew("ConnMuxWait");

		n_events = WaitEventSetWait(wes, 1000, events, lengthof(events),
									mux_we_wait);
		FreeWaitEventSet(wes);

		ResetLatch(&MuxState->mux_latch);

		for (i = 0; i < n_events; i++)
		{
			WaitEvent *ev = &events[i];

			if (ev->events & WL_LATCH_SET)
				continue;

			if (ev->user_data == (void *)(intptr_t)-1)
			{
				/* listen socket */
				ConnMuxAcceptConn();
				continue;
			}

			if (!(ev->events & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)))
				continue;

			{
				int kind = MUX_EVENT_KIND(ev->user_data);
				int kidx = MUX_EVENT_IDX(ev->user_data);

				switch (kind)
				{
				case 0: /* pending */
					ConnMuxClassifyPending(kidx);
					break;
				case MUX_EV_CHAN_BACKEND:
					mux_channel_backend_event(kidx, ev->events);
					break;
				case MUX_EV_CHAN_CTRL:
					mux_channel_ctrl_event(kidx, ev->events);
					break;
				case MUX_EV_CTRL_SOCK:
					mux_ctrl_event(kidx, ev->events);
					break;
				case MUX_EV_CTRL_WORKER:
					mux_ctrl_worker_event(kidx, ev->events);
					break;
				default:
					break;
				}
			}
		}

		/* Classify any pending sockets that became readable */
		for (i = 0; i < MUX_MAX_PENDING; i++)
		{
			if (mux_pending[i] != PGINVALID_SOCKET)
				ConnMuxClassifyPending(i);
		}

		ConnMuxPublishStats();
	}
}

static void
ConnMuxPublishStats(void)
{
	MuxStatsSnapshot snapshot;
	int i;

	if (MuxState == NULL)
		return;

	MemSet(&snapshot, 0, sizeof(snapshot));

	snapshot.mux_pid = MuxState->mux_pid;
	snapshot.mux_ready = MuxState->mux_ready;
	snapshot.mux_pending_count = mux_pending_count;
	snapshot.mux_n_workers = mux_n_workers;

	for (i = 0; i < MUX_MAX_CHANNELS; i++)
	{
		MuxChannelSlot *ch = &mux_channels[i];

		if (ch->state == MCH_EMPTY)
			continue;

		snapshot.mux_channel_count++;
		switch (ch->state)
		{
			case MCH_STARTUP:
				snapshot.mux_channel_startup++;
				break;
			case MCH_CONNECTING:
				snapshot.mux_channel_connecting++;
				break;
			case MCH_READY:
				snapshot.mux_channel_ready++;
				break;
			case MCH_TX_PENDING:
				snapshot.mux_channel_tx_pending++;
				break;
			case MCH_IN_TX:
				snapshot.mux_channel_in_tx++;
				break;
			case MCH_EMPTY:
				break;
		}
	}

	for (i = 0; i < MUX_MAX_CTRL_CONNS; i++)
	{
		if (mux_ctrl[i].state != MCC_EMPTY)
			snapshot.mux_ctrl_count++;
	}

	for (i = 0; i < MUX_MAX_WORKERS; i++)
	{
		MuxWorkerSlot *w = &mux_workers[i];
		MuxWorkerStats *ws = &snapshot.workers[i];

		if (w->worker_sock == PGINVALID_SOCKET)
			continue;

		ws->valid = true;
		ws->worker_pid = w->worker_pid;
		ws->in_tx = w->in_tx;
		ws->active_channel = w->active_channel;
		ws->connect_cnt = w->connect_cnt;
		strlcpy(ws->database, w->database, sizeof(ws->database));
		strlcpy(ws->username, w->username, sizeof(ws->username));
		if (w->in_tx)
			snapshot.mux_workers_in_tx++;
	}

	SpinLockAcquire(&MuxState->mutex);
	memcpy(&MuxState->stats, &snapshot, sizeof(snapshot));
	SpinLockRelease(&MuxState->mutex);
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

	/* Initialize process-local state */
	for (i = 0; i < MUX_MAX_PENDING; i++)
		mux_pending[i] = PGINVALID_SOCKET;
	mux_pending_count = 0;

	for (i = 0; i < MUX_MAX_CHANNELS; i++)
		mux_channel_init(&mux_channels[i]);

	for (i = 0; i < MUX_MAX_CTRL_CONNS; i++)
		mux_ctrl_init(&mux_ctrl[i]);

	for (i = 0; i < MUX_MAX_WORKERS; i++)
		mux_worker_init(&mux_workers[i]);
	mux_n_workers = 0;

	mux_next_channel_id = 1;

	ConnMuxSetupListenSocket();

	MuxState->mux_ready = true;
	ConnMuxPublishStats();

	elog(LOG, "connection multiplexer started on TCP port %d",
		 mux_tcp_port);

	set_ps_display("idle");

	ConnMuxEventLoop();

	/* Cleanup */
	MuxState->mux_ready = false;
	MuxState->mux_pid = 0;
	ConnMuxPublishStats();
	DisownLatch(&MuxState->mux_latch);

	if (mux_listen_sock != PGINVALID_SOCKET)
	{
		closesocket(mux_listen_sock);
		mux_listen_sock = PGINVALID_SOCKET;
	}

	for (i = 0; i < MUX_MAX_CHANNELS; i++)
	{
		MuxChannelSlot *ch = &mux_channels[i];

		if (ch->backend_sock != PGINVALID_SOCKET)
			closesocket(ch->backend_sock);
		if (ch->ctrl_sock != PGINVALID_SOCKET)
			closesocket(ch->ctrl_sock);
	}

	for (i = 0; i < MUX_MAX_WORKERS; i++)
	{
		if (mux_workers[i].worker_sock != PGINVALID_SOCKET)
			closesocket(mux_workers[i].worker_sock);
	}

	proc_exit(0);
}
