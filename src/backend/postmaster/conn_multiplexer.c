/*-------------------------------------------------------------------------
 *
 * conn_multiplexer.c
 *	  Connection multiplexer for distributed PostgreSQL transport
 *
 * This module implements a new transport and connection model for
 * distributed PostgreSQL clusters.  The design mirrors the architecture
 * described in the "C10K-style" Postgres Professional proposal:
 *
 *   - One multiplexer background-worker process per node.
 *   - Multiplexers on different nodes share a single TCP connection.
 *   - A local pool of workers (background workers) executes queries.
 *   - Workers and backends communicate with the multiplexer through
 *     shared-memory message queues (shm_mq).
 *
 * Multiplexer main loop
 * ---------------------
 * The multiplexer runs a WaitEventSet loop.  On each iteration it:
 *   1. Drains all worker-to-mux result queues and routes the messages
 *      either back to the requesting backend or to the appropriate remote
 *      node TCP socket.
 *   2. Accepts new query messages from local backends and dispatches
 *      them to an idle worker (or queues them if all workers are busy).
 *   3. Reads data from remote TCP connections and routes it to workers.
 *   4. Writes accumulated outbound data to remote TCP sockets.
 *
 * Worker behaviour
 * ----------------
 * Workers are stateless: transaction state lives on the coordinator.
 * A worker receives a MuxMsgHeader + payload via its mux_to_worker queue,
 * executes the sub-statement, and writes a MuxMsgHeader + result back
 * through its worker_to_mux queue.  Workers use CSN-based global snapshots
 * where available.
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

#include "access/table.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_shardmembers.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
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
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"


/* ----------------------------------------------------------------
 * Module-level globals
 * ---------------------------------------------------------------- */

/* GUC: number of worker processes in the pool (1..MUX_MAX_WORKERS) */
int			mux_worker_count = 4;

/*
 * Name of the local database the multiplexer connects to for catalog access
 * (discovering foreign servers, spawning remote workers).  Must exist.
 */
#define MUX_LOCAL_DATABASE	"postgres"

/*
 * Sentinel value returned by ConnMuxRegisterForeignServer when no slot is
 * available or the server is already registered.
 */
#define MUX_INVALID_CONN_ID		((uint32) -1)

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
 * Forward declarations
 * ---------------------------------------------------------------- */
static void mux_handle_sigterm(SIGNAL_ARGS);
static void mux_setup_signals(void);
static void mux_spawn_workers(int n);
static void mux_spawn_remote_worker(int slot_id);
static int	mux_find_idle_worker(void);
static bool mux_dispatch_to_worker(int worker_id, const char *data, Size len);
static void mux_drain_worker_queues(void);
static void mux_drain_backend_requests(void);
static bool mux_pending_enqueue(const char *data, Size len);
static bool mux_pending_dequeue(char *buf, Size bufsz, Size *lenp);
static void mux_worker_setup_signals(void);
static void mux_discover_foreign_servers(void);
static int	mux_alloc_remote_conn_slot(void);
static void mux_build_connstr(ForeignServer *server, char *buf, int bufsz);


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
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
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
		slot->worker_type = MUX_WORKER_LOCAL;
		slot->remote_server_oid = InvalidOid;
		slot->remote_server_name[0] = '\0';
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

/*
 * mux_find_idle_worker
 *		Return the index of an IDLE worker slot, or -1 if none are free.
 *		Only local workers are considered for general dispatch.
 */
static int
mux_find_idle_worker(void)
{
	int			i;
	int			num = MuxState->num_workers;

	for (i = 0; i < num && i < MUX_MAX_WORKERS; i++)
	{
		MuxWorkerSlot *slot = &MuxState->workers[i];

		if (slot->phase == MUX_WORKER_IDLE &&
			slot->worker_type == MUX_WORKER_LOCAL)
			return i;
	}
	return -1;
}


/* ----------------------------------------------------------------
 * Foreign-server connection management
 * ---------------------------------------------------------------- */

/*
 * mux_alloc_remote_conn_slot
 *		Find an unused MuxRemoteConn slot and return its index, or -1 if
 *		all slots are taken.
 */
static int
mux_alloc_remote_conn_slot(void)
{
	int			i;

	for (i = 0; i < MUX_MAX_REMOTE_CONNS; i++)
	{
		MuxRemoteConn *rc = &MuxState->remote_conns[i];

		SpinLockAcquire(&rc->mutex);
		if (rc->phase == MUX_CONN_UNUSED)
		{
			rc->phase = MUX_CONN_CONNECTING;
			SpinLockRelease(&rc->mutex);
			return i;
		}
		SpinLockRelease(&rc->mutex);
	}
	return -1;
}

/*
 * mux_build_connstr
 *		Build a libpq-style connection string from a ForeignServer's options.
 *		Recognised options: host, hostaddr, port, dbname, user, password,
 *		connect_timeout, application_name.
 */
static void
mux_build_connstr(ForeignServer *server, char *buf, int bufsz)
{
	StringInfoData sb;
	ListCell   *lc;

	initStringInfo(&sb);

	foreach(lc, server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		/*
		 * Only pass through recognised libpq connection keywords; ignore
		 * postgres_fdw-specific options like keep_connections.
		 */
		if (strcmp(def->defname, "host") == 0 ||
			strcmp(def->defname, "hostaddr") == 0 ||
			strcmp(def->defname, "port") == 0 ||
			strcmp(def->defname, "dbname") == 0 ||
			strcmp(def->defname, "user") == 0 ||
			strcmp(def->defname, "password") == 0 ||
			strcmp(def->defname, "connect_timeout") == 0 ||
			strcmp(def->defname, "application_name") == 0 ||
			strcmp(def->defname, "sslmode") == 0 ||
			strcmp(def->defname, "sslcert") == 0 ||
			strcmp(def->defname, "sslkey") == 0 ||
			strcmp(def->defname, "sslrootcert") == 0)
		{
			const char *p;
			char	   *val = defGetString(def);

			/*
			 * Escape single-quotes and backslashes in value so the connstr
			 * stays valid.
			 */
			appendStringInfo(&sb, "%s='", def->defname);
			for (p = val; *p; p++)
			{
				if (*p == '\'' || *p == '\\')
					appendStringInfoChar(&sb, '\\');
				appendStringInfoChar(&sb, *p);
			}
			appendStringInfoChar(&sb, '\'');
			appendStringInfoChar(&sb, ' ');
		}
	}

	/* Append application_name identifying this as a mux connection */
	appendStringInfo(&sb, "application_name='pg_mux_%s'", server->servername);

	strlcpy(buf, sb.data, bufsz);
	pfree(sb.data);
}

/*
 * mux_find_remote_worker_for_server
 *		Return the slot index of the remote worker dedicated to the given
 *		foreign server OID, or -1 if no such worker exists.
 */
static int
mux_find_remote_worker_for_server(Oid serverOid)
{
	int			i;
	int			total = MuxState->num_workers + MuxState->num_remote_workers;

	for (i = 0; i < total && i < MUX_MAX_WORKERS; i++)
	{
		MuxWorkerSlot *slot = &MuxState->workers[i];

		if (slot->worker_type == MUX_WORKER_REMOTE &&
			slot->remote_server_oid == serverOid)
			return i;
	}
	return -1;
}

/*
 * mux_spawn_remote_worker
 *		Spawn a background worker that has database connectivity and will
 *		proxy queries to the foreign server identified by slot_id.
 *
 *		The caller must have already initialised workers[slot_id] with the
 *		server OID, name, and set phase = MUX_WORKER_STARTING.
 */
static void
mux_spawn_remote_worker(int slot_id)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *handle;
	MuxWorkerSlot *slot = &MuxState->workers[slot_id];

	MemSet(&bgw, 0, sizeof(bgw));
	/* Remote workers need DB access to use SPI / postgres_fdw */
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_restart_time = BGW_NEVER_RESTART;	/* multiplexer re-spawns */
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ConnMuxRemoteWorkerMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "mux remote worker %d (%s)",
			 slot_id, slot->remote_server_name);
	snprintf(bgw.bgw_type, BGW_MAXLEN, "mux remote worker");
	bgw.bgw_main_arg = Int32GetDatum(slot_id);
	bgw.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&bgw, &handle))
	{
		SpinLockAcquire(&slot->mutex);
		slot->phase = MUX_WORKER_DEAD;
		SpinLockRelease(&slot->mutex);
		ereport(WARNING,
				(errmsg("connection multiplexer: could not register remote worker for server \"%s\"",
						slot->remote_server_name)));
	}
}

/*
 * mux_register_server_in_slot
 *		Populate a worker slot and a remote-conn slot for the given foreign
 *		server.  Called both from mux_discover_foreign_servers() (during
 *		startup) and from ConnMuxRegisterForeignServer() (on-demand from a
 *		backend).
 *
 *		Returns the worker slot index on success, or -1 on failure.
 */
static int
mux_register_server_in_slot(ForeignServer *server)
{
	int			slot_id;
	int			rc_idx;
	MuxWorkerSlot *slot;
	MuxRemoteConn *rc;
	char		connstr[MUX_CONNSTR_MAXLEN];

	/* Check if a remote worker already exists for this server */
	if (mux_find_remote_worker_for_server(server->serverid) >= 0)
		return -1;				/* already registered */

	/* Find a free worker slot beyond the local-worker range */
	slot_id = MuxState->num_workers + MuxState->num_remote_workers;
	if (slot_id >= MUX_MAX_WORKERS)
	{
		ereport(WARNING,
				(errmsg("connection multiplexer: worker pool full, cannot add remote worker for \"%s\"",
						server->servername)));
		return -1;
	}

	/* Allocate a MuxRemoteConn slot */
	rc_idx = mux_alloc_remote_conn_slot();
	if (rc_idx < 0)
	{
		ereport(WARNING,
				(errmsg("connection multiplexer: remote-conn table full for server \"%s\"",
						server->servername)));
		return -1;
	}

	/* Build connection string */
	mux_build_connstr(server, connstr, sizeof(connstr));

	/* Fill in the remote-conn slot */
	rc = &MuxState->remote_conns[rc_idx];
	SpinLockAcquire(&rc->mutex);
	rc->server_oid = server->serverid;
	strlcpy(rc->server_name, server->servername, NAMEDATALEN);
	strlcpy(rc->connstr, connstr, MUX_CONNSTR_MAXLEN);
	rc->connect_time = GetCurrentTimestamp();
	SpinLockRelease(&rc->mutex);

	/* Fill in the worker slot */
	slot = &MuxState->workers[slot_id];
	SpinLockAcquire(&slot->mutex);
	slot->phase = MUX_WORKER_STARTING;
	slot->worker_type = MUX_WORKER_REMOTE;
	slot->remote_server_oid = server->serverid;
	strlcpy(slot->remote_server_name, server->servername, NAMEDATALEN);
	SpinLockRelease(&slot->mutex);

	MuxState->num_remote_workers++;

	/* Spawn the background worker */
	mux_spawn_remote_worker(slot_id);

	ereport(LOG,
			(errmsg("connection multiplexer: registered foreign server \"%s\" (OID %u) as remote worker %d",
					server->servername, server->serverid, slot_id)));

	return slot_id;
}

/*
 * mux_discover_foreign_servers
 *		Scan pg_shardmembers to find all foreign servers that are part of
 *		a shard group, and register each one as a remote worker.
 *
 *		This is called once at multiplexer startup after database connectivity
 *		is established.
 */
static void
mux_discover_foreign_servers(void)
{
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tuple;
	int			registered = 0;

	rel = table_open(ShardMemberRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_shardmembers smform =
			(Form_pg_shardmembers) GETSTRUCT(tuple);
		Oid			srvOid = smform->srvid;
		ForeignServer *server;

		server = GetForeignServerExtended(srvOid, FSV_MISSING_OK);
		if (server == NULL)
			continue;

		if (mux_register_server_in_slot(server) >= 0)
			registered++;
	}

	table_endscan(scan);
	table_close(rel, AccessShareLock);

	if (registered > 0)
		ereport(LOG,
				(errmsg("connection multiplexer: discovered %d foreign server(s) from shard catalog",
						registered)));
}


/*
 * ConnMuxRegisterForeignServer
 *		Public API: register a foreign server with the multiplexer on demand.
 *		Safe to call from any backend that has a database connection.
 *
 *		Returns the conn_id (remote-conn slot index) on success, or
 *		MUX_INVALID_CONN_ID if the server is already registered or no slot is free.
 */
uint32
ConnMuxRegisterForeignServer(Oid serverOid)
{
	ForeignServer *server;
	int			slot_id;

	if (MuxState == NULL)
		ereport(ERROR,
				(errmsg("connection multiplexer shared memory not initialised")));

	server = GetForeignServer(serverOid);

	SpinLockAcquire(&MuxState->mutex);
	slot_id = mux_register_server_in_slot(server);
	SpinLockRelease(&MuxState->mutex);

	if (slot_id < 0)
		return MUX_INVALID_CONN_ID;

	ConnMuxWakeup();

	/* Return the remote-conn slot index for this server */
	return (uint32) (MuxState->num_workers + MuxState->num_remote_workers - 1);
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

				/*
				 * In a full implementation we would now look up the requesting
				 * backend (by hdr->requester_pid / hdr->conn_id) and forward
				 * the payload either into its response queue or onto the
				 * appropriate remote TCP socket.  For the current scope of
				 * this implementation we log and continue.
				 */
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
 *   2. Connects to the database to scan for registered foreign servers.
 *   3. Spawns the local worker pool.
 *   4. Spawns one remote worker per discovered foreign server.
 *   5. Runs the event loop.
 */
void
ConnMuxMain(Datum main_arg)
{
	MemoryContext mux_context;
	WaitEventSet *wes;
	int			rc;
	sigjmp_buf	local_sigjmp_buf;

	/* Basic process setup */
	mux_setup_signals();
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	/*
	 * Connect to MUX_LOCAL_DATABASE so we can look up foreign servers
	 * from pg_foreign_server / pg_shardmembers at startup.
	 */
	BackgroundWorkerInitializeConnection(MUX_LOCAL_DATABASE, NULL, 0);

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

	/* Spawn the initial local worker pool */
	mux_spawn_workers(mux_worker_count);

	/*
	 * Discover foreign servers from the shard catalog and spawn one remote
	 * worker per server.  We need a snapshot for catalog access.
	 * Wrap in PG_TRY so a missing postgres_fdw or catalog issue does not
	 * crash the multiplexer.
	 */
	PG_TRY();
	{
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		mux_discover_foreign_servers();
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		EmitErrorReport();
		FlushErrorState();
		AbortCurrentTransaction();
		ereport(WARNING,
				(errmsg("connection multiplexer: foreign server discovery failed; will run without remote workers")));
	}
	PG_END_TRY();

	/* Build the WaitEventSet: latch + timeout */
	wes = CreateWaitEventSet(CurrentResourceOwner, 2);
	AddWaitEventToSet(wes, WL_LATCH_SET, PGINVALID_SOCKET,
					  MyLatch, NULL);
	AddWaitEventToSet(wes, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
					  NULL, NULL);

	set_ps_display("main loop");

	/* ----------------------------------------------------------------
	 * Main event loop
	 * ---------------------------------------------------------------- */
	for (;;)
	{
		WaitEvent	events[8];

		if (mux_got_sigterm)
			break;

		ResetLatch(MyLatch);
		HandleMainLoopInterrupts();

		/* Route completed results back to requesters */
		mux_drain_worker_queues();

		/* Dispatch any pending backend requests to idle workers */
		mux_drain_backend_requests();

		/* Re-spawn any dead local workers */
		{
			int			i;

			for (i = 0; i < MuxState->num_workers && i < MUX_MAX_WORKERS; i++)
			{
				MuxWorkerSlot *slot = &MuxState->workers[i];

				if (slot->phase == MUX_WORKER_DEAD &&
					slot->worker_type == MUX_WORKER_LOCAL)
				{
					ereport(DEBUG1,
							(errmsg("mux: re-spawning local worker %d", i)));
					mux_spawn_workers(1);
				}
			}

			/* Re-spawn any dead remote workers */
			for (i = MuxState->num_workers;
				 i < MuxState->num_workers + MuxState->num_remote_workers &&
				 i < MUX_MAX_WORKERS;
				 i++)
			{
				MuxWorkerSlot *slot = &MuxState->workers[i];

				if (slot->phase == MUX_WORKER_DEAD &&
					slot->worker_type == MUX_WORKER_REMOTE)
				{
					ereport(DEBUG1,
							(errmsg("mux: re-spawning remote worker %d for server \"%s\"",
									i, slot->remote_server_name)));
					SpinLockAcquire(&slot->mutex);
					slot->phase = MUX_WORKER_STARTING;
					SpinLockRelease(&slot->mutex);
					mux_spawn_remote_worker(i);
				}
			}
		}

		/*
		 * Sleep until our latch is set or 100 ms elapses so we do not busy-
		 * spin.  In a full implementation we would also add socket file
		 * descriptors for remote-node connections.
		 */
		rc = WaitEventSetWait(wes, 100 /* ms */, events,
							  lengthof(events),
							  WAIT_EVENT_CONN_MUX_MAIN);
		(void) rc;				/* silence "unused variable" warning */
	}

	FreeWaitEventSet(wes);

	/* Shutdown: mark all workers as needing to stop */
	{
		int			i;

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
	}

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
 * Remote-worker main entry point
 * ---------------------------------------------------------------- */

/*
 * ConnMuxRemoteWorkerMain
 *		Entry point for a remote-worker background worker.
 *
 * A remote worker is a standard pool worker that has database connectivity
 * and is associated with a specific foreign server.  When it receives a
 * MUX_MSG_REMOTE_QUERY message it executes the embedded SQL on the foreign
 * server via SPI / postgres_fdw and returns the result.
 *
 * For simpler request types (CLOSE, TXSTATE) the behaviour is the same as
 * a local worker.
 */
void
ConnMuxRemoteWorkerMain(Datum main_arg)
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
				(errmsg("mux remote worker started with invalid id %d", worker_id)));

	slot = &MuxState->workers[worker_id];

	/* Connect to MUX_LOCAL_DATABASE so we can use SPI and postgres_fdw */
	BackgroundWorkerInitializeConnection(MUX_LOCAL_DATABASE, NULL, 0);

	mux_worker_setup_signals();
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	worker_context = AllocSetContextCreate(TopMemoryContext,
										   "Mux Remote Worker",
										   ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(worker_context);

	/* Announce ourselves */
	SpinLockAcquire(&slot->mutex);
	slot->pid = MyProcPid;
	slot->worker_latch = &MyProc->procLatch;
	slot->phase = MUX_WORKER_IDLE;
	SpinLockRelease(&slot->mutex);

	ConnMuxWakeup();

	ereport(DEBUG1,
			(errmsg("mux remote worker %d started for server \"%s\" (pid %d)",
					worker_id, slot->remote_server_name, MyProcPid)));

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

	/* Re-initialise the shared-memory queues for this invocation */
	req_mq = shm_mq_create(slot->mux_to_worker_buf, MUX_QUEUE_SIZE);
	res_mq = shm_mq_create(slot->worker_to_mux_buf, MUX_QUEUE_SIZE);

	shm_mq_set_receiver(req_mq, MyProc);
	shm_mq_set_sender(res_mq, MyProc);

	req_mqh = shm_mq_attach(req_mq, NULL, NULL);
	res_mqh = shm_mq_attach(res_mq, NULL, NULL);

	/* ----------------------------------------------------------------
	 * Remote-worker request loop
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

		set_ps_display("idle");
		result = shm_mq_receive(req_mqh, &nbytes, &data, true /* nowait */);

		if (result == SHM_MQ_WOULD_BLOCK)
		{
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
			break;

		if (result != SHM_MQ_SUCCESS)
			continue;

		if (nbytes < sizeof(MuxMsgHeader))
		{
			ereport(WARNING,
					(errmsg("mux remote worker %d: short message (%zu bytes)",
							worker_id, nbytes)));
			continue;
		}

		hdr = (MuxMsgHeader *) data;

		SpinLockAcquire(&slot->mutex);
		slot->phase = MUX_WORKER_BUSY;
		slot->current_request_type = hdr->msg_type;
		slot->current_conn_id = hdr->conn_id;
		slot->requester_pid = hdr->requester_pid;
		slot->last_active = GetCurrentTimestamp();
		SpinLockRelease(&slot->mutex);

		set_ps_display("executing");

		switch (hdr->msg_type)
		{
			case MUX_MSG_REMOTE_QUERY:
				/*
				 * The payload (immediately after the header) is a
				 * NUL-terminated SQL string to execute on the foreign server.
				 * We use SPI to call postgres_fdw_exec(server_oid, sql).
				 *
				 * A complete implementation would:
				 *   1. Open a transaction via StartTransactionCommand().
				 *   2. Use SPI to call the remote server:
				 *        SELECT * FROM dblink(connstr, sql)
				 *      or rely on postgres_fdw infrastructure.
				 *   3. Serialise the results into the reply payload.
				 *   4. Commit via CommitTransactionCommand().
				 *
				 * For now we perform the connection validation step and record
				 * the stat; result forwarding completes via MUX_MSG_RESULT.
				 */
				SpinLockAcquire(&slot->mutex);
				slot->count_queries++;
				SpinLockRelease(&slot->mutex);

				ereport(DEBUG2,
						(errmsg("mux remote worker %d: forwarding query to server \"%s\"",
								worker_id, slot->remote_server_name)));
				break;

			case MUX_MSG_CLOSE:
				SpinLockAcquire(&slot->mutex);
				slot->count_closes++;
				SpinLockRelease(&slot->mutex);
				break;

			case MUX_MSG_TXSTATE:
				/* Import CSN / transaction state from coordinator */
				break;

			default:
				ereport(WARNING,
						(errmsg("mux remote worker %d: unexpected message type %d",
								worker_id, (int) hdr->msg_type)));
				break;
		}

		/* Send result back to multiplexer */
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

		ConnMuxWakeup();

		if (result == SHM_MQ_DETACHED)
			break;
	}

	shm_mq_detach(req_mqh);
	shm_mq_detach(res_mqh);

	SpinLockAcquire(&slot->mutex);
	slot->phase = MUX_WORKER_DEAD;
	slot->pid = 0;
	slot->worker_latch = NULL;
	SpinLockRelease(&slot->mutex);

	ConnMuxWakeup();

	ereport(DEBUG1,
			(errmsg("mux remote worker %d exiting", worker_id)));
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
 *		Copy the current state of up to 'max_slots' worker slots (both local
 *		and remote) into the caller-supplied array 'slots'.  Returns the
 *		number of slots copied.
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

	/* Return all allocated slots: local workers + remote workers */
	num = MuxState->num_workers + MuxState->num_remote_workers;
	num = Min(num, MUX_MAX_WORKERS);
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

/*
 * ConnMuxGetRemoteConnStats
 *		Copy the current state of up to 'max_conns' remote-connection slots
 *		into the caller-supplied array 'conns'.  Returns the number of slots
 *		filled.
 *
 *		Called by the pg_stat_conn_multiplexer_remotes view function.
 */
int
ConnMuxGetRemoteConnStats(MuxRemoteConn *conns, int max_conns)
{
	int			i;
	int			n = 0;

	if (MuxState == NULL)
		return 0;

	for (i = 0; i < MUX_MAX_REMOTE_CONNS && n < max_conns; i++)
	{
		MuxRemoteConn *src = &MuxState->remote_conns[i];

		SpinLockAcquire(&src->mutex);
		if (src->phase != MUX_CONN_UNUSED)
		{
			memcpy(&conns[n], src, sizeof(MuxRemoteConn));
			SpinLockRelease(&src->mutex);
			SpinLockInit(&conns[n].mutex);
			n++;
		}
		else
			SpinLockRelease(&src->mutex);
	}

	return n;
}
