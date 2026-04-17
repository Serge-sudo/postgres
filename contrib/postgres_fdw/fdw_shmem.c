/*-------------------------------------------------------------------------
 *
 * fdw_shmem.c
 *		  Shared memory tracking for FDW connections
 *
 * This module maintains a shared memory hash table tracking active
 * FDW connections from all backends. This is needed for distributed
 * deadlock detection to identify FDW connection dependencies.
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/postgres_fdw/fdw_shmem.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "postgres_fdw.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "miscadmin.h"

/*
 * Shared memory hash table entry for FDW connections
 * Key: (local_pid, cluster_name)
 * Value: remote_backend_pid
 */
typedef struct FdwConnShmemKey
{
	int			local_pid;
	char		cluster_name[NAMEDATALEN];
} FdwConnShmemKey;

typedef struct FdwConnShmemEntry
{
	FdwConnShmemKey key;		/* hash key (must be first) */
	int			remote_backend_pid;
} FdwConnShmemEntry;

/* Shared memory state */
typedef struct FdwConnShmemState
{
	LWLock		lock;			/* protects the hash table */
} FdwConnShmemState;

static FdwConnShmemState *fdw_conn_shmem_state = NULL;
static HTAB *FdwConnShmemHash = NULL;

/* Estimated number of FDW connections */
#define FDWCONN_SHMEM_ENTRIES 1000

/*
 * Estimate shared memory space needed
 */
Size
FdwConnShmemSize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(FdwConnShmemState));
	size = add_size(size, hash_estimate_size(FDWCONN_SHMEM_ENTRIES,
											   sizeof(FdwConnShmemEntry)));
	return size;
}

/*
 * Initialize shared memory structures
 */
void
FdwConnShmemInit(void)
{
	HASHCTL		info;
	bool		found;

	/* Create or attach to shared memory state */
	fdw_conn_shmem_state = (FdwConnShmemState *)
		ShmemInitStruct("FDW Connection State",
						sizeof(FdwConnShmemState),
						&found);

	if (!found)
	{
		/* First time through - initialize */
		LWLockInitialize(&fdw_conn_shmem_state->lock,
						 LWTRANCHE_PARALLEL_HASH_JOIN);
	}

	/* Set up hash table */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(FdwConnShmemKey);
	info.entrysize = sizeof(FdwConnShmemEntry);

	FdwConnShmemHash = ShmemInitHash("FDW Connection Hash",
									  FDWCONN_SHMEM_ENTRIES,
									  FDWCONN_SHMEM_ENTRIES,
									  &info,
									  HASH_ELEM | HASH_BLOBS);
}

/*
 * Register an FDW connection in shared memory
 */
void
FdwConnShmemRegister(const char *cluster_name, int remote_backend_pid)
{
	FdwConnShmemKey key;
	FdwConnShmemEntry *entry;
	bool		found;

	if (!FdwConnShmemHash)
		return;

	key.local_pid = MyProcPid;
	strncpy(key.cluster_name, cluster_name, NAMEDATALEN);
	LWLockAcquire(&fdw_conn_shmem_state->lock, LW_EXCLUSIVE);

	entry = (FdwConnShmemEntry *) hash_search(FdwConnShmemHash,
											   &key,
											   HASH_ENTER,
											   &found);

	if (entry)
		entry->remote_backend_pid = remote_backend_pid;

	LWLockRelease(&fdw_conn_shmem_state->lock);
}

/*
 * Unregister an FDW connection from shared memory
 */
void
FdwConnShmemUnregister(const char *cluster_name)
{
	FdwConnShmemKey key;

	if (!FdwConnShmemHash)
		return;

	key.local_pid = MyProcPid;
	strncpy(key.cluster_name, cluster_name, NAMEDATALEN);

	LWLockAcquire(&fdw_conn_shmem_state->lock, LW_EXCLUSIVE);

	hash_search(FdwConnShmemHash,
				&key,
				HASH_REMOVE,
				NULL);

	LWLockRelease(&fdw_conn_shmem_state->lock);
}

/*
 * Unregister all FDW connections for the current backend
 * Called at backend exit
 */
void
FdwConnShmemUnregisterAll(void)
{
	HASH_SEQ_STATUS status;
	FdwConnShmemEntry *entry;

	if (!FdwConnShmemHash)
		return;

	LWLockAcquire(&fdw_conn_shmem_state->lock, LW_EXCLUSIVE);

	hash_seq_init(&status, FdwConnShmemHash);
	while ((entry = (FdwConnShmemEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->key.local_pid == MyProcPid)
		{
			hash_search(FdwConnShmemHash,
						&entry->key,
						HASH_REMOVE,
						NULL);
		}
	}

	LWLockRelease(&fdw_conn_shmem_state->lock);
}

/*
 * Remove stale rows whose local_pid no longer maps to an active backend.
 */
void
FdwConnShmemCleanupStaleEntries(void)
{
	HASH_SEQ_STATUS status;
	FdwConnShmemEntry *entry;
	FdwConnShmemKey stale_keys[FDWCONN_SHMEM_ENTRIES];
	int			stale_count = 0;

	if (!FdwConnShmemHash)
		return;

	LWLockAcquire(&fdw_conn_shmem_state->lock, LW_EXCLUSIVE);

	hash_seq_init(&status, FdwConnShmemHash);
	while ((entry = (FdwConnShmemEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->key.local_pid <= 0 ||
			BackendPidGetProc(entry->key.local_pid) == NULL)
		{
			if (stale_count < FDWCONN_SHMEM_ENTRIES)
				stale_keys[stale_count++] = entry->key;
		}
	}

	for (int i = 0; i < stale_count; i++)
		hash_search(FdwConnShmemHash, &stale_keys[i], HASH_REMOVE, NULL);

	LWLockRelease(&fdw_conn_shmem_state->lock);
}

/*
 * Get iterator for shared memory FDW connections
 * Returns true if iteration can proceed, false if hash table not initialized
 */
bool
FdwConnShmemGetIterator(HASH_SEQ_STATUS *status)
{
	if (!FdwConnShmemHash)
		return false;

	LWLockAcquire(&fdw_conn_shmem_state->lock, LW_SHARED);
	hash_seq_init(status, FdwConnShmemHash);
	return true;
}

void
FdwConnShmemGetIteratorFinish(void)
{
	LWLockRelease(&fdw_conn_shmem_state->lock);
}

/*
 * Get next entry from iterator
 */
bool
FdwConnShmemGetNext(HASH_SEQ_STATUS *status, char *cluster_name,
					int *local_pid, int *remote_backend_pid)
{
	FdwConnShmemEntry *entry;

	while ((entry = (FdwConnShmemEntry *) hash_seq_search(status)) != NULL)
	{
		if (entry->key.local_pid <= 0 ||
			BackendPidGetProc(entry->key.local_pid) == NULL)
			continue;

		strncpy(cluster_name, entry->key.cluster_name, NAMEDATALEN);
		*local_pid = entry->key.local_pid;
		*remote_backend_pid = entry->remote_backend_pid;
		return true;
	}

	return false;
}

/*
 * Cleanup callback for backend exit
 */
static void
fdw_conn_shmem_on_proc_exit(int code, Datum arg)
{
	FdwConnShmemUnregisterAll();
}

/*
 * Setup backend exit callback
 */
void
FdwConnShmemOnProcExit(void)
{
	on_proc_exit(fdw_conn_shmem_on_proc_exit, 0);
}
