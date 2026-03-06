/*-------------------------------------------------------------------------
 *
 * parallelthread.h
 *	  Infrastructure for launching parallel thread workers.
 *
 * Thread-based parallel workers allow parallel execution of SELECT queries
 * on temporary tables.  Standard process-based parallel workers cannot
 * access temporary table data because it lives in local buffers that are
 * private to the leader process.  Thread workers run inside the same process
 * as the leader, so they share the address space (including local buffers)
 * and can therefore scan temporary tables in parallel.
 *
 * To safely share local buffers across threads, callers in parallelthread.c
 * hold buf_mutex around every ReadBuffer, ReleaseBuffer, and PrefetchBuffer
 * call, and around VM buffer refreshes (visibilitymap_get_status / ReadBuffer
 * inside visibilitymap.c).  mvcc_mutex is a separate, finer-grained lock
 * held only around HeapTupleSatisfiesVisibility and the slow MVCC path.
 * Individual buffer pages may be read by multiple threads simultaneously
 * once the buffer is pinned; the mutexes only protect the bookkeeping.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 * src/include/access/parallelthread.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARALLELTHREAD_H
#define PARALLELTHREAD_H

#include <pthread.h>
#include <semaphore.h>

#include "access/htup.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "port/atomics.h"
#include "storage/buf.h"
#include "utils/palloc.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

/* Maximum number of thread workers per parallel context */
#define MAX_PARALLEL_THREAD_WORKERS		8

/* Maximum length of a worker error message */
#define PARALLEL_THREAD_ERRMSG_LEN		1024

/*
 * PT_BLOCK_BATCH: number of heap blocks each worker claims per atomic
 * fetch_add on next_block.  Claiming a batch of blocks reduces contention
 * on the atomic counter and allows workers to prefetch the 2nd..Nth block
 * while processing the 1st.
 */
#define PT_BLOCK_BATCH			4

/*
 * PT_BATCH_DATA_BYTES: capacity of the fixed data area inside each
 * PTBatchNode.  Must be large enough to hold all qualifying tuples from
 * a single heap page, including their serialisation headers.
 *
 * Each serialised entry occupies PT_ENTRY_HDR bytes of overhead plus the
 * raw tuple bytes.  MaxHeapTuplesPerPage is ~291 for the default BLCKSZ=8192,
 * so the worst-case overhead is 291 * 14 ≈ 4 KB.  Together with at most
 * BLCKSZ bytes of raw tuple data, 16 KB is comfortably sufficient.
 */
#define PT_BATCH_DATA_BYTES		16384

/*
 * PT_ENTRY_HDR: bytes of fixed-size metadata preceding each serialised
 * tuple in a PTBatchNode's data[] area:
 *   uint32          t_len        (4 bytes)
 *   Oid             t_tableOid   (4 bytes)
 *   ItemPointerData t_self       (6 bytes)
 */
#define PT_ENTRY_HDR	((int)(sizeof(uint32) + sizeof(Oid) + sizeof(ItemPointerData)))

/* Free-pool nodes pre-allocated per worker (plus a small constant margin). */
#define PT_POOL_PER_WORKER		4

/*
 * ParallelThreadTupleList
 *
 * A lock-free MPSC (multiple-producer, single-consumer) singly-linked list
 * used to pass batches of tuples from thread workers to the leader.
 *
 * Algorithm (Dmitry Vyukov MPSC intrusive linked list)
 * -----------------------------------------------------
 * Each node (PTBatchNode) carries a full page's worth of qualifying tuples
 * serialised end-to-end in its data[] area.  The leader enqueues one node
 * per page, reducing sem_post/sem_wait calls by ~(tuples-per-page).
 *
 * Nodes come from a pre-allocated PTNodePool.  Workers pop from the pool
 * front; the leader returns consumed nodes to the pool back (FIFO order).
 *
 * Producer (enqueue):
 *   1. Fill the node's data[] with qualifying tuples for the current page.
 *   2. Atomically exchange tail with the node.
 *   3. Store-release prev->next = node.
 *   4. sem_post(items_sem) to unblock the consumer.
 *
 * Consumer (dequeue):
 *   1. sem_wait(items_sem) (or sem_trywait + leader scan if empty).
 *   2. Spin-read head->next until non-NULL.
 *   3. Advance head = next (next becomes the new dummy).
 *   4. Unpack tuples one by one from next->data[], returning the node to
 *      the pool after the last tuple.
 */

/* Opaque to callers; defined fully in parallelthread.c */
typedef struct ParallelThreadTupleList ParallelThreadTupleList;

/*
 * PTNodePool
 *
 * Lock-free SPMC (single-producer multiple-consumer) FIFO pool of
 * PTBatchNode objects.  Workers pop batch nodes from the front to fill and
 * enqueue; the leader returns consumed nodes to the back.
 *
 * Workers use CAS on an atomic head index to pop (multiple consumers).
 * The leader writes ring[tail % cap] then increments the non-atomic tail
 * (single producer — safe because only the leader calls pool_return).
 *
 * FIFO ordering: a returned node must cycle through the full pool before it
 * can be popped again, preventing ABA reuse in the MPSC list.
 *
 * Opaque to callers; defined fully in parallelthread.c.
 */
typedef struct PTNodePool PTNodePool;


/* Forward declaration */
struct ParallelThreadContext;

/*
 * ParallelThreadWorkerState
 *
 * Per-worker state, allocated inside ParallelThreadContext by the leader
 * before launching the thread.
 */
typedef struct ParallelThreadWorkerState
{
	/* Thread handle and identity */
	pthread_t		thread;
	int				worker_num;	/* 0-based index */
	bool			launched;	/* true if pthread_create succeeded */

	/* Error state filled in by the worker if it fails */
	bool			had_error;
	char			errmsg[PARALLEL_THREAD_ERRMSG_LEN];

	/* Per-worker MemoryContext created by the leader before launch */
	MemoryContext	worker_context;

	/*
	 * Per-worker tuple table slot for qual evaluation.  Created by the leader
	 * before thread launch; cleaned up in DestroyParallelThreadContext.
	 * NULL when there is no qual to evaluate.
	 */
	TupleTableSlot *eval_slot;

	/* Back-pointer to the owning context */
	struct ParallelThreadContext *ptcxt;
} ParallelThreadWorkerState;


/*
 * ParallelThreadContext
 *
 * Top-level object managing a pool of parallel thread workers.
 * Created and owned by the leader process.
 */
typedef struct ParallelThreadContext
{
	/* Number of workers requested / actually launched */
	int				nworkers;
	int				nworkers_launched;

	/* Shared result list (all workers write, leader reads) */
	ParallelThreadTupleList *list;

	/* Worker states (nworkers entries) */
	ParallelThreadWorkerState *workers;

	/*
	 * Pre-allocated FIFO node pool.  Workers pop batch nodes from the front
	 * to fill and enqueue; the leader returns consumed nodes to the back.
	 * Created in CreateParallelThreadContext; destroyed in
	 * DestroyParallelThreadContext.
	 */
	PTNodePool		*pool;

	/*
	 * Consumer state for unpacking a multi-tuple batch node one tuple at a
	 * time.  consumer_batch is the node currently being drained; it is
	 * returned to the pool once consumer_tuple_idx reaches batch_count.
	 * consumer_data_off is the byte offset into consumer_batch->data[].
	 */
	void			*consumer_batch;   /* PTBatchNode *, opaque here */
	int				 consumer_tuple_idx;
	int				 consumer_data_off;

	/*
	 * Dynamic block allocation: workers call pg_atomic_fetch_add_u64 on
	 * next_block to claim the next block to scan, analogous to
	 * phs_nallocated in ParallelBlockTableScanDesc.
	 */
	pg_atomic_uint64 next_block;
	BlockNumber		 nblocks;	/* total number of blocks in the relation */
	BlockNumber		 prefetch_limit; /* blocks pre-fetched by launcher */

	/*
	 * Number of worker-done sentinels received by ParallelThreadGetNextTuple.
	 * Must be a context field (not a local variable) so that the count
	 * accumulates correctly across multiple calls to the function.
	 */
	int				 sentinels_received;

	/*
	 * Buffer serialisation mutex.  PostgreSQL local-buffer bookkeeping state
	 * (LocalBufHash, LocalRefCount, nextFreeLocalBufId, NLocalPinnedBuffers)
	 * is not individually atomic.  buf_mutex serialises every ReadBuffer,
	 * ReleaseBuffer, and PrefetchBuffer call, as well as VM buffer refreshes
	 * (visibilitymap_get_status calls ReadBuffer internally).
	 */
	pthread_mutex_t  buf_mutex;

	/*
	 * MVCC serialisation mutex.  PostgreSQL LWLocks are not thread-safe: they
	 * use MyProc for sleep/wakeup and InterruptHoldoffCount for interrupt
	 * tracking, both of which are process-global.  Worker threads must not
	 * call into LWLock code simultaneously.
	 *
	 * HeapTupleSatisfiesVisibility → TransactionIdGetStatus acquires the
	 * XactSLRU LWLock.  Workers hold mvcc_mutex around every
	 * HeapTupleSatisfiesVisibility call to ensure at most one thread is
	 * inside LWLock code at any time.
	 * The leader also holds this mutex when it scans a page locally.
	 */
	pthread_mutex_t  mvcc_mutex;

	/*
	 * Leader-side page scanning.  When the result list is momentarily empty,
	 * ParallelThreadGetNextTuple has the leader claim a block from next_block
	 * and scan it directly rather than blocking on the semaphore.  Visible
	 * tuples from that page are stored here and returned one per call.
	 *
	 * leader_rel / leader_snapshot are saved from the LaunchParallelThreadWorkers
	 * arguments so the leader can call ReadBuffer and HeapTupleSatisfiesVisibility
	 * without extra parameters.
	 *
	 * leader_vmbuf is the leader's pinned visibility-map buffer, kept across
	 * calls for efficiency (consecutive blocks often share the same VM page).
	 * Initialised to InvalidBuffer; released in DestroyParallelThreadContext.
	 */
	Relation		 leader_rel;
	Snapshot		 leader_snapshot;
	Buffer			 leader_vmbuf;		/* pinned VM buffer for leader scans */
	HeapTuple		*leader_pending_tups;  /* palloc'd array of palloc'd tuples */
	int				 leader_pending_ntups; /* valid entries in array */
	int				 leader_pending_idx;   /* index of next tuple to return */
} ParallelThreadContext;


/*
 * Thread-local worker number.
 * -1  => not a parallel thread worker (main/leader context)
 * >=0 => worker index within the enclosing ParallelThreadContext
 *
 * Declared __thread so each thread has its own copy.  The main thread's
 * value stays -1 throughout; each worker thread sets it to its assigned
 * worker_num before executing any user code.
 */
extern __thread int ParallelThreadWorkerNumber;

#define IsParallelThreadWorker()	(ParallelThreadWorkerNumber >= 0)


/* ------------------------------------------------------------------
 * Public API
 * ------------------------------------------------------------------ */

/* Shared-memory size and initialisation — called from ipci.c */
extern Size ParallelThreadShmemSize(void);
extern void ParallelThreadShmemInit(void);

extern ParallelThreadContext *CreateParallelThreadContext(int nworkers);

extern void LaunchParallelThreadWorkers(ParallelThreadContext *ptcxt,
										Relation rel,
										Snapshot snapshot,
										BlockNumber nblocks,
										List *qual,
										ExprContext *leader_econtext);

/*
 * ParallelThreadGetNextTuple - retrieve the next tuple from the result queue.
 *
 * Returns a palloc'd HeapTuple, or NULL when all workers are done and the
 * queue is empty.  Raises ereport(ERROR) if any worker encountered an error.
 * The caller is responsible for pfree'ing the returned tuple when done.
 */
extern HeapTuple ParallelThreadGetNextTuple(ParallelThreadContext *ptcxt);

extern void WaitForParallelThreadWorkers(ParallelThreadContext *ptcxt);

extern void DestroyParallelThreadContext(ParallelThreadContext *ptcxt);




#endif							/* PARALLELTHREAD_H */
