/*-------------------------------------------------------------------------
 *
 * parallelthread.c
 *	  Infrastructure for parallel thread workers.
 *
 * Thread-based parallel workers allow parallel execution of SELECT queries
 * on temporary tables.  Unlike the standard process-based parallel workers,
 * thread workers run inside the same process as the leader and therefore
 * share the address space (including local buffers used by temporary tables).
 *
 * Architecture overview
 * ---------------------
 * The leader creates a ParallelThreadContext, launches N worker threads, and
 * then reads result tuples from a shared ParallelThreadTupleQueue.  Each
 * worker is assigned a disjoint range of heap blocks to scan.  Workers push
 * raw HeapTuple bytes into the queue; the leader reconstructs HeapTuples from
 * those bytes and hands them to the executor.
 *
 * Thread safety
 * -------------
 * Key PostgreSQL globals that affect error handling and memory allocation
 * (PG_exception_stack, error_context_stack, errordata, CurrentMemoryContext,
 * ErrorContext, CurrentResourceOwner) are declared with __thread (see elog.c,
 * mcxt.c, resowner.c) so that each thread has its own independent copy.
 * Access to the local buffer bookkeeping state (LocalBufHash, LocalRefCount,
 * nextFreeLocalBufId, NLocalPinnedBuffers) is serialised by buf_mutex: every
 * ReadBuffer, ReleaseBuffer, and PrefetchBuffer call in the hot path is
 * wrapped with pthread_mutex_lock/unlock(buf_mutex) so that at most one
 * thread modifies the local buffer hash table at a time.  VM buffer refreshes
 * (visibilitymap_get_status, which calls ReadBuffer internally) are also
 * covered by buf_mutex.  A separate mvcc_mutex serialises only
 * HeapTupleSatisfiesVisibility (which acquires LWLocks internally).
 * Page data in pinned buffers may be read concurrently without holding any
 * mutex.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/parallelthread.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <pthread.h>
#include <sched.h>
#include <semaphore.h>
#include <setjmp.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/parallelthread.h"
#include "access/visibilitymap.h"
#include "executor/executor.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/itemid.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/guc.h"
#include "storage/shmem.h"
#include "miscadmin.h"


/*
 * pt_sema_wait / pt_sema_post / pt_sema_trywait
 *
 * Thin wrappers around sem_wait / sem_post / sem_trywait that retry on EINTR,
 * matching the behaviour of PGSemaphoreLock / PGSemaphoreUnlock /
 * PGSemaphoreTryLock (see backend/port/posix_sema.c).
 *
 * PGSemaphoreCreate cannot be called from a backend (it asserts
 * !IsUnderPostmaster), so we manage our own sem_t instances directly, but
 * ensure the same signal-safe retry semantics that PGSemaphore provides.
 */
static inline void
pt_sema_wait(sem_t *s)
{
	int		ret;

	do
	{
		ret = sem_wait(s);
	} while (ret < 0 && errno == EINTR);

	if (ret < 0)
		elog(FATAL, "sem_wait failed: %m");
}

static inline void
pt_sema_post(sem_t *s)
{
	int		ret;

	do
	{
		ret = sem_post(s);
	} while (ret < 0 && errno == EINTR);

	if (ret < 0)
		elog(FATAL, "sem_post failed: %m");
}

static inline bool
pt_sema_trywait(sem_t *s)
{
	int		ret;

	do
	{
		ret = sem_trywait(s);
	} while (ret < 0 && errno == EINTR);

	return ret;
}


/* Thread-local worker number; -1 means "not a thread worker". */
__thread int ParallelThreadWorkerNumber = -1;


/* ------------------------------------------------------------------
 * Global shared-memory limit for thread workers
 * ------------------------------------------------------------------ */

/*
 * ParallelThreadShmemStruct
 *
 * A tiny shared-memory struct that tracks the total number of thread workers
 * currently active across all backends.  This lets us enforce a server-wide
 * limit (max_parallel_thread_workers) similar to how max_parallel_workers
 * caps process-based background workers.
 *
 * active_thread_workers is modified atomically:
 *   - incremented (fetch_add) before attempting each pthread_create; decremented
 *     back immediately if the limit is exceeded or pthread_create fails
 *   - decremented (fetch_sub) when the worker thread reaches worker_done
 */
typedef struct ParallelThreadShmemStruct
{
	pg_atomic_uint32 active_thread_workers;
} ParallelThreadShmemStruct;

static ParallelThreadShmemStruct *ParallelThreadShmem = NULL;

/* GUC: server-wide cap on thread workers; declared in cost.h */
int		max_parallel_thread_workers = 8;

/*
 * ParallelThreadShmemSize
 *
 * Return the number of bytes of shared memory needed by this module.
 * Called from CalculateShmemSize() in ipci.c.
 */
Size
ParallelThreadShmemSize(void)
{
	return sizeof(ParallelThreadShmemStruct);
}

/*
 * ParallelThreadShmemInit
 *
 * Allocate (or re-attach to) the module's shared-memory area and
 * initialise it on first call.  Called from CreateOrAttachShmemStructs()
 * in ipci.c, both for the postmaster and for forked backends.
 */
void
ParallelThreadShmemInit(void)
{
	bool found;

	ParallelThreadShmem = (ParallelThreadShmemStruct *)
		ShmemInitStruct("Parallel Thread Workers",
						sizeof(ParallelThreadShmemStruct),
						&found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		pg_atomic_init_u32(&ParallelThreadShmem->active_thread_workers, 0);
	}
	else
		Assert(found);
}


/*
 * qual_safe_for_thread_workers
 *
 * Return true if the qual list can be safely compiled with ExecInitQual(qual,
 * NULL) and evaluated inside a thread worker's standalone ExprContext.
 *
 * We only reject quals that contain SubPlan/InitPlan nodes, because those
 * require a parent PlanState to initialise.  Quals with Param nodes are fine:
 * each worker's ExprContext gets the param-exec/param-list pointers copied
 * from the leader's ExprContext, so PARAM_EXEC lookups work correctly.
 * Aggregate nodes cannot appear in a scan-level qual (only in HAVING), so
 * no special check is needed for them.
 */
static bool
qual_safe_for_thread_workers(List *qual)
{
	if (qual == NIL)
		return false;			/* no qual to compile */
	if (contain_subplans((Node *) qual))
		return false;
	return true;
}


/* ------------------------------------------------------------------
 * Lock-free MPSC batch-node list + pre-allocated FIFO node pool
 * ------------------------------------------------------------------ */

/*
 * PTBatchNode - one element in the MPSC linked list.
 *
 * Replaces the old per-tuple PTListNode.  Instead of one node per visible
 * tuple, a PTBatchNode carries all qualifying tuples from an entire heap
 * page, serialised end-to-end in its fixed-size data[] area.  This reduces
 * MPSC list operations (and sem_post/sem_wait pairs) by roughly
 * (tuples-per-page) times compared to per-tuple enqueueing.
 *
 * data[] layout for each serialised tuple (no padding between entries):
 *   uint32          t_len
 *   Oid             t_tableOid
 *   ItemPointerData t_self    (6 bytes)
 *   char            tuple_data[t_len]   (raw HeapTupleHeader bytes)
 *
 * Nodes come from a pre-allocated PTNodePool, not from palloc.
 */
typedef struct PTBatchNode
{
	pg_atomic_uint64 next;			/* Vyukov MPSC link; 0 == NULL */
	bool			 is_sentinel;	/* true = worker-done marker */
	int				 batch_count;	/* number of serialised tuples */
	int				 data_len;		/* bytes used in data[] */
	char			 data[PT_BATCH_DATA_BYTES]; /* packed tuple entries */
} PTBatchNode;

/* Helpers to convert between pointer and uint64 */
#define PTR_TO_U64(p)	((uint64) (uintptr_t) (p))
#define U64_TO_BPTR(u)	((PTBatchNode *) (uintptr_t) (u))

/*
 * ParallelThreadTupleList
 *
 * Lock-free MPSC singly-linked list (Dmitry Vyukov's algorithm).
 *
 * head     - the current dummy node; only the consumer reads/writes this.
 * tail     - the atomic tail pointer; each producer atomically swaps this to
 *            append a new node.
 * items_sem - semaphore (init 0) that the consumer waits on; each producer
 *            posts once after making its new node reachable from the previous
 *            tail node.
 */
struct ParallelThreadTupleList
{
	PTBatchNode		   *init_sentinel;	/* initial dummy; returned to pool on destroy */
	PTBatchNode		   *head;			/* consumer's current dummy */
	pg_atomic_uint64	tail;			/* atomic tail pointer */

	sem_t				items_sem;		/* consumer blocks here */
	int					nwriters;		/* expected sentinel count */

	/* Error reporting: atomic flag + write-once message buffer */
	pg_atomic_uint32	had_error;
	pthread_mutex_t		errmsg_mutex;
	char				errmsg[PARALLEL_THREAD_ERRMSG_LEN];
};

/*
 * PTNodePool
 *
 * Lock-free SPMC (single-producer multiple-consumer) FIFO pool of
 * PTBatchNode objects.
 *
 * Workers (multiple consumers) pop from the front by CAS-advancing head.
 * The leader (single producer) pushes to the back by writing ring[tail%cap]
 * and then incrementing the non-atomic tail (safe: only the leader writes).
 *
 * FIFO ordering: a returned node must traverse the full pool length before
 * it can be reused, preventing ABA reuse in the MPSC list.
 *
 * No mutex is needed:
 *   - pool_pop   : workers atomically claim index h via CAS(head, h, h+1),
 *                  then read ring[h % cap].
 *   - pool_return: leader writes ring[tail % cap] + write-barrier + tail++.
 *
 * If the pool is momentarily exhausted, pool_pop falls back to palloc so
 * workers never stall.  pool_return detects palloc'd nodes via pointer
 * range check and frees them instead of returning them to the ring.
 */
struct PTNodePool
{
	pg_atomic_uint64  head;		/* pop index; workers CAS-advance */
	uint64			  tail;		/* push index; only the leader writes this */
	uint64			  cap;		/* ring capacity (>= n_backing + 1) */
	PTBatchNode		 *backing;	/* flat pre-allocated array of nodes */
	int				  n_backing;/* number of elements in backing[] */
	PTBatchNode		**ring;		/* circular ring of node pointers (cap entries) */
};

/*
 * pt_atomic_exchange_u64
 *
 * Atomically replace *ptr with newval and return the old value.
 * Implemented via CAS loop because pg_atomic_exchange_u64 is not part of
 * PostgreSQL's portable atomics API.
 */
static uint64
pt_atomic_exchange_u64(pg_atomic_uint64 *ptr, uint64 newval)
{
	uint64		oldval = pg_atomic_read_u64(ptr);

	while (!pg_atomic_compare_exchange_u64(ptr, &oldval, newval))
		/* oldval updated by failed CAS; retry */ ;
	return oldval;
}

/* ---- Pool helpers ---- */

/*
 * create_node_pool
 *
 * Allocate and initialise the node pool.  Called single-threaded from the
 * leader.
 */
static PTNodePool *
create_node_pool(int nworkers)
{
	PTNodePool *pool;
	int			n;
	int			i;

	/*
	 * Allocate enough nodes for each worker to have PT_POOL_PER_WORKER nodes
	 * in-flight simultaneously, plus a fixed margin:
	 *   +nworkers  one sentinel node per worker (enqueued at end of scan)
	 *   +1         the initial MPSC dummy node
	 *   +1         slack / rounding
	 * Total margin = nworkers + 2 ≤ MAX_PARALLEL_THREAD_WORKERS + 2 ≤ 10, so
	 * capping at MAX_PARALLEL_THREAD_WORKERS + 2 keeps the expression readable.
	 */
	n = nworkers * PT_POOL_PER_WORKER + (nworkers + 2);

	pool = (PTNodePool *) palloc0(sizeof(PTNodePool));
	pool->n_backing = n;
	pool->cap		= (uint64) n + 1;	/* cap > n_backing so ring never wraps */
	pool->backing	= (PTBatchNode *) palloc0((Size) n * sizeof(PTBatchNode));
	pool->ring		= (PTBatchNode **) palloc((Size) pool->cap * sizeof(PTBatchNode *));

	/* All nodes pre-loaded: head = 0, tail = n */
	pg_atomic_init_u64(&pool->head, 0);
	pool->tail = (uint64) n;

	for (i = 0; i < n; i++)
	{
		pg_atomic_init_u64(&pool->backing[i].next, PTR_TO_U64(NULL));
		pool->ring[i] = &pool->backing[i];
	}

	return pool;
}

/*
 * pool_pop
 *
 * Pop a node from the front of the free pool (called by workers).
 * Returns a ready-to-use node with batch_count=0, data_len=0.
 * Falls back to palloc0 if the pool is temporarily exhausted.
 *
 * Multiple consumers (workers) call this concurrently; they compete via CAS
 * on pool->head.  No mutex is needed.
 */
static PTBatchNode *
pool_pop(PTNodePool *pool)
{
	PTBatchNode *node = NULL;

	for (;;)
	{
		uint64	h = pg_atomic_read_u64(&pool->head);
		uint64	t;

		/*
		 * Acquire barrier: ensure we see the ring slot write that the
		 * producer issued before incrementing tail.
		 */
		pg_read_barrier();
		t = pool->tail;

		if (h >= t)
		{
			/* Pool exhausted; fall back to palloc so workers never stall. */
			node = (PTBatchNode *) palloc0(sizeof(PTBatchNode));
			pg_atomic_init_u64(&node->next, PTR_TO_U64(NULL));
			break;
		}

		/*
		 * Atomically claim index h.  If the CAS fails, another worker got
		 * here first; re-read head and retry.
		 */
		if (pg_atomic_compare_exchange_u64(&pool->head, &h, h + 1))
		{
			/*
			 * CAS is a full memory barrier; the ring slot write is visible.
			 */
			node = pool->ring[h % pool->cap];
			break;
		}
		/* CAS failed: retry */
	}

	/* Reset node fields for fresh use */
	node->is_sentinel = false;
	node->batch_count = 0;
	node->data_len	  = 0;
	pg_atomic_write_u64(&node->next, PTR_TO_U64(NULL));

	return node;
}

/*
 * pool_return
 *
 * Return a consumed node to the back of the free pool (called ONLY by the
 * leader — single producer).  If the node was palloc'd as a fallback (not
 * part of pool->backing[]), free it directly.
 */
static void
pool_return(PTNodePool *pool, PTBatchNode *node)
{
	/* Check if this node belongs to the pre-allocated backing array. */
	if (node < pool->backing || node >= pool->backing + pool->n_backing)
	{
		/* palloc'd fallback node — free it directly */
		pfree(node);
		return;
	}

	/* Reset the node so workers get a clean slate. */
	node->is_sentinel = false;
	node->batch_count = 0;
	node->data_len	  = 0;
	pg_atomic_write_u64(&node->next, PTR_TO_U64(NULL));

	/*
	 * Write the node into the ring, then issue a write barrier before
	 * incrementing tail.  The barrier ensures workers that see the new tail
	 * value also see the ring slot write (acquire-release pairing with the
	 * pg_read_barrier() in pool_pop).
	 */
	pool->ring[pool->tail % pool->cap] = node;
	pg_write_barrier();
	pool->tail++;
}

/*
 * destroy_node_pool
 *
 * Free all pool resources.  Must be called only after all workers have
 * joined and all batch nodes have been consumed and returned.
 */
static void
destroy_node_pool(PTNodePool *pool)
{
	pfree(pool->ring);
	pfree(pool->backing);
	pfree(pool);
}

/* ---- MPSC list helpers ---- */

/*
 * list_enqueue
 *
 * Append node to the MPSC list tail using the Vyukov algorithm.
 * May be called from multiple producer threads concurrently.
 */
static void
list_enqueue(struct ParallelThreadTupleList *list, PTBatchNode *node)
{
	uint64		 prev_u64;
	PTBatchNode *prev;

	prev_u64 = pt_atomic_exchange_u64(&list->tail, PTR_TO_U64(node));
	prev	 = U64_TO_BPTR(prev_u64);

	/*
	 * Write barrier: ensure the consumer sees node's payload before the
	 * non-NULL next pointer becomes visible.
	 */
	pg_write_barrier();
	pg_atomic_write_u64(&prev->next, PTR_TO_U64(node));

	/* Wake the consumer. */
	pt_sema_post(&list->items_sem);
}

/*
 * batch_add_tuple
 *
 * Serialise htup into the batch node's data[] area.
 * Returns false (and leaves the node unchanged) if data[] is full.
 */
static bool
batch_add_tuple(PTBatchNode *node, HeapTuple htup)
{
	int		entry_size = PT_ENTRY_HDR + (int) htup->t_len;
	char   *p;

	if (node->data_len + entry_size > PT_BATCH_DATA_BYTES)
		return false;			/* node is full */

	p = node->data + node->data_len;
	memcpy(p, &htup->t_len, sizeof(uint32));
	p += sizeof(uint32);
	memcpy(p, &htup->t_tableOid, sizeof(Oid));
	p += sizeof(Oid);
	memcpy(p, &htup->t_self, sizeof(ItemPointerData));
	p += sizeof(ItemPointerData);
	memcpy(p, htup->t_data, htup->t_len);

	node->data_len += entry_size;
	node->batch_count++;
	return true;
}

/*
 * batch_read_tuple
 *
 * Deserialise one tuple from a batch node at byte offset *offsetp.
 * Advances *offsetp past the tuple.  Returns a palloc'd HeapTuple.
 */
static HeapTuple
batch_read_tuple(PTBatchNode *node, int *offsetp)
{
	const char	   *p = node->data + *offsetp;
	uint32			t_len;
	Oid				t_tableOid;
	ItemPointerData t_self;
	HeapTuple		result;

	memcpy(&t_len, p, sizeof(uint32));
	p += sizeof(uint32);
	memcpy(&t_tableOid, p, sizeof(Oid));
	p += sizeof(Oid);
	memcpy(&t_self, p, sizeof(ItemPointerData));
	p += sizeof(ItemPointerData);

	result = (HeapTuple) palloc(HEAPTUPLESIZE + t_len);
	result->t_len	   = t_len;
	result->t_tableOid = t_tableOid;
	result->t_self	   = t_self;
	result->t_data	   = (HeapTupleHeader) ((char *) result + HEAPTUPLESIZE);
	memcpy(result->t_data, p, t_len);

	*offsetp += PT_ENTRY_HDR + (int) t_len;
	return result;
}

/*
 * list_worker_done
 *
 * Called by a worker when it has finished (successfully or with an error).
 *
 * If filling_batch is non-NULL and has accumulated tuples, it is enqueued
 * first.  A sentinel node (marking this worker as done) is then enqueued.
 * The sentinel reuses filling_batch if it is already empty; otherwise a
 * fresh node is obtained from the pool.
 */
static void
list_worker_done(struct ParallelThreadTupleList *list,
				 PTNodePool *pool,
				 PTBatchNode *filling_batch,
				 bool had_error, const char *errmsg)
{
	PTBatchNode *sentinel;

	if (had_error)
	{
		if (pg_atomic_read_u32(&list->had_error) == 0)
		{
			pthread_mutex_lock(&list->errmsg_mutex);
			if (pg_atomic_read_u32(&list->had_error) == 0)
			{
				strlcpy(list->errmsg,
						errmsg ? errmsg : "unknown error in parallel thread worker",
						PARALLEL_THREAD_ERRMSG_LEN);
				pg_write_barrier();
				pg_atomic_write_u32(&list->had_error, 1);
			}
			pthread_mutex_unlock(&list->errmsg_mutex);
		}
	}

	/* Flush any accumulated tuples in the partial batch first. */
	if (filling_batch != NULL && filling_batch->batch_count > 0)
	{
		list_enqueue(list, filling_batch);
		/* Claim a fresh node for the sentinel. */
		sentinel = pool_pop(pool);
	}
	else if (filling_batch != NULL)
	{
		/* Reuse the empty filling_batch node as the sentinel; saves a pool op. */
		sentinel = filling_batch;
	}
	else
	{
		sentinel = pool_pop(pool);
	}

	sentinel->is_sentinel = true;
	sentinel->batch_count = 0;
	sentinel->data_len	  = 0;
	pg_atomic_write_u64(&sentinel->next, PTR_TO_U64(NULL));

	list_enqueue(list, sentinel);
}

/*
 * CreateParallelThreadTupleList
 *
 * Allocate and initialise the MPSC list.  Called single-threaded from the
 * leader before launching any workers.  pool must already be created.
 */
static struct ParallelThreadTupleList *
CreateParallelThreadTupleList(PTNodePool *pool)
{
	struct ParallelThreadTupleList *list;
	PTBatchNode *dummy;

	list = (struct ParallelThreadTupleList *)
		palloc0(sizeof(struct ParallelThreadTupleList));

	/*
	 * Initial dummy/sentinel node: anchors head and tail before any producer
	 * runs.  Comes from the pool; returned to pool on DestroyParallelThreadTupleList.
	 */
	dummy = pool_pop(pool);
	dummy->is_sentinel = false;
	dummy->batch_count = 0;
	dummy->data_len	   = 0;

	list->init_sentinel = dummy;
	list->head			= dummy;
	pg_atomic_init_u64(&list->tail, PTR_TO_U64(dummy));

	if (sem_init(&list->items_sem, 0, 0) != 0)
		elog(ERROR, "could not initialise parallel thread list semaphore");

	list->nwriters = 0;
	pg_atomic_init_u32(&list->had_error, 0);
	if (pthread_mutex_init(&list->errmsg_mutex, NULL) != 0)
		elog(ERROR, "could not initialise parallel thread list error mutex");

	return list;
}

/*
 * DestroyParallelThreadTupleList
 *
 * Release list resources.  Must be called only after all workers have
 * joined and the consumer has finished reading.
 */
static void
DestroyParallelThreadTupleList(struct ParallelThreadTupleList *list,
							   PTNodePool *pool)
{
	sem_destroy(&list->items_sem);
	pthread_mutex_destroy(&list->errmsg_mutex);
	/*
	 * Return the current dummy head to the pool.  This is either the initial
	 * dummy (if no items were dequeued), or the last node that was advanced
	 * past as old_head during dequeue (which is now list->head after each
	 * dequeue).  Covers both the consumer_batch case (consumer_batch == head
	 * when a batch is partially consumed) and the all-done case (head points
	 * to the last sentinel).
	 */
	pool_return(pool, list->head);
	pfree(list);
}


/* ------------------------------------------------------------------
 * Internal structures for thread scan context
 * ------------------------------------------------------------------ */

/*
 * Arguments passed to each thread at launch time.  These are read-only after
 * the thread starts (the leader must not modify them while threads run),
 * except for ptcxt->next_block which is updated atomically.
 */
typedef struct ThreadScanArgs
{
	ParallelThreadContext *ptcxt;		/* owning context (for next_block) */
	Relation		rel;				/* relation to scan (open in leader) */
	Snapshot		snapshot;			/* snapshot for visibility checks */
	struct ParallelThreadTupleList *list;	/* result list */
	int				worker_num;

	/* Per-worker MemoryContext created by the leader before launch */
	MemoryContext	worker_context;
	MemoryContext	worker_error_context;

	/*
	 * Per-worker qual machinery, all compiled/created by the leader in the
	 * worker's memory context before the thread is launched.  worker_qual is
	 * NULL when the scan has no qual or when the qual cannot be evaluated
	 * without a parent PlanState (e.g., it contains SubPlans).
	 */
	ExprState	   *worker_qual;		/* compiled qual, or NULL */
	ExprContext	   *econtext;			/* standalone eval context, or NULL */
	TupleTableSlot *eval_slot;			/* heap-tuple slot for eval, or NULL */

	/*
	 * Pointer to the context's mvcc_mutex.  Workers must hold this mutex
	 * around every HeapTupleSatisfiesVisibility call to prevent concurrent
	 * LWLock acquisitions (LWLocks are not thread-safe: they use the shared
	 * MyProc for sleep/wakeup).
	 */
	pthread_mutex_t *mvcc_mutex;

	/*
	 * Pointer to the context's buf_mutex.  Workers must hold this mutex
	 * around every ReadBuffer, ReleaseBuffer, and PrefetchBuffer call.
	 */
	pthread_mutex_t *buf_mutex;

	/*
	 * Per-worker visibility map buffer, kept pinned across block iterations
	 * for efficiency (consecutive blocks often share the same VM page).
	 * Initialised to InvalidBuffer; released explicitly before ResourceOwner
	 * cleanup at the end of the worker.
	 */
	Buffer		vmbuf;

	/*
	 * Current batch node being filled.  Obtained from ptcxt->pool before the
	 * thread is launched; enqueued (possibly together with a fresh node for
	 * the next page) during the scan.  After all blocks are processed the
	 * remainder is flushed in list_worker_done.
	 */
	PTBatchNode *filling_batch;
} ThreadScanArgs;


/*
 * pt_vm_mapblock - VM fork block number for a given heap block.
 *
 * Matches the HEAPBLK_TO_MAPBLOCK formula in visibilitymap.c without
 * requiring that internal macro to be exported.  Used to decide whether the
 * cached vmbuf still covers the current heap block (and therefore whether
 * buf_mutex must be held to refresh vmbuf).
 *
 * Note: each worker's vmbuf is stored in its private ThreadScanArgs, and the
 * leader's vmbuf is stored in ParallelThreadContext.leader_vmbuf.  Neither is
 * shared between different threads, so comparing BufferGetBlockNumber(vmbuf)
 * against pt_vm_mapblock(blkno) without holding buf_mutex is safe: only one
 * thread ever reads or writes that vmbuf field.
 */
static inline BlockNumber
pt_vm_mapblock(BlockNumber heap_blkno)
{
	const uint32 hbpp =
		(BLCKSZ - (uint32) MAXALIGN(SizeOfPageHeaderData)) *
		(BITS_PER_BYTE / BITS_PER_HEAPBLOCK);

	return heap_blkno / hbpp;
}


/* ------------------------------------------------------------------
 * Thread scan worker
 * ------------------------------------------------------------------ */

/*
 * thread_scan_worker
 *
 * Entry point for each parallel thread worker.  Scans heap blocks claimed
 * in batches of PT_BLOCK_BATCH via next_block and pushes qualifying tuples
 * into the shared MPSC list as PTBatchNode batches (one node per page).
 *
 * Performance design:
 *   - Coarser work units: workers claim PT_BLOCK_BATCH blocks per atomic op,
 *     reducing contention on next_block.
 *   - Prefetch: after claiming a batch, issue PrefetchBuffer for blocks 2..N
 *     so their I/O is in-flight while we process block 1.
 *   - Batch accumulation: qualifying tuples are packed into filling_batch
 *     (a pool node) without any palloc or list operation per tuple.  The
 *     batch is enqueued once per page (one sem_post per page, not per tuple).
 *   - Pool: filling_batch comes from the pre-allocated pool; workers never
 *     call palloc in the hot tuple-copy path.
 */
static void *
thread_scan_worker(void *arg)
{
	ThreadScanArgs *args = (ThreadScanArgs *) arg;
	sigjmp_buf	local_jmp;
	struct ParallelThreadTupleList *list = args->list;
	bool		had_error = false;
	char		errmsg[PARALLEL_THREAD_ERRMSG_LEN];
	PTBatchNode *filling_batch = args->filling_batch;

	/* Set the thread-local worker number. */
	ParallelThreadWorkerNumber = args->worker_num;

	/*
	 * Set up thread-local versions of the key PostgreSQL globals so that
	 * error handling and memory allocation work independently for this thread.
	 */
	CurrentMemoryContext = args->worker_context;
	ErrorContext = args->worker_error_context;
	CurrentResourceOwner = NULL;
	PG_exception_stack = NULL;
	error_context_stack = NULL;

	/*
	 * Create a minimal ResourceOwner for this thread so that
	 * ResourceOwnerEnlarge() and ResourceOwnerRememberBuffer() have a valid
	 * owner to work with.  We create it under the worker context so it is
	 * automatically released when the context is deleted.
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "ParallelThreadWorker");

	/*
	 * Set up a top-level exception handler for this thread.  If PostgreSQL
	 * raises an error (e.g., via ereport/elog) the siglongjmp will land here
	 * and we can report the failure back to the leader via the queue.
	 */
	if (sigsetjmp(local_jmp, 1) != 0)
	{
		ErrorData  *edata = CopyErrorData();

		if (edata != NULL)
		{
			strlcpy(errmsg, edata->message ? edata->message : "unknown error",
					PARALLEL_THREAD_ERRMSG_LEN);
			FreeErrorData(edata);
		}
		else
			strlcpy(errmsg, "unknown error in parallel thread worker",
					PARALLEL_THREAD_ERRMSG_LEN);

		had_error = true;
		goto worker_done;
	}

	/* Register this thread's exception stack so elog(ERROR) lands here. */
	PG_exception_stack = &local_jmp;

	/*
	 * Wire up the per-worker ExprContext to its scan slot.
	 */
	if (args->econtext != NULL)
		args->econtext->ecxt_scantuple = args->eval_slot;

	/*
	 * Main scan loop.
	 *
	 * Each iteration claims a batch of PT_BLOCK_BATCH consecutive blocks
	 * atomically, issues prefetch hints for blocks 2..N in the batch, then
	 * processes each block in sequence.
	 */
	for (;;)
	{
		uint64		 batch_start;
		uint64		 batch_end;
		uint64		 pb;
		uint64		 blkno_u64;

		/* Claim the next batch of blocks. */
		batch_start = pg_atomic_fetch_add_u64(&args->ptcxt->next_block,
											  PT_BLOCK_BATCH);
		if (batch_start >= (uint64) args->ptcxt->nblocks)
			break;				/* all blocks claimed */

		batch_end = Min(batch_start + PT_BLOCK_BATCH,
						(uint64) args->ptcxt->nblocks);

		/*
		 * Prefetch blocks 2..N of this batch under buf_mutex.
		 * PrefetchLocalBuffer calls hash_search(LocalBufHash, HASH_FIND);
		 * all local-buffer hash-table operations must be serialised via
		 * buf_mutex (LocalBufMutex has been removed from localbuf.c).
		 * Skip blocks already pre-fetched by the launcher.
		 */
		for (pb = Max(batch_start + 1, (uint64) args->ptcxt->prefetch_limit);
			 pb < batch_end; pb++)
		{
			pthread_mutex_lock(args->buf_mutex);
			PrefetchBuffer(args->rel, MAIN_FORKNUM, (BlockNumber) pb);
			pthread_mutex_unlock(args->buf_mutex);
		}

		/* Process each block in the claimed batch. */
		for (blkno_u64 = batch_start; blkno_u64 < batch_end; blkno_u64++)
		{
			BlockNumber  blkno = (BlockNumber) blkno_u64;
			Buffer		 buf;
			Page		 page;
			OffsetNumber maxoff;
			OffsetNumber offnum;
			OffsetNumber vis_offnums[MaxHeapTuplesPerPage + 1];
			int			 nvis = 0;
			int			 v;
			bool		 all_visible;

			/*
			 * ReadBuffer under buf_mutex: LocalBufferAlloc modifies
			 * LocalBufHash, LocalRefCount, and nextFreeLocalBufId, none of
			 * which are individually atomic.  Serialising via buf_mutex
			 * keeps all buffer bookkeeping under one lock.
			 */
			pthread_mutex_lock(args->buf_mutex);
			buf = ReadBuffer(args->rel, blkno);
			pthread_mutex_unlock(args->buf_mutex);

			page = BufferGetPage(buf);
			maxoff = PageGetMaxOffsetNumber(page);

			/*
			 * Phase 1: MVCC visibility pass.
			 *
			 * Fast path: if the visibility map marks this page ALL_VISIBLE,
			 * all normal tuples are committed and visible to every
			 * transaction.  We collect all normal offsets without calling
			 * HeapTupleSatisfiesVisibility, skipping mvcc_mutex entirely.
			 *
			 * For the VM buffer (vmbuf), acquire buf_mutex only when we
			 * need to refresh it — i.e. the first time, or when crossing a
			 * VM-page boundary.  When vmbuf is already pinned and covers the
			 * current heap block, VM_ALL_VISIBLE is a single-byte read from
			 * an already-pinned page: no buffer operations, no mutex.
			 *
			 * Each VM page covers HEAPBLOCKS_PER_PAGE consecutive heap
			 * blocks.  Compute the expected VM block number inline using the
			 * same formula as HEAPBLK_TO_MAPBLOCK in visibilitymap.c.
			 *
			 * Slow path: hold mvcc_mutex for the entire page so that at most
			 * one worker calls into LWLock code (HeapTupleSatisfiesVisibility
			 * → TransactionIdGetStatus → SLRU LWLock) at a time.
			 */
			{
				BlockNumber vm_blkno = pt_vm_mapblock(blkno);
				bool		need_vm_refresh =
					!BufferIsValid(args->vmbuf) ||
					BufferGetBlockNumber(args->vmbuf) != vm_blkno;

				if (need_vm_refresh)
				{
					pthread_mutex_lock(args->buf_mutex);
					all_visible = VM_ALL_VISIBLE(args->rel, blkno,
												 &args->vmbuf);
					pthread_mutex_unlock(args->buf_mutex);
				}
				else
					all_visible = VM_ALL_VISIBLE(args->rel, blkno,
												 &args->vmbuf);
			}

			if (all_visible)
			{
				for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
				{
					ItemId	lp = PageGetItemId(page, offnum);

					if (!ItemIdIsNormal(lp))
						continue;
					Assert(nvis <= MaxHeapTuplesPerPage);
					vis_offnums[nvis++] = offnum;
				}
			}
			else
			{
				pthread_mutex_lock(args->mvcc_mutex);
				for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
				{
					ItemId		 lp;
					HeapTupleData htup;

					lp = PageGetItemId(page, offnum);
					if (!ItemIdIsNormal(lp))
						continue;

					htup.t_data = (HeapTupleHeader) PageGetItem(page, lp);
					htup.t_len = ItemIdGetLength(lp);
					htup.t_tableOid = RelationGetRelid(args->rel);
					ItemPointerSet(&htup.t_self, blkno, offnum);

					if (!HeapTupleSatisfiesVisibility(&htup, args->snapshot, buf))
						continue;

					Assert(nvis <= MaxHeapTuplesPerPage);
					vis_offnums[nvis++] = offnum;
				}
				pthread_mutex_unlock(args->mvcc_mutex);
			}

			/*
			 * Phase 2: qual evaluation and batch accumulation (no mutex held).
			 *
			 * Qualifying tuples are packed into filling_batch->data[] using
			 * batch_add_tuple.  No palloc occurs here: filling_batch comes from
			 * the pre-allocated pool.  If the batch data area fills up (unusual
			 * — it is sized for a full page worth of tuples), we flush it and
			 * claim a fresh node from the pool.
			 *
			 * After all offsets on this page are processed, we flush the batch
			 * if it contains any tuples.  This delivers results to the leader
			 * page-by-page for good latency.
			 */
			for (v = 0; v < nvis; v++)
			{
				ItemId		 lp;
				HeapTupleData htup;

				lp = PageGetItemId(page, vis_offnums[v]);
				htup.t_data = (HeapTupleHeader) PageGetItem(page, lp);
				htup.t_len = ItemIdGetLength(lp);
				htup.t_tableOid = RelationGetRelid(args->rel);
				ItemPointerSet(&htup.t_self, blkno, vis_offnums[v]);

				/* Apply the scan qual if present. */
				if (args->worker_qual != NULL)
				{
					ExecStoreHeapTuple(&htup, args->eval_slot, false);
					ResetExprContext(args->econtext);
					if (!ExecQual(args->worker_qual, args->econtext))
					{
						ExecClearTuple(args->eval_slot);
						continue;
					}
					ExecClearTuple(args->eval_slot);
				}

				/*
				 * Add tuple to the current batch.  If the data area is
				 * unexpectedly full (shouldn't happen for a single page, but
				 * be safe), flush and get a new node.
				 */
				if (!batch_add_tuple(filling_batch, &htup))
				{
					list_enqueue(list, filling_batch);
					filling_batch = pool_pop(args->ptcxt->pool);
					/*
					 * A single tuple must always fit: PT_BATCH_DATA_BYTES is
					 * sized above BLCKSZ so one tuple can never exceed it.
					 */
					if (!batch_add_tuple(filling_batch, &htup))
						elog(ERROR, "parallel thread batch node unexpectedly full");
				}

				if (pg_atomic_read_u32(&list->had_error) != 0)
				{
					pthread_mutex_lock(args->buf_mutex);
					ReleaseBuffer(buf);
					pthread_mutex_unlock(args->buf_mutex);
					goto worker_done;
				}
			}

			/*
			 * Flush the batch after each page to deliver results promptly.
			 * (One sem_post per page, not per tuple.)
			 */
			if (filling_batch->batch_count > 0)
			{
				list_enqueue(list, filling_batch);
				filling_batch = pool_pop(args->ptcxt->pool);
			}

			/* ReleaseBuffer under buf_mutex (same reason as ReadBuffer). */
			pthread_mutex_lock(args->buf_mutex);
			ReleaseBuffer(buf);
			pthread_mutex_unlock(args->buf_mutex);
		}
	}

worker_done:
	/* Release the visibility map buffer under buf_mutex. */
	if (BufferIsValid(args->vmbuf))
	{
		pthread_mutex_lock(args->buf_mutex);
		ReleaseBuffer(args->vmbuf);
		pthread_mutex_unlock(args->buf_mutex);
	}

	/* Signal the leader that this worker is done (flush partial batch first). */
	list_worker_done(list, args->ptcxt->pool, filling_batch,
					 had_error, had_error ? errmsg : NULL);

	/*
	 * Release the per-thread ResourceOwner.  We must save the pointer and set
	 * CurrentResourceOwner to NULL *before* calling ResourceOwnerDelete,
	 * because ResourceOwnerDelete asserts owner != CurrentResourceOwner.
	 */
	if (CurrentResourceOwner != NULL)
	{
		ResourceOwner myOwner = CurrentResourceOwner;

		ResourceOwnerRelease(myOwner,
							 RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
		ResourceOwnerRelease(myOwner,
							 RESOURCE_RELEASE_LOCKS, false, true);
		ResourceOwnerRelease(myOwner,
							 RESOURCE_RELEASE_AFTER_LOCKS, false, true);
		CurrentResourceOwner = NULL;
		ResourceOwnerDelete(myOwner);
	}

	/*
	 * Decrement the global active-thread counter so other backends can use
	 * the freed slot immediately (without waiting for the leader to join).
	 */
	if (ParallelThreadShmem != NULL)
		pg_atomic_fetch_sub_u32(&ParallelThreadShmem->active_thread_workers, 1);

	return NULL;
}


/* ------------------------------------------------------------------
 * Public API
 * ------------------------------------------------------------------ */

/*
 * CreateParallelThreadContext
 *
 * Allocate and initialise a ParallelThreadContext for nworkers thread
 * workers.  The caller must subsequently call LaunchParallelThreadWorkers()
 * to actually start the threads.
 */
ParallelThreadContext *
CreateParallelThreadContext(int nworkers)
{
	ParallelThreadContext *ptcxt;

	if (nworkers < 1)
		nworkers = 1;
	if (nworkers > MAX_PARALLEL_THREAD_WORKERS)
		nworkers = MAX_PARALLEL_THREAD_WORKERS;

	ptcxt = (ParallelThreadContext *) palloc0(sizeof(ParallelThreadContext));
	ptcxt->nworkers = nworkers;
	ptcxt->nworkers_launched = 0;
	ptcxt->workers = (ParallelThreadWorkerState *)
		palloc0(nworkers * sizeof(ParallelThreadWorkerState));
	ptcxt->leader_vmbuf = InvalidBuffer;

	/* Create the pre-allocated FIFO node pool. */
	ptcxt->pool = create_node_pool(nworkers);

	/* Consumer state: no batch node in progress yet. */
	ptcxt->consumer_batch		= NULL;
	ptcxt->consumer_tuple_idx	= 0;
	ptcxt->consumer_data_off	= 0;

	if (pthread_mutex_init(&ptcxt->buf_mutex, NULL) != 0)
		elog(ERROR, "could not initialise parallel thread buffer mutex");

	if (pthread_mutex_init(&ptcxt->mvcc_mutex, NULL) != 0)
		elog(ERROR, "could not initialise parallel thread MVCC mutex");

	return ptcxt;
}

/*
 * LaunchParallelThreadWorkers
 *
 * Initialise the shared block counter, pre-compile per-worker qual machinery,
 * and start each worker thread.  rel and snapshot are passed read-only to
 * each worker.
 *
 * qual is the raw qual list from the scan plan (Plan.qual).  For each worker,
 * LaunchParallelThreadWorkers compiles a private ExprState from qual (in the
 * worker's own memory context) so that workers can evaluate the predicate
 * independently without sharing mutable ExprState fields.  Quals containing
 * SubPlan/InitPlan nodes (which require a parent PlanState) are excluded;
 * the leader's ExecScan path handles filtering for those cases.
 *
 * leader_econtext is the leader's expression-evaluation context.  Its
 * ecxt_param_exec_vals and ecxt_param_list_info pointers are copied into
 * each per-worker ExprContext so that PARAM_EXEC lookups (correlated
 * parameters from an outer query) resolve correctly.  These param arrays
 * are set once by the outer plan before the scan starts and are read-only
 * during the scan, so concurrent reads from multiple threads are safe.
 *
 * The list is created here and stored in ptcxt->list.
 */
void
LaunchParallelThreadWorkers(ParallelThreadContext *ptcxt,
							Relation rel,
							Snapshot snapshot,
							BlockNumber nblocks,
							List *qual,
							ExprContext *leader_econtext)
{
	int			i;
	TupleDesc	tupdesc;
	bool		use_worker_qual;

	Assert(ptcxt != NULL);
	Assert(ptcxt->list == NULL); /* must not launch twice */

	if (nblocks == 0)
		return;

	/* Initialise dynamic block allocation counter (mirrors phs_nallocated). */
	pg_atomic_init_u64(&ptcxt->next_block, 0);
	ptcxt->nblocks = nblocks;
	ptcxt->sentinels_received = 0;

	/* Save relation and snapshot for leader-side block scanning. */
	ptcxt->leader_rel = rel;
	ptcxt->leader_snapshot = snapshot;
	ptcxt->leader_pending_tups = NULL;
	ptcxt->leader_pending_ntups = 0;
	ptcxt->leader_pending_idx = 0;

	ptcxt->list = CreateParallelThreadTupleList(ptcxt->pool);

	/* Relation tuple descriptor needed for per-worker eval slots. */
	tupdesc = RelationGetDescr(rel);

	/*
	 * Determine whether the qual can be safely compiled without a parent
	 * PlanState.  Quals containing SubPlan/InitPlan nodes require a parent;
	 * skip per-worker qual eval for those cases and let the leader's ExecScan
	 * path filter.  Quals with Param nodes are fine: each worker ExprContext
	 * gets the param pointers copied from leader_econtext below.
	 */
	use_worker_qual = qual_safe_for_thread_workers(qual);

	/*
	 * Pre-fetch the first VM page and the first prefetch_limit heap blocks
	 * single-threadedly before launching any workers.  These calls happen
	 * before pthread_create, so no mutex is needed.
	 */
	{
		uint32		heap_blocks_per_vm_page =
			(BLCKSZ - (uint32) MAXALIGN(SizeOfPageHeaderData)) *
			(BITS_PER_BYTE / BITS_PER_HEAPBLOCK);
		BlockNumber prefetch_limit;

		/* Limit to one VM page worth, available temp buffers, and nblocks. */
		prefetch_limit = (BlockNumber) Min((uint64) heap_blocks_per_vm_page,
										  (uint64) (num_temp_buffers - 1));
		prefetch_limit = (BlockNumber) Min((uint64) prefetch_limit,
										  (uint64) nblocks);

		ptcxt->prefetch_limit = prefetch_limit;

		if (nblocks > 0)
		{
			BlockNumber b;

			PrefetchBuffer(rel, VISIBILITYMAP_FORKNUM, 0);
			for (b = 0; b < prefetch_limit; b++)
				PrefetchBuffer(rel, MAIN_FORKNUM, b);
		}
	}

	for (i = 0; i < ptcxt->nworkers; i++)
	{
		ParallelThreadWorkerState *ws = &ptcxt->workers[i];
		ThreadScanArgs *args;
		MemoryContext wctx,
					wectx,
					oldctx;
		int			rc;

		ws->worker_num = i;
		ws->had_error = false;
		ws->ptcxt = ptcxt;
		ws->eval_slot = NULL;

		/*
		 * Pre-create per-worker memory contexts in the leader (single-
		 * threaded) to avoid MemoryContextCreate races later.
		 */
		wctx = AllocSetContextCreate(TopMemoryContext,
									 "ParallelThreadWorker",
									 ALLOCSET_DEFAULT_SIZES);
		wectx = AllocSetContextCreate(wctx,
									  "ParallelThreadWorkerError",
									  ALLOCSET_DEFAULT_SIZES);
		ws->worker_context = wctx;

		/*
		 * Switch to the worker's context to allocate all per-worker resources
		 * there.  This keeps them together for easy cleanup.
		 */
		oldctx = MemoryContextSwitchTo(wctx);

		args = (ThreadScanArgs *) palloc(sizeof(ThreadScanArgs));
		args->ptcxt = ptcxt;
		args->rel = rel;
		args->snapshot = snapshot;
		args->list = ptcxt->list;
		args->worker_num = i;
		args->worker_context = wctx;
		args->worker_error_context = wectx;
		args->mvcc_mutex = &ptcxt->mvcc_mutex;
		args->buf_mutex = &ptcxt->buf_mutex;
		args->vmbuf = InvalidBuffer;

		/*
		 * Pre-allocate the initial batch node for this worker from the pool.
		 * The worker fills this node with qualifying tuples page by page;
		 * once full (or after each page) it is enqueued and a fresh node is
		 * popped from the pool.  No palloc occurs in the hot tuple-copy path.
		 */
		args->filling_batch = pool_pop(ptcxt->pool);

		/*
		 * Compile a private ExprState for this worker from the scan qual.
		 * Doing this in the leader (single-threaded) is safe because catalog
		 * access (fmgr_info etc.) is not thread-safe.  Each worker gets its
		 * own ExprState so they can evaluate the qual concurrently without
		 * sharing mutable ExprState fields (resvalue/resnull).
		 *
		 * Mirrors what each background parallel worker does when it calls
		 * ExecInitSeqScan in its own process.
		 */
		if (use_worker_qual)
		{
			args->worker_qual = ExecInitQual(qual, NULL);
			args->eval_slot = MakeSingleTupleTableSlot(tupdesc,
													   &TTSOpsHeapTuple);
			args->econtext = CreateStandaloneExprContext();

			/*
			 * Copy the leader's parameter pointers so that PARAM_EXEC lookups
			 * (correlated parameters from an outer query) work correctly.
			 * These arrays are populated by the outer plan before the scan
			 * starts and are not modified during the scan, so multiple threads
			 * reading them concurrently is safe without any additional locking.
			 */
			if (leader_econtext != NULL)
			{
				args->econtext->ecxt_param_exec_vals =
					leader_econtext->ecxt_param_exec_vals;
				args->econtext->ecxt_param_list_info =
					leader_econtext->ecxt_param_list_info;
			}

			/* Track slot for cleanup in DestroyParallelThreadContext. */
			ws->eval_slot = args->eval_slot;
		}
		else
		{
			args->worker_qual = NULL;
			args->eval_slot = NULL;
			args->econtext = NULL;
		}

		MemoryContextSwitchTo(oldctx);

		/*
		 * Atomically claim a slot in the global thread-worker limit.
		 * If we'd exceed max_parallel_thread_workers, release the unused
		 * resources and stop launching further workers for this query.
		 */
		if (ParallelThreadShmem != NULL)
		{
			uint32 old = pg_atomic_fetch_add_u32(
				&ParallelThreadShmem->active_thread_workers, 1);

			if (old >= (uint32) max_parallel_thread_workers)
			{
				/* Give the slot back; we won't create this thread. */
				pg_atomic_fetch_sub_u32(
					&ParallelThreadShmem->active_thread_workers, 1);

				ws->launched = false;
				ws->eval_slot = NULL;
				pool_return(ptcxt->pool, args->filling_batch);
				MemoryContextDelete(wctx);
				ws->worker_context = NULL;

				ereport(DEBUG1,
						(errmsg("parallel thread worker %d not started: "
								"global thread worker limit (%d) reached",
								i, max_parallel_thread_workers)));
				break;			/* stop launching workers for this query */
			}
			/* Slot claimed; decrement will happen in thread_scan_worker. */
		}

		rc = pthread_create(&ws->thread, NULL, thread_scan_worker, args);
		if (rc != 0)
		{
			/*
			 * pthread_create failed: release the global slot we just claimed
			 * (no thread will run to do the decrement).
			 */
			if (ParallelThreadShmem != NULL)
				pg_atomic_fetch_sub_u32(
					&ParallelThreadShmem->active_thread_workers, 1);

			ws->launched = false;
			ws->eval_slot = NULL;
			/* Return the unused filling_batch to the pool before deleting ctx. */
			pool_return(ptcxt->pool, args->filling_batch);
			MemoryContextDelete(wctx);
			ws->worker_context = NULL;

			ereport(WARNING,
					(errmsg("could not create parallel thread worker %d: %s",
							i, strerror(rc))));
		}
		else
		{
			ptcxt->list->nwriters++;
			ws->launched = true;
			ptcxt->nworkers_launched++;
		}
	}
}

/*
 * leader_scan_block
 *
 * Called by the leader inside ParallelThreadGetNextTuple when the result
 * list is momentarily empty.  Scans one heap block (claimed atomically from
 * next_block) and stores all visible tuples in ptcxt->leader_pending_tups[].
 * No qual is applied here; the caller's ExecScan path handles filtering.
 *
 * Uses the same approach as thread_scan_worker:
 *   - ReadBuffer under buf_mutex (serialises local-buffer bookkeeping).
 *   - VM fast path: acquire buf_mutex only when vmbuf needs refreshing.
 *   - MVCC check under mvcc_mutex (serialises LWLock access).
 *   - ReleaseBuffer under buf_mutex.
 *   - Phase 2 (tuple copy): no mutex held.
 */
static void
leader_scan_block(ParallelThreadContext *ptcxt, BlockNumber blkno)
{
	Buffer		buf;
	Page		page;
	OffsetNumber maxoff,
				offnum;
	OffsetNumber vis_offnums[MaxHeapTuplesPerPage + 1];
	int			nvis = 0;
	int			v,
				ntups;
	HeapTuple  *tups;
	bool		all_visible;

	/* ReadBuffer under buf_mutex. */
	pthread_mutex_lock(&ptcxt->buf_mutex);
	buf = ReadBuffer(ptcxt->leader_rel, blkno);
	pthread_mutex_unlock(&ptcxt->buf_mutex);

	page = BufferGetPage(buf);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * VM fast path: only lock when the leader's vmbuf needs refreshing
	 * (first call, or when blkno crosses a VM-page boundary).
	 */
	{
		BlockNumber vm_blkno = pt_vm_mapblock(blkno);
		bool		need_vm_refresh =
			!BufferIsValid(ptcxt->leader_vmbuf) ||
			BufferGetBlockNumber(ptcxt->leader_vmbuf) != vm_blkno;

		if (need_vm_refresh)
		{
			pthread_mutex_lock(&ptcxt->buf_mutex);
			all_visible = VM_ALL_VISIBLE(ptcxt->leader_rel, blkno,
										 &ptcxt->leader_vmbuf);
			pthread_mutex_unlock(&ptcxt->buf_mutex);
		}
		else
			all_visible = VM_ALL_VISIBLE(ptcxt->leader_rel, blkno,
										 &ptcxt->leader_vmbuf);
	}

	if (all_visible)
	{
		for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
		{
			ItemId		lp = PageGetItemId(page, offnum);

			if (!ItemIdIsNormal(lp))
				continue;
			Assert(nvis <= MaxHeapTuplesPerPage);
			vis_offnums[nvis++] = offnum;
		}
	}
	else
	{
		/* MVCC check under mvcc_mutex (same as workers). */
		pthread_mutex_lock(&ptcxt->mvcc_mutex);
		for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
		{
			ItemId		lp;
			HeapTupleData htup;

			lp = PageGetItemId(page, offnum);
			if (!ItemIdIsNormal(lp))
				continue;

			htup.t_data = (HeapTupleHeader) PageGetItem(page, lp);
			htup.t_len = ItemIdGetLength(lp);
			htup.t_tableOid = RelationGetRelid(ptcxt->leader_rel);
			ItemPointerSet(&htup.t_self, blkno, offnum);

			if (!HeapTupleSatisfiesVisibility(&htup, ptcxt->leader_snapshot, buf))
				continue;

			Assert(nvis <= MaxHeapTuplesPerPage);
			vis_offnums[nvis++] = offnum;
		}
		pthread_mutex_unlock(&ptcxt->mvcc_mutex);
	}

	if (nvis == 0)
	{
		pthread_mutex_lock(&ptcxt->buf_mutex);
		ReleaseBuffer(buf);
		pthread_mutex_unlock(&ptcxt->buf_mutex);
		return;
	}

	/* Phase 2: copy visible tuples. No qual — ExecScan filters. */
	tups = (HeapTuple *) palloc(nvis * sizeof(HeapTuple));
	ntups = 0;
	for (v = 0; v < nvis; v++)
	{
		ItemId		lp;
		HeapTupleData htup;

		lp = PageGetItemId(page, vis_offnums[v]);
		htup.t_data = (HeapTupleHeader) PageGetItem(page, lp);
		htup.t_len = ItemIdGetLength(lp);

		tups[ntups] = (HeapTuple) palloc(HEAPTUPLESIZE + htup.t_len);
		tups[ntups]->t_len = htup.t_len;
		tups[ntups]->t_tableOid = RelationGetRelid(ptcxt->leader_rel);
		ItemPointerSet(&tups[ntups]->t_self, blkno, vis_offnums[v]);
		tups[ntups]->t_data =
			(HeapTupleHeader) ((char *) tups[ntups] + HEAPTUPLESIZE);
		memcpy(tups[ntups]->t_data, htup.t_data, htup.t_len);
		ntups++;
	}

	/* ReleaseBuffer under buf_mutex. */
	pthread_mutex_lock(&ptcxt->buf_mutex);
	ReleaseBuffer(buf);
	pthread_mutex_unlock(&ptcxt->buf_mutex);

	ptcxt->leader_pending_tups = tups;
	ptcxt->leader_pending_ntups = ntups;
	ptcxt->leader_pending_idx = 0;
}


/*
 * ParallelThreadGetNextTuple
 *
 * Return the next visible tuple from either the worker result list or a page
 * scanned directly by the leader.  Returns a palloc'd HeapTuple, or NULL
 * when all workers are done and the list is drained.  Raises ereport(ERROR)
 * if any worker encountered an error.
 *
 * Batch unpacking
 * ---------------
 * Workers enqueue one PTBatchNode per page containing all qualifying tuples
 * for that page.  This function unpacks one tuple per call from the current
 * consumer_batch node.  When the batch is fully consumed, the node is
 * returned to the pool and the next node is dequeued from the MPSC list.
 *
 * Leader participation
 * -------------------
 * Instead of blocking on items_sem when the list is empty, the leader tries
 * to claim and scan a heap page itself (leader_scan_block).  This keeps the
 * leader productive while workers are scanning other pages.  Results are
 * stored in ptcxt->leader_pending_tups[] and returned one per call.
 */
HeapTuple
ParallelThreadGetNextTuple(ParallelThreadContext *ptcxt)
{
	struct ParallelThreadTupleList *list;
	PTBatchNode *next;
	uint64		next_u64;

	if (ptcxt->list == NULL)
		return NULL;

	/*
	 * consume_next_from_batch: inline helper that pops the next tuple from
	 * ptcxt->consumer_batch and returns it to the pool when the batch is
	 * fully consumed.  Used in two places below.
	 */
#define CONSUME_NEXT_FROM_BATCH(ptcxt_) \
	do { \
		PTBatchNode *_cb = (PTBatchNode *) (ptcxt_)->consumer_batch; \
		HeapTuple	_t; \
		_t = batch_read_tuple(_cb, &(ptcxt_)->consumer_data_off); \
		(ptcxt_)->consumer_tuple_idx++; \
		if ((ptcxt_)->consumer_tuple_idx >= _cb->batch_count) \
		{ \
			/*
			 * Batch fully consumed.  Do NOT return _cb to the pool here:
			 * _cb is still the MPSC list's dummy head and will be returned
			 * as old_head on the next call into the dequeue loop.
			 */ \
			(ptcxt_)->consumer_batch      = NULL; \
			(ptcxt_)->consumer_tuple_idx  = 0; \
			(ptcxt_)->consumer_data_off   = 0; \
		} \
		return _t; \
	} while (0)

	/*
	 * If we have a partially-consumed batch node, return the next tuple from
	 * it before touching the MPSC list.
	 */
	if (ptcxt->consumer_batch != NULL)
		CONSUME_NEXT_FROM_BATCH(ptcxt);

	/*
	 * Return any tuples the leader scanned locally on a previous call.
	 */
	if (ptcxt->leader_pending_ntups > 0)
	{
		HeapTuple	t;

		Assert(ptcxt->leader_pending_idx < ptcxt->leader_pending_ntups);
		t = ptcxt->leader_pending_tups[ptcxt->leader_pending_idx++];
		if (ptcxt->leader_pending_idx >= ptcxt->leader_pending_ntups)
		{
			pfree(ptcxt->leader_pending_tups);
			ptcxt->leader_pending_tups	= NULL;
			ptcxt->leader_pending_ntups = 0;
			ptcxt->leader_pending_idx	= 0;
		}
		return t;
	}

	list = ptcxt->list;

	for (;;)
	{
		/*
		 * Try a non-blocking dequeue first.  If the list is momentarily
		 * empty, have the leader claim and scan a heap page itself rather
		 * than blocking: this keeps the leader productive while workers are
		 * still scanning other pages.
		 */
		if (parallel_leader_participation)
		{
			if (pt_sema_trywait(&list->items_sem) != 0)
			{
				uint64		blkno_u64;

				if (ptcxt->sentinels_received >= list->nwriters)
					return NULL;

				/*
				* Claim the next unclaimed block atomically.  If all blocks have
				* been claimed, fall back to a blocking wait for workers to finish.
				*/
				blkno_u64 = pg_atomic_fetch_add_u64(&ptcxt->next_block, 1);
				if (blkno_u64 < (uint64) ptcxt->nblocks)
				{
					leader_scan_block(ptcxt, (BlockNumber) blkno_u64);
					if (ptcxt->leader_pending_ntups > 0)
					{
						HeapTuple	t;

						t = ptcxt->leader_pending_tups[ptcxt->leader_pending_idx++];
						if (ptcxt->leader_pending_idx >= ptcxt->leader_pending_ntups)
						{
							pfree(ptcxt->leader_pending_tups);
							ptcxt->leader_pending_tups	= NULL;
							ptcxt->leader_pending_ntups = 0;
							ptcxt->leader_pending_idx	= 0;
						}
						return t;
					}
					/* Block was all-invisible; loop to try again. */
					continue;
				}

				/* All blocks claimed; wait for workers to finish. */
				pt_sema_wait(&list->items_sem);
			}
		}
		else
		{
			if (ptcxt->sentinels_received >= list->nwriters)
				return NULL;

			/* All blocks claimed; wait for workers to finish. */
			pt_sema_wait(&list->items_sem);
		}

		/*
		 * Dequeue one batch node from the Vyukov MPSC list.
		 *
		 * Save old_head (the current dummy).  Spin until old_head->next is
		 * non-NULL — the window is a few instructions between a producer's
		 * atomic_exchange(tail) and its subsequent store to prev->next.
		 */
		{
			PTBatchNode *old_head = list->head;

			for (;;)
			{
				pg_read_barrier();
				next_u64 = pg_atomic_read_u64(&old_head->next);
				if (next_u64 != PTR_TO_U64(NULL))
					break;
				sched_yield();
			}

			next = U64_TO_BPTR(next_u64);

			/* Advance head: next becomes the new dummy node. */
			list->head = next;

			/*
			 * Return the OLD dummy to the pool.  It is fully disconnected:
			 * no producer holds a reference to it any more (they only write
			 * to the tail's ->next, which this node already had written).
			 * Returning old_head (not next) is the Vyukov-correct behaviour;
			 * returning next while it is still the head would allow a worker
			 * to pop and reuse it while we're about to spin on its ->next.
			 */
			pool_return(ptcxt->pool, old_head);
		}

		if (next->is_sentinel)
		{
			/*
			 * Worker-done sentinel.  next is now the MPSC list dummy; do NOT
			 * return it to the pool here.  It will be returned as old_head on
			 * the next dequeue (or by DestroyParallelThreadTupleList).
			 */
			ptcxt->sentinels_received++;

			if (ptcxt->sentinels_received == list->nwriters)
			{
				if (pg_atomic_read_u32(&list->had_error) != 0)
				{
					char		errmsg_copy[PARALLEL_THREAD_ERRMSG_LEN];

					pg_read_barrier();
					strlcpy(errmsg_copy, list->errmsg, sizeof(errmsg_copy));
					ereport(ERROR,
							(errmsg("error in parallel thread worker: %s",
									errmsg_copy)));
				}
				return NULL;
			}

			/* More workers still running; loop for next item. */
			continue;
		}

		/*
		 * Batch node with one or more tuples.  Install it as the current
		 * consumer batch and return the first tuple via the shared helper.
		 */
		Assert(next->batch_count > 0);
		ptcxt->consumer_batch		= next;
		ptcxt->consumer_tuple_idx	= 0;
		ptcxt->consumer_data_off	= 0;
		CONSUME_NEXT_FROM_BATCH(ptcxt);
	}
}

#undef CONSUME_NEXT_FROM_BATCH

/*
 * WaitForParallelThreadWorkers
 *
 * Join all launched thread workers, then check for any errors they reported.
 */
void
WaitForParallelThreadWorkers(ParallelThreadContext *ptcxt)
{
	int			i;

	for (i = 0; i < ptcxt->nworkers; i++)
	{
		ParallelThreadWorkerState *ws = &ptcxt->workers[i];

		if (ws->launched)
		{
			pthread_join(ws->thread, NULL);
			ws->launched = false;
		}
	}

	/* Re-raise any error that was reported via the list. */
	if (ptcxt->list != NULL &&
		pg_atomic_read_u32(&ptcxt->list->had_error) != 0)
		ereport(ERROR,
				(errmsg("error in parallel thread worker: %s",
						ptcxt->list->errmsg)));
}

/*
 * DestroyParallelThreadContext
 *
 * Wait for all workers to finish (if not already done), release the result
 * queue, per-worker memory contexts, and the context itself.
 */
void
DestroyParallelThreadContext(ParallelThreadContext *ptcxt)
{
	int			i;

	if (ptcxt == NULL)
		return;

	/* Make sure all threads have finished. */
	for (i = 0; i < ptcxt->nworkers; i++)
	{
		if (ptcxt->workers[i].launched)
		{
			pthread_join(ptcxt->workers[i].thread, NULL);
			ptcxt->workers[i].launched = false;
		}

		/*
		 * Drop the per-worker eval slot before deleting the memory context.
		 * ExecDropSingleTupleTableSlot decrements the TupleDesc refcount via
		 * UnpinTupleDesc; skipping this would leak the refcount.
		 */
		if (ptcxt->workers[i].eval_slot != NULL)
		{
			ExecDropSingleTupleTableSlot(ptcxt->workers[i].eval_slot);
			ptcxt->workers[i].eval_slot = NULL;
		}

		/* Delete the per-worker memory context (allocated by leader). */
		if (ptcxt->workers[i].worker_context != NULL)
		{
			MemoryContextDelete(ptcxt->workers[i].worker_context);
			ptcxt->workers[i].worker_context = NULL;
		}
	}

	/*
	 * The current consumer_batch (if any) is still the MPSC list's dummy head.
	 * DestroyParallelThreadTupleList will return list->head to the pool, which
	 * covers consumer_batch.  Clear the pointer so no stale reference remains.
	 */
	if (ptcxt->consumer_batch != NULL)
	{
		ptcxt->consumer_batch		= NULL;
		ptcxt->consumer_tuple_idx	= 0;
		ptcxt->consumer_data_off	= 0;
	}

	if (ptcxt->list != NULL)
	{
		DestroyParallelThreadTupleList(ptcxt->list, ptcxt->pool);
		ptcxt->list = NULL;
	}

	/* Destroy the node pool (after the list, which returned its dummy node). */
	if (ptcxt->pool != NULL)
	{
		destroy_node_pool(ptcxt->pool);
		ptcxt->pool = NULL;
	}

	pthread_mutex_destroy(&ptcxt->buf_mutex);
	pthread_mutex_destroy(&ptcxt->mvcc_mutex);

	/* Release the leader's visibility map buffer if pinned. */
	if (BufferIsValid(ptcxt->leader_vmbuf))
	{
		ReleaseBuffer(ptcxt->leader_vmbuf);
		ptcxt->leader_vmbuf = InvalidBuffer;
	}

	/* Free any leader-scanned tuples that were not yet consumed. */
	if (ptcxt->leader_pending_tups != NULL)
	{
		int			j;

		for (j = ptcxt->leader_pending_idx; j < ptcxt->leader_pending_ntups; j++)
			pfree(ptcxt->leader_pending_tups[j]);
		pfree(ptcxt->leader_pending_tups);
	}

	pfree(ptcxt->workers);
	pfree(ptcxt);
}
