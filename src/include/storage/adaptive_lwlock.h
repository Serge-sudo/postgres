/*-------------------------------------------------------------------------
 *
 * adaptive_lwlock.h
 *	  Adaptive lightweight lock manager
 *
 * An adaptive LWLock implementation that reduces cache line contention for
 * shared locks by using an array of padded atomic counters. For shared lock
 * acquisition, a backend randomly selects one counter to increment. For
 * exclusive locks, all counters must be acquired. The lock adaptively
 * adjusts the number of active counters based on workload statistics.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/adaptive_lwlock.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ADAPTIVE_LWLOCK_H
#define ADAPTIVE_LWLOCK_H

#ifdef FRONTEND
#error "adaptive_lwlock.h may not be included from frontend code"
#endif

#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/spin.h"
#include "storage/proclist_types.h"

/* Maximum number of counter slots for adaptive lock */
#define ADAPTIVE_LWLOCK_MAX_COUNTERS	8

/*
 * Per-counter structure with atomic counter, wait queue, and statistics.
 * Each counter has its own spinlock-protected wait queue and tracks its own
 * statistics to eliminate cache line contention from both lock operations
 * and statistics updates.
 * 
 * Note: This structure will span multiple cache lines. What matters is that
 * counters in the array don't share cache lines.
 */
typedef struct AdaptiveLWLockCounter
{
	pg_atomic_uint32 count;					/* Atomic counter for lock acquisition */
	slock_t		mutex;						/* Spinlock protecting wait queue */
	proclist_head waiters;					/* Queue of waiting backends */
	pg_atomic_uint32 exclusive_acquisitions; /* Per-counter exclusive lock count */
	pg_atomic_uint32 shared_acquisitions;	/* Per-counter shared lock count */
	pg_atomic_uint32 contentions;			/* Per-counter contention count */
	char		pad[PG_CACHE_LINE_SIZE];	/* Padding to prevent false sharing */
} AdaptiveLWLockCounter;

/*
 * Adaptive LWLock structure.
 *
 * The active_counters field determines how many counter slots are currently
 * in use. This can be adjusted by exclusive lock holders based on statistics.
 * Reading this field doesn't require a lock, but changes must be made while
 * holding an exclusive lock.
 */
typedef struct AdaptiveLWLock
{
	uint16		tranche;		/* tranche ID */
	pg_atomic_uint32 active_counters;	/* number of active counter slots */
	AdaptiveLWLockCounter counters[ADAPTIVE_LWLOCK_MAX_COUNTERS];
#ifdef LOCK_DEBUG
	pg_atomic_uint32 nwaiters;	/* number of waiters */
	struct PGPROC *owner;		/* last exclusive owner of the lock */
#endif
} AdaptiveLWLock;

/*
 * Padded adaptive LWLock - aligned to cache line boundary.
 * Note: Due to the counter array, this will span multiple cache lines.
 */
typedef union AdaptiveLWLockPadded
{
	AdaptiveLWLock lock;
	char		pad[LWLOCK_PADDED_SIZE];
} AdaptiveLWLockPadded;

/* Function declarations */
extern void AdaptiveLWLockInitialize(AdaptiveLWLock *lock, int tranche_id);
extern bool AdaptiveLWLockAcquire(AdaptiveLWLock *lock, LWLockMode mode);
extern bool AdaptiveLWLockConditionalAcquire(AdaptiveLWLock *lock, LWLockMode mode);
extern void AdaptiveLWLockRelease(AdaptiveLWLock *lock);
extern bool AdaptiveLWLockHeldByMe(AdaptiveLWLock *lock);
extern bool AdaptiveLWLockHeldByMeInMode(AdaptiveLWLock *lock, LWLockMode mode);

#endif							/* ADAPTIVE_LWLOCK_H */
