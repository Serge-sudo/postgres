/*-------------------------------------------------------------------------
 *
 * adaptive_lwlock.h
 *	  Adaptive lightweight lock manager
 *
 * An adaptive LWLock implementation that reduces cache line contention for
 * shared locks by using an array of padded LWLocks. For shared lock
 * acquisition, a backend randomly selects one LWLock to acquire. For
 * exclusive locks, all LWLocks must be acquired. The lock adaptively
 * adjusts the number of active LWLocks based on workload statistics.
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

/* Maximum number of LWLock slots for adaptive lock */
#define ADAPTIVE_LWLOCK_MAX_LOCKS	8

/*
 * Statistics for adaptive mode switching.
 * These are kept separate from the lock structure to avoid cache bouncing
 * during normal lock operations.
 */
typedef struct AdaptiveLWLockCounters
{
	LWLock 			lock;
	uint32 			exclusive_acquisitions;
	uint32 			shared_acquisitions;
	uint32			contentions;
	float			pressure;
	char			pad[PG_CACHE_LINE_SIZE - sizeof(LWLock) - 3 * sizeof(uint32) - sizeof(float)];
} AdaptiveLWLockCounters;


/*
 * Adaptive LWLock structure.
 *
 * The active_locks field determines how many LWLock slots are currently
 * in use. This can be adjusted by exclusive lock holders based on statistics.
 * Reading this field doesn't require a lock, but changes must be made while
 * holding all exclusive locks.
 */
typedef struct AdaptiveLWLock
{
	bool				adaptive;			/* is this lock adaptive? */
	pg_atomic_uint32 	active_locks;	/* number of active LWLock slots */
	
	char			pad[PG_CACHE_LINE_SIZE - sizeof(bool) - sizeof(pg_atomic_uint32)]; 		
	AdaptiveLWLockCounters locks[ADAPTIVE_LWLOCK_MAX_LOCKS];	/* array of padded LWLocks */
} AdaptiveLWLock;

/* Function declarations */
extern void AdaptiveLWLockInitialize(AdaptiveLWLock *lock, int tranche_id, int init_size, bool adaptive);
extern bool AdaptiveLWLockAcquire(AdaptiveLWLock *lock, LWLockMode mode);
extern bool AdaptiveLWLockConditionalAcquire(AdaptiveLWLock *lock, LWLockMode mode);
extern void AdaptiveLWLockRelease(AdaptiveLWLock *lock);
extern bool AdaptiveLWLockHeldByMe(AdaptiveLWLock *lock);
extern bool AdaptiveLWLockHeldByMeInMode(AdaptiveLWLock *lock, LWLockMode mode);
extern void AdaptiveLWLockReleaseAll(void);

#endif							/* ADAPTIVE_LWLOCK_H */