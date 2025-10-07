/*-------------------------------------------------------------------------
 *
 * adaptive_lwlock.c
 *	  Adaptive lightweight lock manager implementation
 *
 * This module implements an adaptive LWLock that uses an array of standard
 * LWLocks to reduce cache line contention for shared locks. The key insight
 * is that with standard LWLocks, every shared lock acquisition causes cache
 * line invalidation. By distributing shared locks across multiple LWLocks,
 * we reduce this contention.
 *
 * The adaptive approach:
 * - Shared lock: Randomly select one of N LWLocks and acquire it in shared mode
 * - Exclusive lock: Acquire all N LWLocks in exclusive mode
 * - Adaptive switching: Based on statistics, exclusive lock holders can
 *   reduce the number of active LWLocks to improve write performance
 *
 * The active_locks field is read without locking but only modified by
 * exclusive lock holders (who hold all locks).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/lmgr/adaptive_lwlock.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/adaptive_lwlock.h"
#include "storage/lwlock.h"

/*
 * Backend-local state for tracking held adaptive locks.
 */
#define MAX_ADAPTIVE_LWLOCKS	50

typedef struct AdaptiveLWLockHandle
{
	AdaptiveLWLock *lock;
	LWLockMode	mode;
	int			slot_index;		/* which lock slot we acquired (-1 for exclusive) */
} AdaptiveLWLockHandle;

static AdaptiveLWLockHandle held_adaptive_lwlocks[MAX_ADAPTIVE_LWLOCKS];
static int num_held_adaptive_lwlocks = 0;

/* Forward declarations */
static int AdaptiveSelectRandomSlot(uint32 active_locks);
static void AdaptiveUpdateStats(AdaptiveLWLock *lock, LWLockMode mode, bool contended);
static void AdaptiveAdjustLocks(AdaptiveLWLock *lock);

/*
 * AdaptiveLWLockInitialize - initialize an adaptive LWLock
 */
void
AdaptiveLWLockInitialize(AdaptiveLWLock *lock, int tranche_id)
{
	int			i;

	lock->tranche = tranche_id;
	pg_atomic_init_u32(&lock->active_locks, ADAPTIVE_LWLOCK_MAX_LOCKS);

	/* Initialize all LWLock slots */
	for (i = 0; i < ADAPTIVE_LWLOCK_MAX_LOCKS; i++)
		LWLockInitialize(&lock->locks[i].lock, tranche_id);

	/* Stats pointer should be set separately if needed */
	lock->stats = NULL;
}

/*
 * AdaptiveSelectRandomSlot - select a random LWLock slot
 */
static int
AdaptiveSelectRandomSlot(uint32 active_locks)
{
	static uint32 counter = 0;
	uint32		val;

	/*
	 * Combine process number with a counter for better distribution.
	 */
	val = (uint32) MyProcPid + (counter++);
	return val % active_locks;
}

/*
 * AdaptiveLWLockAcquire - acquire an adaptive LWLock
 */
bool
AdaptiveLWLockAcquire(AdaptiveLWLock *lock, LWLockMode mode)
{
	uint32		active_locks;
	int			slot_index;
	bool		result = true;
	int			i;

	Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

	/* Track held locks */
	if (num_held_adaptive_lwlocks >= MAX_ADAPTIVE_LWLOCKS)
		elog(ERROR, "too many adaptive LWLocks taken");

	/* Read active_locks - this is our snapshot for this operation */
	active_locks = pg_atomic_read_u32(&lock->active_locks);

	if (mode == LW_EXCLUSIVE)
	{
		/*
		 * For exclusive lock, acquire all active LWLocks in order.
		 * This prevents deadlock since all exclusive acquirers use the same order.
		 */
		for (i = 0; i < (int) active_locks; i++)
		{
			if (!LWLockAcquire(&lock->locks[i].lock, LW_EXCLUSIVE))
				result = false;
		}

		slot_index = -1;		/* exclusive lock uses all slots */

		/*
		 * After acquiring exclusive lock, check if we should adjust the
		 * number of active locks based on statistics.
		 */
		AdaptiveAdjustLocks(lock);
	}
	else
	{
		/* Shared lock - acquire one randomly selected LWLock */
		slot_index = AdaptiveSelectRandomSlot(active_locks);
		
		if (!LWLockAcquire(&lock->locks[slot_index].lock, LW_SHARED))
			result = false;
	}

	/* Record that we hold this lock */
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].lock = lock;
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].mode = mode;
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].slot_index = slot_index;
	num_held_adaptive_lwlocks++;

	/* Update stats */
	AdaptiveUpdateStats(lock, mode, !result);

	return result;
}

/*
 * AdaptiveLWLockConditionalAcquire - try to acquire without waiting
 */
bool
AdaptiveLWLockConditionalAcquire(AdaptiveLWLock *lock, LWLockMode mode)
{
	uint32		active_locks;
	int			slot_index;
	int			i;

	Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

	/* Track held locks */
	if (num_held_adaptive_lwlocks >= MAX_ADAPTIVE_LWLOCKS)
		elog(ERROR, "too many adaptive LWLocks taken");

	/* Read active_locks */
	active_locks = pg_atomic_read_u32(&lock->active_locks);

	if (mode == LW_EXCLUSIVE)
	{
		/* Try to acquire all active LWLocks */
		for (i = 0; i < (int) active_locks; i++)
		{
			if (!LWLockConditionalAcquire(&lock->locks[i].lock, LW_EXCLUSIVE))
			{
				/* Failed - release what we got */
				while (--i >= 0)
					LWLockRelease(&lock->locks[i].lock);
				return false;
			}
		}

		slot_index = -1;
		
		/* Adjust locks if needed */
		AdaptiveAdjustLocks(lock);
	}
	else
	{
		/* Try to acquire one LWLock */
		slot_index = AdaptiveSelectRandomSlot(active_locks);
		
		if (!LWLockConditionalAcquire(&lock->locks[slot_index].lock, LW_SHARED))
			return false;
	}

	/* Record that we hold this lock */
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].lock = lock;
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].mode = mode;
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].slot_index = slot_index;
	num_held_adaptive_lwlocks++;

	AdaptiveUpdateStats(lock, mode, false);

	return true;
}

/*
 * AdaptiveLWLockRelease - release an adaptive LWLock
 */
void
AdaptiveLWLockRelease(AdaptiveLWLock *lock)
{
	int			i;
	LWLockMode	mode;
	int			slot_index;
	uint32		active_locks;

	/* Find this lock in our held locks array */
	for (i = num_held_adaptive_lwlocks - 1; i >= 0; i--)
	{
		if (held_adaptive_lwlocks[i].lock == lock)
			break;
	}

	if (i < 0)
		elog(ERROR, "lock is not held");

	mode = held_adaptive_lwlocks[i].mode;
	slot_index = held_adaptive_lwlocks[i].slot_index;

	/* Remove from held locks array */
	for (; i < num_held_adaptive_lwlocks - 1; i++)
		held_adaptive_lwlocks[i] = held_adaptive_lwlocks[i + 1];
	num_held_adaptive_lwlocks--;

	/* Release the lock(s) */
	if (mode == LW_EXCLUSIVE)
	{
		/* Release all LWLocks that were active when we acquired */
		active_locks = pg_atomic_read_u32(&lock->active_locks);
		
		for (i = 0; i < (int) active_locks; i++)
			LWLockRelease(&lock->locks[i].lock);
	}
	else
	{
		/* Release the one LWLock we acquired */
		LWLockRelease(&lock->locks[slot_index].lock);
	}
}

/*
 * AdaptiveLWLockHeldByMe - check if we hold this lock
 */
bool
AdaptiveLWLockHeldByMe(AdaptiveLWLock *lock)
{
	int			i;

	for (i = 0; i < num_held_adaptive_lwlocks; i++)
	{
		if (held_adaptive_lwlocks[i].lock == lock)
			return true;
	}

	return false;
}

/*
 * AdaptiveLWLockHeldByMeInMode - check if we hold this lock in given mode
 */
bool
AdaptiveLWLockHeldByMeInMode(AdaptiveLWLock *lock, LWLockMode mode)
{
	int			i;

	for (i = 0; i < num_held_adaptive_lwlocks; i++)
	{
		if (held_adaptive_lwlocks[i].lock == lock &&
			held_adaptive_lwlocks[i].mode == mode)
			return true;
	}

	return false;
}

/*
 * AdaptiveUpdateStats - update statistics for adaptive behavior
 */
static void
AdaptiveUpdateStats(AdaptiveLWLock *lock, LWLockMode mode, bool contended)
{
	if (lock->stats == NULL)
		return;

	if (mode == LW_EXCLUSIVE)
		pg_atomic_fetch_add_u32(&lock->stats->exclusive_acquisitions, 1);
	else
		pg_atomic_fetch_add_u32(&lock->stats->shared_acquisitions, 1);

	if (contended)
		pg_atomic_fetch_add_u32(&lock->stats->contentions, 1);
}

/*
 * AdaptiveAdjustLocks - adjust number of active locks based on stats
 *
 * This is called by exclusive lock holders to potentially reduce the number
 * of active locks when write contention is high.
 *
 * Strategy:
 * - If we see high exclusive lock rate (> 10% of total) and low contention,
 *   reduce locks to improve exclusive lock performance
 * - If we see high contention, increase locks to reduce cache bouncing
 */
static void
AdaptiveAdjustLocks(AdaptiveLWLock *lock)
{
	uint32		current_active;
	uint32		exclusive_acq;
	uint32		shared_acq;
	uint32		contentions;
	uint32		total_acq;
	uint32		new_active;

	if (lock->stats == NULL)
		return;

	current_active = pg_atomic_read_u32(&lock->active_locks);
	exclusive_acq = pg_atomic_read_u32(&lock->stats->exclusive_acquisitions);
	shared_acq = pg_atomic_read_u32(&lock->stats->shared_acquisitions);
	contentions = pg_atomic_read_u32(&lock->stats->contentions);

	total_acq = exclusive_acq + shared_acq;

	/* Need enough samples before adjusting */
	if (total_acq < 1000)
		return;

	new_active = current_active;

	/*
	 * If exclusive lock rate is > 10% and contention is low (< 1%),
	 * reduce locks to speed up exclusive acquisitions.
	 */
	if (exclusive_acq * 10 > total_acq && contentions * 100 < total_acq)
	{
		if (current_active > 1)
			new_active = current_active / 2;
	}
	/*
	 * If contention is high (> 5%), increase locks to reduce cache bouncing.
	 */
	else if (contentions * 20 > total_acq)
	{
		if (current_active < ADAPTIVE_LWLOCK_MAX_LOCKS)
			new_active = current_active * 2;
	}

	/* Update if changed */
	if (new_active != current_active)
	{
		/*
		 * We hold all locks exclusively, so it's safe to change active_locks.
		 */
		pg_atomic_write_u32(&lock->active_locks, new_active);

		/*
		 * Reset stats to start fresh with new configuration.
		 */
		pg_atomic_write_u32(&lock->stats->exclusive_acquisitions, 0);
		pg_atomic_write_u32(&lock->stats->shared_acquisitions, 0);
		pg_atomic_write_u32(&lock->stats->contentions, 0);
	}
}
