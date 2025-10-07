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
	uint32		aquired_sz; /* used for exclsive locks to remember how many locks were acquired */
							/* in case active_locks changed while acquiring */
} AdaptiveLWLockHandle;

static AdaptiveLWLockHandle held_adaptive_lwlocks[MAX_ADAPTIVE_LWLOCKS];
static int num_held_adaptive_lwlocks = 0;

/* Forward declarations */
static int AdaptiveSelectSlot(uint32 active_locks);
static void AdaptiveUpdateStats(AdaptiveLWLock *lock, LWLockMode mode, bool contended, int slot_index);
static void AdaptiveAdjustLocks(AdaptiveLWLock *lock);

/*
 * AdaptiveLWLockInitialize - initialize an adaptive LWLock
 */
void
AdaptiveLWLockInitialize(AdaptiveLWLock *lock, int tranche_id, int init_size, bool adaptive)
{
	int			i;
	
	if (init_size == -1)
		init_size = ADAPTIVE_LWLOCK_MAX_LOCKS;

	Assert(init_size > 0 && init_size <= ADAPTIVE_LWLOCK_MAX_LOCKS);

	lock->adaptive = adaptive;
	pg_atomic_init_u32(&lock->active_locks, init_size);

	/* Initialize all LWLock slots */
	for (i = 0; i < ADAPTIVE_LWLOCK_MAX_LOCKS; i++)
	{
		LWLockInitialize(&lock->locks[i].lock, tranche_id);
		
		lock->locks[i].shared_acquisitions = 0;
		lock->locks[i].exclusive_acquisitions = 0;
		lock->locks[i].contentions = 0;
		lock->locks[i].pressure = 0.0f;
		
		pg_write_barrier();
	}
}

/*
 * AdaptiveSelectSlot - select a LWLock slot
 */
static int
AdaptiveSelectSlot(uint32 active_locks)
{
	return MyProcPid % active_locks;
}

/*
 * AdaptiveLWLockAcquire - acquire an adaptive LWLock
 */
bool
AdaptiveLWLockAcquire(AdaptiveLWLock *lock, LWLockMode mode)
{
	uint32		active_locks;
	int			slot_index;
	uint32		i;
	bool		result = true;

	Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

	/* Track held locks */
	if (num_held_adaptive_lwlocks >= MAX_ADAPTIVE_LWLOCKS)
		elog(ERROR, "too many adaptive LWLocks taken");
retry_adaptive_acquire:
	/* Read active_locks - this is our snapshot for this operation */
	active_locks = pg_atomic_read_u32(&lock->active_locks);
	
	pg_read_barrier();

	if (mode == LW_EXCLUSIVE)
	{
		/*
		 * For exclusive lock, acquire all active LWLocks in order.
		 * This prevents deadlock since all exclusive acquirers use the same order.
		 */
		for (i = 0; i < active_locks; i++)
		{
			if (!LWLockAcquire(&lock->locks[i].lock, LW_EXCLUSIVE))
			{
				result = false;
			}
			
			if (i == 0)
			{
				/* check that active_locks didn't change */
				uint32		new_active;
				new_active = pg_atomic_read_u32(&lock->active_locks);
				
				pg_read_barrier();

				if (new_active != active_locks)
				{
					result = true; /* reset result for retry */
					LWLockRelease(&lock->locks[0].lock);
					goto retry_adaptive_acquire;
				}
			}
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
		uint32		new_active;
		
		/* Shared lock - acquire one randomly selected LWLock */
		slot_index = AdaptiveSelectSlot(active_locks);
		
		if (!LWLockAcquire(&lock->locks[slot_index].lock, LW_SHARED))
			result = false;
			
		/* check that active_locks didn't change */
		new_active = pg_atomic_read_u32(&lock->active_locks);
		
		pg_read_barrier();

		if (new_active != active_locks)
		{
			result = true; /* reset result for retry */
			LWLockRelease(&lock->locks[slot_index].lock);
			goto retry_adaptive_acquire;
		}
	}

	/* Record that we hold this lock */
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].lock = lock;
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].mode = mode;
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].slot_index = slot_index;
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].aquired_sz = active_locks;
	num_held_adaptive_lwlocks++;

	/* Update stats */
	AdaptiveUpdateStats(lock, mode, !result, slot_index);

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
retry_cond_acquire:
	/* Read active_locks */
	active_locks = pg_atomic_read_u32(&lock->active_locks);
	
	pg_read_barrier();

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
			
			if (i == 0)
			{
				/* check that active_locks didn't change */
				uint32		new_active;
				new_active = pg_atomic_read_u32(&lock->active_locks);
				
				pg_read_barrier();

				if (new_active != active_locks)
				{
					/* Release and fail */
					LWLockRelease(&lock->locks[0].lock);
					goto retry_cond_acquire;
				}
			}
		}

		slot_index = -1;
		
		/* Adjust locks if needed */
		AdaptiveAdjustLocks(lock);
	}
	else
	{
		uint32		new_active;
		/* Try to acquire one LWLock */
		slot_index = AdaptiveSelectSlot(active_locks);
		
		if (!LWLockConditionalAcquire(&lock->locks[slot_index].lock, LW_SHARED))
			return false;
			
		/* check that active_locks didn't change */
		new_active = pg_atomic_read_u32(&lock->active_locks);
		
		pg_read_barrier();

		if (new_active != active_locks)
		{
			/* Release and fail */
			LWLockRelease(&lock->locks[slot_index].lock);
			goto retry_cond_acquire;
		}
	}

	/* Record that we hold this lock */
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].lock = lock;
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].mode = mode;
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].slot_index = slot_index;
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].aquired_sz = active_locks;
	num_held_adaptive_lwlocks++;

	AdaptiveUpdateStats(lock, mode, false, slot_index);

	return true;
}

/*
 * AdaptiveLWLockRelease - release an adaptive LWLock
 */
void
AdaptiveLWLockRelease(AdaptiveLWLock *lock)
{
	uint32			i;
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
		active_locks = held_adaptive_lwlocks[i].aquired_sz;

		for (i = 0; i < active_locks; i++)
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

static int some_counter = 0;

/*
 * AdaptiveUpdateStats - update statistics for adaptive behavior
 */
static void
AdaptiveUpdateStats(AdaptiveLWLock *lock, LWLockMode mode, bool contended, int slot_index)
{
	uint32			i;
	uint32		active_counters;
	
	if (!lock->adaptive)
		return;
	
	active_counters = pg_atomic_read_u32(&lock->active_locks);

	if (slot_index == -1)
		slot_index = 0; /* exclusive lock doesn't have a specific slot */

	if (mode == LW_EXCLUSIVE)
	{
		/* Update stats on all active counters for exclusive lock */
		for (i = 0; i < active_counters; i++)
		{
			lock->locks[i].exclusive_acquisitions += 1;
			if (contended)
				lock->locks[i].contentions += 1;
		}
	}
	else
	{
		/* Update stats only on the acquired counter for shared lock */
		lock->locks[slot_index].shared_acquisitions += 1;
		if (contended)
			lock->locks[slot_index].contentions += 1;
	}

	if (some_counter++ % 1000 == 0 && false)
	{
		for (int k = 0; k < active_counters; k++)
		{
			elog(WARNING, "%p: lock slot %d shared_acquisitions: %d exclusive_acquisitions: %d contentions: %d presure: %f", lock, k,
					lock->locks[k].shared_acquisitions,
					lock->locks[k].exclusive_acquisitions,
					lock->locks[k].contentions,
					lock->locks[k].pressure);
		}
	}
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
	uint32		exclusive_acq = 0;
	uint32		shared_acq = 0;
	uint32		max_contentions = 0;
	uint64		total_acq;
	float		pressure = 0;
	uint32		new_active;
	uint32		i;
	
	if (!lock->adaptive)
		return;

	current_active = pg_atomic_read_u32(&lock->active_locks);

	/* Aggregate statistics from all active counters */
	for (i = 0; i <  current_active; i++)
	{
		uint32		excl = lock->locks[i].exclusive_acquisitions;
		uint32		shr = lock->locks[i].shared_acquisitions;
		uint32		cont = lock->locks[i].contentions;
		float		press = lock->locks[i].pressure;
		float 		contention_ratio;
		float 		alpha;
		uint64		sum;

		
		exclusive_acq = Max(excl, exclusive_acq);
		shared_acq += shr;
		max_contentions = Max(cont, max_contentions);
		pressure += press;
		sum = (uint64)excl + (uint64)shr;
		contention_ratio = (float)cont / (float)(sum);
		contention_ratio = Min(Max(0.0f, contention_ratio), 1.0f);
		alpha = 0.05f + 0.45f * contention_ratio;
		
		if (sum >= 1000)
		{	
			lock->locks[i].pressure = lock->locks[i].pressure * (1 - alpha) + ((float) excl / (float)(sum)) * alpha;

			lock->locks[i].exclusive_acquisitions = 0;
			lock->locks[i].shared_acquisitions = 0;
			lock->locks[i].contentions = 0;
		}
	}

	total_acq = (uint64)exclusive_acq + (uint64)shared_acq;
	pressure = pressure / (float) current_active;

	/* Need enough samples before adjusting */
	if (total_acq < 1000)
		return;

	new_active = current_active;
	
	if (pressure <= 0.2f)
	{
		/* Mostly shared locks - increase active locks to reduce contention */
		if (current_active < ADAPTIVE_LWLOCK_MAX_LOCKS)
			new_active = current_active * 2;
	}
	else if (pressure >= 0.7f)
	{
		/* Mostly exclusive locks - decrease active locks to improve exclusive performance */
		if (current_active > 1)
			new_active = current_active / 2;
	}

	/* Update if changed */
	if (new_active != current_active)
	{
		/*
		 * We hold all counters exclusively, so it's safe to change active_counters.
		 */
		pg_atomic_write_u32(&lock->active_locks, new_active);

		/*
		* Reset all per-counter stats to start fresh with new configuration.
		*/
		for (i = 0; i < ADAPTIVE_LWLOCK_MAX_LOCKS; i++)
		{
			lock->locks[i].exclusive_acquisitions = 0;
			lock->locks[i].shared_acquisitions = 0;
			lock->locks[i].contentions = 0;
		}
		
		pg_write_barrier();
	}
}

/*
 * AdaptiveLWLockReleaseAll - release all held adaptive LWLocks
 */
void
AdaptiveLWLockReleaseAll(void)
{
	while(num_held_adaptive_lwlocks > 0)
	{
		HOLD_INTERRUPTS();		/* match the upcoming RESUME_INTERRUPTS */

		AdaptiveLWLockRelease(held_adaptive_lwlocks[num_held_adaptive_lwlocks - 1].lock);
	}
}
	