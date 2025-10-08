/*-------------------------------------------------------------------------
 *
 * adaptive_lwlock.c
 *	  Adaptive lightweight lock manager implementation
 *
 * This module implements an adaptive LWLock that uses an array of atomic
 * counters to reduce cache line contention for shared locks. The key insight
 * is that with standard LWLocks, every shared lock acquisition causes cache
 * line invalidation as the shared counter is incremented atomically.
 *
 * The adaptive approach:
 * - Shared lock: Randomly select one of N atomic counters and increment it
 * - Exclusive lock: Acquire all N counters (set to special value)
 * - Adaptive switching: Based on statistics, exclusive lock holders can
 *   reduce the number of active counters to improve write performance
 *
 * The active_counters field is read without locking but only modified by
 * exclusive lock holders. Readers use a retry loop that validates the
 * active_counters value hasn't changed during acquisition.
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
#include "pg_trace.h"
#include "port/pg_bitutils.h"
#include "storage/adaptive_lwlock.h"
#include "storage/proc.h"
#include "storage/proclist.h"
#include "storage/spin.h"
#include "utils/memutils.h"

/* Special value indicating exclusive lock on a counter */
#define ADAPTIVE_EXCLUSIVE_FLAG		((uint32) 1 << 31)

/* Helper macros */
#define ADAPTIVE_COUNTER_IS_FREE(val)		(((val) & ADAPTIVE_EXCLUSIVE_FLAG) == 0 && (val) == 0)
#define ADAPTIVE_COUNTER_IS_EXCLUSIVE(val)	(((val) & ADAPTIVE_EXCLUSIVE_FLAG) != 0)

/*
 * Backend-local state for tracking held adaptive locks.
 * Similar to regular LWLocks, we need to track which locks we hold
 * for cleanup during error recovery.
 */
#define MAX_ADAPTIVE_LWLOCKS	50

typedef struct AdaptiveLWLockHandle
{
	AdaptiveLWLock *lock;
	LWLockMode	mode;
	int			slot_index;		/* which counter slot we acquired (-1 for exclusive) */
} AdaptiveLWLockHandle;

static AdaptiveLWLockHandle held_adaptive_lwlocks[MAX_ADAPTIVE_LWLOCKS];
static int num_held_adaptive_lwlocks = 0;

/* Forward declarations */
static bool AdaptiveLWLockAttemptLock(AdaptiveLWLock *lock, LWLockMode mode, int *slot_index);
static void AdaptiveLWLockQueueSelf(AdaptiveLWLock *lock, LWLockMode mode, int slot_index);
static void AdaptiveLWLockDequeueSelf(AdaptiveLWLock *lock, int slot_index);
static void AdaptiveLWLockWakeup(AdaptiveLWLock *lock, int slot_index);
static int AdaptiveSelectRandomSlot(uint32 active_counters);
static void AdaptiveUpdateStats(AdaptiveLWLock *lock, int slot_index, LWLockMode mode, bool contended);
static void AdaptiveAdjustCounters(AdaptiveLWLock *lock);

/*
 * AdaptiveLWLockInitialize - initialize an adaptive LWLock
 */
void
AdaptiveLWLockInitialize(AdaptiveLWLock *lock, int tranche_id)
{
	int			i;

	lock->tranche = tranche_id;
	pg_atomic_init_u32(&lock->active_counters, ADAPTIVE_LWLOCK_MAX_COUNTERS);

	/* Initialize all counter slots with wait queues and stats */
	for (i = 0; i < ADAPTIVE_LWLOCK_MAX_COUNTERS; i++)
	{
		pg_atomic_init_u32(&lock->counters[i].count, 0);
		SpinLockInit(&lock->counters[i].mutex);
		proclist_init(&lock->counters[i].waiters);
		pg_atomic_init_u32(&lock->counters[i].exclusive_acquisitions, 0);
		pg_atomic_init_u32(&lock->counters[i].shared_acquisitions, 0);
		pg_atomic_init_u32(&lock->counters[i].contentions, 0);
	}

#ifdef LOCK_DEBUG
	pg_atomic_init_u32(&lock->nwaiters, 0);
	lock->owner = NULL;
#endif
}

/*
 * AdaptiveSelectRandomSlot - select a random counter slot
 *
 * We use a simple approach based on the backend's process number
 * combined with a rotating counter to distribute load.
 */
static int
AdaptiveSelectRandomSlot(uint32 active_counters)
{
	static uint32 counter = 0;
	uint32		val;

	/*
	 * Combine process number with a counter for better distribution.
	 * We don't need cryptographic randomness here.
	 */
	val = (uint32) MyProcPid + (counter++);
	return val % active_counters;
}

/*
 * AdaptiveLWLockAttemptLock - try to acquire the adaptive lock
 *
 * For shared locks: Try to increment one randomly selected counter.
 * For exclusive locks: Try to acquire all active counters.
 *
 * Returns true if we need to wait (lock not available).
 * Returns false if lock was acquired successfully.
 *
 * On success, *slot_index is set to the acquired slot for shared locks,
 * or -1 for exclusive locks.
 */
static bool
AdaptiveLWLockAttemptLock(AdaptiveLWLock *lock, LWLockMode mode, int *slot_index)
{
	uint32		active_counters;
	int			i;

	Assert(mode == LW_EXCLUSIVE || mode == LW_SHARED);

	/* Read active_counters - this is our snapshot for this attempt */
	active_counters = pg_atomic_read_u32(&lock->active_counters);

	if (mode == LW_EXCLUSIVE)
	{
		/*
		 * For exclusive lock, we need to acquire all active counters.
		 * We do this by trying to CAS each counter from 0 to EXCLUSIVE_FLAG.
		 */
		for (i = 0; i < (int) active_counters; i++)
		{
			uint32		expected = 0;
			uint32		desired = ADAPTIVE_EXCLUSIVE_FLAG;

			if (!pg_atomic_compare_exchange_u32(&lock->counters[i].count,
												&expected, desired))
			{
				/* Failed to acquire this counter - roll back what we got */
				int			j;

				for (j = 0; j < i; j++)
					pg_atomic_write_u32(&lock->counters[j].count, 0);

				return true;	/* need to wait */
			}
		}

		*slot_index = -1;		/* exclusive lock doesn't use a specific slot */

#ifdef LOCK_DEBUG
		lock->owner = MyProc;
#endif

		/*
		 * After acquiring exclusive lock, check if we should adjust the
		 * number of active counters based on statistics.
		 */
		AdaptiveAdjustCounters(lock);

		return false;			/* got the lock */
	}
	else
	{
		/* Shared lock - try to increment one randomly selected counter */
		int			slot;
		uint32		old_val;
		uint32		new_val;

		slot = AdaptiveSelectRandomSlot(active_counters);
		*slot_index = slot;

		old_val = pg_atomic_read_u32(&lock->counters[slot].count);

		while (true)
		{
			/* Check if counter has exclusive lock */
			if (ADAPTIVE_COUNTER_IS_EXCLUSIVE(old_val))
				return true;	/* need to wait */

			new_val = old_val + 1;

			/*
			 * Check for overflow - very unlikely but we should handle it.
			 * Leave some room below the exclusive flag.
			 */
			if (new_val >= ADAPTIVE_EXCLUSIVE_FLAG)
				return true;	/* too many shared locks, need to wait */

			if (pg_atomic_compare_exchange_u32(&lock->counters[slot].count,
											   &old_val, new_val))
			{
				/*
				 * Successfully incremented. Now verify that active_counters
				 * hasn't changed, which would mean we might have picked a
				 * counter that's no longer active.
				 */
				uint32		current_active;

				pg_read_barrier();	/* ensure we see latest active_counters */

				current_active = pg_atomic_read_u32(&lock->active_counters);

				if (current_active == active_counters)
				{
					/* All good, we have the lock */
					return false;
				}
				else
				{
					/*
					 * Active counters changed - release and retry.
					 * This is rare but can happen during adaptive switching.
					 */
					pg_atomic_fetch_sub_u32(&lock->counters[slot].count, 1);
					return true;	/* need to retry */
				}
			}

			/* CAS failed, old_val now contains the updated value, retry */
		}
	}

	pg_unreachable();
}

/*
 * AdaptiveLWLockQueueSelf - add ourselves to the wait queue
 */
static void
AdaptiveLWLockQueueSelf(AdaptiveLWLock *lock, LWLockMode mode, int slot_index)
{
	if (MyProc == NULL)
		elog(PANIC, "cannot wait without a PGPROC structure");

	if (MyProc->lwWaiting != LW_WS_NOT_WAITING)
		elog(PANIC, "queueing for lock while waiting on another one");

	SpinLockAcquire(&lock->counters[slot_index].mutex);

	MyProc->lwWaiting = LW_WS_WAITING;
	MyProc->lwWaitMode = mode;

	proclist_push_tail(&lock->counters[slot_index].waiters, MyProcNumber, lwWaitLink);

	SpinLockRelease(&lock->counters[slot_index].mutex);

#ifdef LOCK_DEBUG
	pg_atomic_fetch_add_u32(&lock->nwaiters, 1);
#endif
}

/*
 * AdaptiveLWLockDequeueSelf - remove ourselves from wait queue
 */
static void
AdaptiveLWLockDequeueSelf(AdaptiveLWLock *lock, int slot_index)
{
	bool		on_waitlist;

	SpinLockAcquire(&lock->counters[slot_index].mutex);

	/*
	 * Remove ourselves from the waitlist, unless we've already been removed.
	 * The removal happens with the wait list lock held, so there's no race in
	 * this check.
	 */
	on_waitlist = MyProc->lwWaiting == LW_WS_WAITING;
	if (on_waitlist)
		proclist_delete(&lock->counters[slot_index].waiters, MyProcNumber, lwWaitLink);

	SpinLockRelease(&lock->counters[slot_index].mutex);

	/* Clear waiting state after releasing lock, nice for debugging */
	if (on_waitlist)
		MyProc->lwWaiting = LW_WS_NOT_WAITING;

#ifdef LOCK_DEBUG
	if (on_waitlist)
		pg_atomic_fetch_sub_u32(&lock->nwaiters, 1);
#endif
}

/*
 * AdaptiveLWLockWakeup - wake up waiters
 */
static void
AdaptiveLWLockWakeup(AdaptiveLWLock *lock, int slot_index)
{
	proclist_head wakeup;
	proclist_mutable_iter iter;
	bool		wokeup_somebody = false;

	proclist_init(&wakeup);

	SpinLockAcquire(&lock->counters[slot_index].mutex);

	proclist_foreach_modify(iter, &lock->counters[slot_index].waiters, lwWaitLink)
	{
		PGPROC	   *waiter = GetPGProcByNumber(iter.cur);

		if (wokeup_somebody && waiter->lwWaitMode == LW_EXCLUSIVE)
			break;

		proclist_delete(&lock->counters[slot_index].waiters, iter.cur, lwWaitLink);
		proclist_push_tail(&wakeup, iter.cur, lwWaitLink);

		wokeup_somebody = true;

		Assert(waiter->lwWaiting == LW_WS_WAITING);
		waiter->lwWaiting = LW_WS_PENDING_WAKEUP;

		if (waiter->lwWaitMode == LW_EXCLUSIVE)
			break;
	}

	SpinLockRelease(&lock->counters[slot_index].mutex);

	/* Wake up the waiters */
	proclist_foreach_modify(iter, &wakeup, lwWaitLink)
	{
		PGPROC	   *waiter = GetPGProcByNumber(iter.cur);

		proclist_delete(&wakeup, iter.cur, lwWaitLink);

		pg_write_barrier();
		waiter->lwWaiting = LW_WS_NOT_WAITING;
		PGSemaphoreUnlock(waiter->sem);
	}
}

/*
 * AdaptiveUpdateStats - update per-counter statistics
 */
static void
AdaptiveUpdateStats(AdaptiveLWLock *lock, int slot_index, LWLockMode mode, bool contended)
{
	int			i;
	uint32		active_counters = pg_atomic_read_u32(&lock->active_counters);

	if (mode == LW_EXCLUSIVE)
	{
		/* Update stats on all active counters for exclusive lock */
		for (i = 0; i < (int) active_counters; i++)
		{
			pg_atomic_fetch_add_u32(&lock->counters[i].exclusive_acquisitions, 1);
			if (contended)
				pg_atomic_fetch_add_u32(&lock->counters[i].contentions, 1);
		}
	}
	else
	{
		/* Update stats only on the acquired counter for shared lock */
		pg_atomic_fetch_add_u32(&lock->counters[slot_index].shared_acquisitions, 1);
		if (contended)
			pg_atomic_fetch_add_u32(&lock->counters[slot_index].contentions, 1);
	}
}

/*
 * AdaptiveAdjustCounters - adjust number of active counters based on stats
 *
 * This is called by exclusive lock holders to potentially change the number
 * of active counters based on workload statistics.
 *
 * Strategy:
 * - Sum all per-counter stats to get totals
 * - For contention, use minimum across counters (most contended counter)
 * - If exclusive lock rate is high and contention low, reduce counters
 * - If contention is high, increase counters
 */
static void
AdaptiveAdjustCounters(AdaptiveLWLock *lock)
{
	uint32		current_active;
	uint32		exclusive_acq = 0;
	uint32		shared_acq = 0;
	uint32		min_contentions = UINT32_MAX;
	uint32		total_acq;
	uint32		new_active;
	int			i;

	current_active = pg_atomic_read_u32(&lock->active_counters);

	/* Aggregate statistics from all active counters */
	for (i = 0; i < (int) current_active; i++)
	{
		uint32		excl = pg_atomic_read_u32(&lock->counters[i].exclusive_acquisitions);
		uint32		shr = pg_atomic_read_u32(&lock->counters[i].shared_acquisitions);
		uint32		cont = pg_atomic_read_u32(&lock->counters[i].contentions);

		exclusive_acq += excl;
		shared_acq += shr;
		
		/* Use minimum contention (most contended counter determines behavior) */
		if (cont < min_contentions)
			min_contentions = cont;
	}

	total_acq = exclusive_acq + shared_acq;

	/* Need enough samples before adjusting */
	if (total_acq < 1000)
		return;

	new_active = current_active;

	/*
	 * If exclusive lock rate is > 10% and contention is low (< 1%),
	 * reduce counters to speed up exclusive acquisitions.
	 */
	if (exclusive_acq * 10 > total_acq && min_contentions * 100 < total_acq)
	{
		if (current_active > 1)
			new_active = current_active / 2;
	}
	/*
	 * If contention is high (> 5%), increase counters to reduce cache bouncing.
	 */
	else if (min_contentions * 20 > total_acq)
	{
		if (current_active < ADAPTIVE_LWLOCK_MAX_COUNTERS)
			new_active = current_active * 2;
	}

	/* Update if changed */
	if (new_active != current_active)
	{
		/*
		 * We hold all counters exclusively, so it's safe to change active_counters.
		 */
		pg_atomic_write_u32(&lock->active_counters, new_active);

		/*
		 * Reset all per-counter stats to start fresh with new configuration.
		 */
		for (i = 0; i < ADAPTIVE_LWLOCK_MAX_COUNTERS; i++)
		{
			pg_atomic_write_u32(&lock->counters[i].exclusive_acquisitions, 0);
			pg_atomic_write_u32(&lock->counters[i].shared_acquisitions, 0);
			pg_atomic_write_u32(&lock->counters[i].contentions, 0);
		}
	}
}

/*
 * AdaptiveLWLockAcquire - acquire an adaptive LWLock
 */
bool
AdaptiveLWLockAcquire(AdaptiveLWLock *lock, LWLockMode mode)
{
	bool		mustwait;
	int			slot_index;
	int			queue_slot;
	bool		result = true;
	int			extraWaits = 0;

	Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

	HOLD_INTERRUPTS();

	/* Track held locks */
	if (num_held_adaptive_lwlocks >= MAX_ADAPTIVE_LWLOCKS)
		elog(ERROR, "too many adaptive LWLocks taken");

	/* Try to acquire the lock */
	mustwait = AdaptiveLWLockAttemptLock(lock, mode, &slot_index);

	if (mustwait)
	{
		result = false;

		/* For exclusive, queue on counter 0; for shared, queue on the selected slot */
		queue_slot = (mode == LW_EXCLUSIVE) ? 0 : slot_index;
		AdaptiveLWLockQueueSelf(lock, mode, queue_slot);

		/*
		 * Try to acquire again after queueing. This is critical to avoid a
		 * race condition where the lock is released after our first attempt
		 * but before we queue ourselves. If we get the lock now, we need to
		 * dequeue ourselves.
		 */
		mustwait = AdaptiveLWLockAttemptLock(lock, mode, &slot_index);

		if (!mustwait)
		{
			/* Got the lock on second try, undo queueing */
			AdaptiveLWLockDequeueSelf(lock, queue_slot);
		}
		else
		{
			/* Loop until we acquire the lock */
			for (;;)
			{
				PGSemaphoreLock(MyProc->sem);

				/* Try to acquire again after waking up */
				if (!AdaptiveLWLockAttemptLock(lock, mode, &slot_index))
				{
					AdaptiveLWLockDequeueSelf(lock, queue_slot);
					break;
				}

				extraWaits++;
			}
		}

		/* Update contention stats */
		AdaptiveUpdateStats(lock, slot_index, mode, true);
	}
	else
	{
		/* Got lock immediately */
		AdaptiveUpdateStats(lock, slot_index, mode, false);
	}

	/* Record that we hold this lock */
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].lock = lock;
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].mode = mode;
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].slot_index = slot_index;
	num_held_adaptive_lwlocks++;

	return result;
}

/*
 * AdaptiveLWLockConditionalAcquire - try to acquire without waiting
 */
bool
AdaptiveLWLockConditionalAcquire(AdaptiveLWLock *lock, LWLockMode mode)
{
	bool		mustwait;
	int			slot_index;

	Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

	HOLD_INTERRUPTS();

	mustwait = AdaptiveLWLockAttemptLock(lock, mode, &slot_index);

	if (mustwait)
	{
		RESUME_INTERRUPTS();
		return false;
	}

	/* Track held locks */
	if (num_held_adaptive_lwlocks >= MAX_ADAPTIVE_LWLOCKS)
		elog(ERROR, "too many adaptive LWLocks taken");

	held_adaptive_lwlocks[num_held_adaptive_lwlocks].lock = lock;
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].mode = mode;
	held_adaptive_lwlocks[num_held_adaptive_lwlocks].slot_index = slot_index;
	num_held_adaptive_lwlocks++;

	AdaptiveUpdateStats(lock, slot_index, mode, false);

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

#ifdef LOCK_DEBUG
	if (mode == LW_EXCLUSIVE)
		lock->owner = NULL;
#endif

	/* Release the lock */
	if (mode == LW_EXCLUSIVE)
	{
		/* Release all counters that were active when we acquired */
		uint32		active = pg_atomic_read_u32(&lock->active_counters);
		int			j;

		for (j = 0; j < (int) active; j++)
		{
			pg_atomic_write_u32(&lock->counters[j].count, 0);
			
			/* Wake up waiters on this counter if any */
			if (!proclist_is_empty(&lock->counters[j].waiters))
				AdaptiveLWLockWakeup(lock, j);
		}
		
		/* Try to adjust counters based on stats */
		AdaptiveAdjustCounters(lock);
	}
	else
	{
		/* Decrement the counter we incremented */
		pg_atomic_fetch_sub_u32(&lock->counters[slot_index].count, 1);
		
		/* Wake up any exclusive waiters on this counter's queue */
		if (!proclist_is_empty(&lock->counters[slot_index].waiters))
			AdaptiveLWLockWakeup(lock, slot_index);
	}

	RESUME_INTERRUPTS();
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
