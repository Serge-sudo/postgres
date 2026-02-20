/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitSeqScan			creates and initializes a seqscan node.
 *		ExecEndSeqScan			releases any storage allocated.
 *		ExecReScanSeqScan		rescans the relation
 *
 *		ExecSeqScanEstimate		estimates DSM space needed for parallel scan
 *		ExecSeqScanInitializeDSM initialize DSM for parallel scan
 *		ExecSeqScanReInitializeDSM reinitialize DSM for fresh parallel scan
 *		ExecSeqScanInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/parallelthread.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "executor/executor.h"
#include "executor/nodeSeqscan.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

static TupleTableSlot *SeqNext(SeqScanState *node);
static void SeqScanLaunchThreadWorkers(SeqScanState *node, Snapshot snapshot);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
SeqNext(SeqScanState *node)
{
	TableScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	/*
	 * If thread-based parallel workers are active for this temp table scan,
	 * retrieve the next tuple from the inter-thread result queue instead of
	 * doing a regular heap scan.  Only forward-direction scans are supported
	 * with thread workers; a backward scan falls through to the normal path.
	 */
	if (node->ptcxt != NULL && ScanDirectionIsForward(direction))
	{
		HeapTuple	tuple = ParallelThreadGetNextTuple(node->ptcxt);

		if (tuple == NULL)
			return ExecClearTuple(slot);

		/*
		 * ExecForceStoreHeapTuple handles any slot type, including the
		 * TTSOpsBufferHeapTuple slots that SeqScan normally uses.
		 * shouldFree = true lets the slot own the palloc'd tuple.
		 */
		ExecForceStoreHeapTuple(tuple, slot, true);
		return slot;
	}

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = table_beginscan(node->ss.ss_currentRelation,
								   estate->es_snapshot,
								   0, NULL);
		node->ss.ss_currentScanDesc = scandesc;
	}

	/*
	 * get the next tuple from the table
	 */
	if (table_scan_getnextslot(scandesc, direction, slot))
		return slot;
	return NULL;
}

/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
SeqRecheck(SeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecSeqScan(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) SeqNext,
					(ExecScanRecheckMtd) SeqRecheck);
}


/*
 * SeqScanLaunchThreadWorkers
 *
 * Launch thread-based parallel workers for a temporary table sequential scan.
 * Stores the resulting ParallelThreadContext in node->ptcxt, or leaves it NULL
 * if no workers could be started.  Used by both ExecInitSeqScan (initial
 * launch) and ExecReScanSeqScan (restart after a rescan).
 */
static void
SeqScanLaunchThreadWorkers(SeqScanState *node, Snapshot snapshot)
{
	BlockNumber			nblocks;
	int					nworkers;
	ParallelThreadContext *ptcxt;
	List			   *qual;

	Assert(node->ptcxt == NULL);

	nblocks = RelationGetNumberOfBlocks(node->ss.ss_currentRelation);
	if (nblocks == 0)
		return;

	/*
	 * Derive the worker count from the actual block count, mirroring the
	 * planner's create_localparallel_seqscan logic.  We cannot call
	 * compute_parallel_worker() here because it requires a RelOptInfo; we
	 * instead replicate the min-cap, which is also what the planner path
	 * does when rel->rel_parallel_workers is unset and the table is large
	 * enough for the maximum worker count.
	 */
	nworkers = node->ptworkers_planned;
	ptcxt = CreateParallelThreadContext(nworkers);

	/* Raw qual list from the plan node, for per-worker ExprState compilation. */
	qual = node->ss.ps.plan->qual;

	LaunchParallelThreadWorkers(ptcxt,
								node->ss.ss_currentRelation,
								snapshot,
								nblocks,
								qual,
								node->ss.ps.ps_ExprContext);

	if (ptcxt->nworkers_launched > 0)
		node->ptcxt = ptcxt;
	else
		DestroyParallelThreadContext(ptcxt);
}


/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
SeqScanState *
ExecInitSeqScan(SeqScan *node, EState *estate, int eflags)
{
	SeqScanState *scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(SeqScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = ExecSeqScan;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * open the scan relation
	 */
	scanstate->ss.ss_currentRelation =
		ExecOpenScanRelation(estate,
							 node->scan.scanrelid,
							 eflags);

	/* and create slot with the appropriate rowtype */
	ExecInitScanTupleSlot(estate, &scanstate->ss,
						  RelationGetDescr(scanstate->ss.ss_currentRelation),
						  table_slot_callbacks(scanstate->ss.ss_currentRelation));

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);

	/*
	 * If this is a scan of a temporary table, enable_parallel_temp_table is
	 * on, and we have a worker budget, launch thread-based parallel workers
	 * now.  Thread workers run inside this process and can directly access
	 * the session's local buffer pool.  We do not launch workers when:
	 *   - We are already inside a parallel worker (avoid nesting).
	 *   - The relation has no blocks (nothing to scan in parallel).
	 *   - An EXPLAIN-only plan run (EXEC_FLAG_EXPLAIN_ONLY).
	 *
	 * We compute ptworkers_planned regardless of EXEC_FLAG_EXPLAIN_ONLY so
	 * that plain EXPLAIN can display "Workers Planned: N".
	 */
	scanstate->ptcxt = NULL;
	scanstate->ptworkers_planned = 0;

	if (enable_parallel_temp_table &&
		RelationUsesLocalBuffers(scanstate->ss.ss_currentRelation) &&
		max_parallel_workers_per_gather > 0 &&
		!IsParallelWorker() && !IsParallelThreadWorker())
	{
		int			rel_parallel_workers;

		/*
		 * Determine the worker count the same way the planner does:
		 * honour the parallel_workers reloption first; otherwise cap at
		 * max_parallel_workers_per_gather and MAX_PARALLEL_THREAD_WORKERS.
		 * The planner's cost_localparallel_seqscan uses compute_parallel_worker()
		 * which also reads the reloption first, so this stays consistent.
		 */
		rel_parallel_workers =
			RelationGetParallelWorkers(scanstate->ss.ss_currentRelation, -1);

		if (rel_parallel_workers != -1)
			scanstate->ptworkers_planned = Min(rel_parallel_workers,
											   MAX_PARALLEL_THREAD_WORKERS);
		else
			scanstate->ptworkers_planned = Min(max_parallel_workers_per_gather,
											   MAX_PARALLEL_THREAD_WORKERS);

		if (scanstate->ptworkers_planned > 0 && !(eflags & EXEC_FLAG_EXPLAIN_ONLY))
			SeqScanLaunchThreadWorkers(scanstate, estate->es_snapshot);
	}

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndSeqScan(SeqScanState *node)
{
	TableScanDesc scanDesc;

	/*
	 * If thread-based parallel workers are running, wait for them to finish
	 * and clean up the thread context.  WaitForParallelThreadWorkers() also
	 * re-raises any error reported by a worker.
	 */
	if (node->ptcxt != NULL)
	{
		WaitForParallelThreadWorkers(node->ptcxt);
		DestroyParallelThreadContext(node->ptcxt);
		node->ptcxt = NULL;
	}

	/*
	 * get information from node
	 */
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * close heap scan
	 */
	if (scanDesc != NULL)
		table_endscan(scanDesc);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanSeqScan(SeqScanState *node)
{
	EState	   *estate = node->ss.ps.state;
	TableScanDesc scan;

	/*
	 * If thread-based parallel workers are active, drain the result queue and
	 * wait for all threads to finish, then restart them so the rescan reads
	 * from the beginning of the relation again.
	 */
	if (node->ptcxt != NULL)
	{
		HeapTuple	tup;

		/* Drain the queue so workers don't block trying to enqueue. */
		while ((tup = ParallelThreadGetNextTuple(node->ptcxt)) != NULL)
			pfree(tup);

		WaitForParallelThreadWorkers(node->ptcxt);
		DestroyParallelThreadContext(node->ptcxt);
		node->ptcxt = NULL;

		/* Re-launch workers from the start of the relation. */
		SeqScanLaunchThreadWorkers(node, estate->es_snapshot);
	}

	scan = node->ss.ss_currentScanDesc;

	if (scan != NULL)
		table_rescan(scan,		/* scan desc */
					 NULL);		/* new scan keys */

	ExecScanReScan((ScanState *) node);
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecSeqScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanEstimate(SeqScanState *node,
					ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	node->pscan_len = table_parallelscan_estimate(node->ss.ss_currentRelation,
												  estate->es_snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeDSM
 *
 *		Set up a parallel heap scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeDSM(SeqScanState *node,
						 ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;
	ParallelTableScanDesc pscan;

	pscan = shm_toc_allocate(pcxt->toc, node->pscan_len);
	table_parallelscan_initialize(node->ss.ss_currentRelation,
								  pscan,
								  estate->es_snapshot);
	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pscan);
	node->ss.ss_currentScanDesc =
		table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanReInitializeDSM(SeqScanState *node,
						   ParallelContext *pcxt)
{
	ParallelTableScanDesc pscan;

	pscan = node->ss.ss_currentScanDesc->rs_parallel;
	table_parallelscan_reinitialize(node->ss.ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeWorker(SeqScanState *node,
							ParallelWorkerContext *pwcxt)
{
	ParallelTableScanDesc pscan;

	pscan = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
	node->ss.ss_currentScanDesc =
		table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}
