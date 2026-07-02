/*-------------------------------------------------------------------------
 *
 * dist_deadlock.h
 *	  Distributed deadlock detection for PostgreSQL shard groups
 *
 * This module implements distributed deadlock detection across multiple
 * PostgreSQL nodes in a shard group. It collects lock graphs from each
 * node, merges them together, and checks for cycles in the merged graph.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 * src/include/storage/dist_deadlock.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DIST_DEADLOCK_H
#define DIST_DEADLOCK_H

#include "storage/lock.h"
#include "storage/proc.h"

/*
 * Represents a node in the distributed wait-for graph.
 * Each node is identified by a combination of cluster name and backend PID.
 */
typedef struct DistWaitNode
{
	char		cluster_name[NAMEDATALEN];	/* Cluster name from pg_foreign_server.srvname (empty for local) */
	int			backendPid;		/* Backend process ID */
	TransactionId xid;			/* Transaction ID */
} DistWaitNode;

/*
 * Represents an edge in the distributed wait-for graph.
 * An edge from A to B means A is waiting for B.
 */
typedef struct DistWaitEdge
{
	DistWaitNode waiter;		/* Process waiting for lock */
	DistWaitNode blocker;		/* Process holding the lock */
	Oid			lockOid;		/* OID of the locked object */
	LOCKMODE	lockMode;		/* Type of lock being waited for */
} DistWaitEdge;

/*
 * Container for a distributed lock graph from a single node
 */
typedef struct DistLockGraph
{
	char		cluster_name[NAMEDATALEN];	/* Cluster name this graph is from */
	int			numEdges;		/* Number of edges in the graph */
	DistWaitEdge *edges;		/* Array of edges */
} DistLockGraph;

/*
 * Result of distributed deadlock detection
 */
typedef struct DistDeadlockInfo
{
	bool		deadlockFound;	/* True if a deadlock cycle was detected */
	int			cycleLength;	/* Number of nodes in the cycle */
	DistWaitNode *cycleNodes;	/* Array of nodes in the cycle */
	DistWaitEdge *cycleEdges;	/* Array of edges in the cycle */
} DistDeadlockInfo;

/*
 * Main functions for distributed deadlock detection
 */

/* Initialize distributed deadlock detection */
extern void InitDistDeadlockDetection(void);

/* Collect local lock graph from current node */
extern DistLockGraph *CollectLocalLockGraph(void);

/* Query remote node for its lock graph */
extern DistLockGraph *QueryRemoteLockGraph(const char *cluster_name);

/* Merge multiple lock graphs into one */
extern DistLockGraph *MergeLockGraphs(DistLockGraph **graphs, int numGraphs);

/* Check for cycles in the merged graph */
extern DistDeadlockInfo *DetectDistributedDeadlock(DistLockGraph *mergedGraph);

/* Get all shard group members for distributed detection */
extern List *GetShardGroupMembersForDeadlockDetection(Oid sgid);

/* Helper function to get cluster name from server OID */
extern char *GetClusterNameFromServerOid(Oid serverOid);

/* Perform global distributed deadlock detection across all shard groups */
extern DistDeadlockInfo *PerformGlobalDistributedDeadlockCheck(void);

/* Free distributed deadlock detection data structures */
extern void FreeDistLockGraph(DistLockGraph *graph);
extern void FreeDistDeadlockInfo(DistDeadlockInfo *info);

#endif							/* DIST_DEADLOCK_H */
