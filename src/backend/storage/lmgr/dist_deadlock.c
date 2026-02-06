/*-------------------------------------------------------------------------
 *
 * dist_deadlock.c
 *	  Distributed deadlock detection for PostgreSQL shard groups
 *
 * This module implements distributed deadlock detection across multiple
 * PostgreSQL nodes in a shard group. It collects lock graphs from each
 * node, merges them together, and checks for cycles in the merged graph.
 *
 * The algorithm works as follows:
 * 1. Collect the local lock graph (wait-for graph) from the current node
 * 2. Query all other nodes in the shard group for their lock graphs
 * 3. Merge all graphs into a single global wait-for graph
 * 4. Use depth-first search (DFS) to detect cycles in the merged graph
 * 5. If a cycle is found, return information about the deadlock
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/lmgr/dist_deadlock.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_shardgroups.h"
#include "catalog/pg_shardmembers.h"
#include "commands/shardgroupcmds.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/dist_deadlock.h"
#include "storage/lock.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

/* Maximum number of nodes in the distributed graph */
#define MAX_DIST_GRAPH_NODES 10000

/* Working memory for cycle detection */
static DistWaitNode *visitedNodes = NULL;
static int maxVisitedNodes = 0;

/* Hash table for node lookup during cycle detection */
typedef struct NodeHashEntry
{
	DistWaitNode node;			/* The key */
	int			index;			/* Index in the edges array */
} NodeHashEntry;

/*
 * InitDistDeadlockDetection
 *		Initialize distributed deadlock detection structures
 */
void
InitDistDeadlockDetection(void)
{
	MemoryContext oldcxt;

	/* Make sure allocations are permanent */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	/* Allocate space for visited nodes during cycle detection */
	maxVisitedNodes = MAX_DIST_GRAPH_NODES;
	visitedNodes = (DistWaitNode *) palloc(maxVisitedNodes * sizeof(DistWaitNode));

	MemoryContextSwitchTo(oldcxt);
}

/*
 * CollectLocalLockGraph
 *		Collect the wait-for graph from the current (local) node
 *
 * This function examines the local lock table and extracts all wait-for
 * relationships, building a graph that can be merged with graphs from
 * other nodes.
 *
 * IMPORTANT: This function is called from DeadLockCheck(), which already
 * holds all lock partition locks. Therefore, this function must NOT attempt
 * to acquire any lock partition locks itself.
 */
DistLockGraph *
CollectLocalLockGraph(void)
{
	DistLockGraph *graph;
	DistWaitEdge *edges;
	int			numEdges = 0;
	int			maxEdges = 1;	/* Initial allocation */
	PGPROC	   *proc;
	int			i;

	/* Allocate initial graph structure */
	graph = (DistLockGraph *) palloc(sizeof(DistLockGraph));
	edges = (DistWaitEdge *) palloc(maxEdges * sizeof(DistWaitEdge));

	strncpy(graph->cluster_name, cluster_name, NAMEDATALEN);
	graph->numEdges = 0;
	graph->edges = edges;

	/*
	 * Scan through all backends and find those that are waiting for locks.
	 * For each waiting backend, identify the blockers and create edges.
	 *
	 * NOTE: The caller (DeadLockCheck) already holds all lock partition locks,
	 * so we must NOT try to acquire them here.
	 */
	for (i = 0; i < ProcGlobal->allProcCount; i++)
	{
		LOCK	   *waitLock;
		dlist_iter	proclock_iter;

		proc = &ProcGlobal->allProcs[i];

		/* Skip backends that aren't waiting */
		if (proc->waitLock == NULL)
			continue;

		waitLock = proc->waitLock;

		/*
		 * This backend is waiting. Now find all the backends that hold
		 * conflicting locks on the same object.
		 *
		 * We don't need to acquire the lock partition lock here because
		 * the caller (DeadLockCheck) already holds all partition locks.
		 */

		/* Iterate through all holders of this lock */
		dlist_foreach(proclock_iter, &waitLock->procLocks)
		{
			PROCLOCK   *proclock = dlist_container(PROCLOCK, lockLink, proclock_iter.cur);
			PGPROC	   *holder;

			/* Get the backend holding the lock */
			holder = proclock->tag.myProc;

			/*
			 * Check if this holder's lock conflicts with what the waiter
			 * wants. We check if the holder has any conflicting lock mode.
			 */
			if (holder != proc)
			{
				LockMethod	lockMethodTable = GetLocksMethodTable(waitLock);
				int			conflictMask = lockMethodTable->conflictTab[proc->waitLockMode];

				/* Check if there's a conflict */
				if (proclock->holdMask & conflictMask)
				{
					/* Found a blocker - create an edge */
					if (numEdges >= maxEdges)
					{
						maxEdges *= 2;
						edges = (DistWaitEdge *) repalloc(edges,
														  maxEdges * sizeof(DistWaitEdge));
						graph->edges = edges;
					}

					/* Fill in the edge information */
					strncpy(edges[numEdges].waiter.cluster_name, cluster_name, NAMEDATALEN);
					edges[numEdges].waiter.backendPid = proc->pid;
					edges[numEdges].waiter.xid = proc->xid;

					strncpy(edges[numEdges].blocker.cluster_name, cluster_name, NAMEDATALEN);
					edges[numEdges].blocker.backendPid = holder->pid;
					edges[numEdges].blocker.xid = holder->xid;

					edges[numEdges].lockOid = waitLock->tag.locktag_field1;
					edges[numEdges].lockMode = proc->waitLockMode;
					
					elog(DEBUG2, "Local edge: waiter(%s,%d) -> blocker(%s,%d) on lock %u mode %d",
						 edges[numEdges].waiter.cluster_name,
						 edges[numEdges].waiter.backendPid,
						 edges[numEdges].blocker.cluster_name,
						 edges[numEdges].blocker.backendPid,
						 edges[numEdges].lockOid,
						 edges[numEdges].lockMode);

					numEdges++;
				}
			}
		}
	}

	graph->numEdges = numEdges;
	return graph;
}

/*
 * QueryRemoteLockGraph
 *		Query a remote node for its lock graph using postgres_fdw
 *
 * This function connects to a remote PostgreSQL node and retrieves its
 * local lock graph by executing a query that examines pg_locks on the
 * remote server using postgres_fdw's connection infrastructure.
 */
DistLockGraph *
QueryRemoteLockGraph(const char *cluster_name)
{
	DistLockGraph *graph;
	int			ret;
	StringInfoData query;
	MemoryContext oldcxt = CurrentMemoryContext;
	Oid			serverOid;
	ForeignServer *server;

	/* Find the server OID by cluster name */
	server = GetForeignServerByName(cluster_name, false);
	if (server == NULL)
		elog(ERROR, "foreign server \"%s\" not found", cluster_name);

	serverOid = server->serverid;

	/* Allocate graph structure */
	graph = (DistLockGraph *) palloc(sizeof(DistLockGraph));
	strncpy(graph->cluster_name, cluster_name, NAMEDATALEN);
	graph->numEdges = 0;
	graph->edges = NULL;

	/*
	 * Query the remote server using postgres_fdw_get_locks() function.
	 * This function should be implemented in postgres_fdw to query pg_locks
	 * on the remote server and return the results.
	 *
	 * We use the postgres_fdw_get_locks() function which internally uses
	 * postgres_fdw's GetConnection() and pgfdw_exec_query() to execute
	 * the query on the remote server.
	 */

	/* Start SPI */
	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "SPI_connect failed: %d", ret);

	/*
	 * Call postgres_fdw_get_locks(serverOid) which returns lock information
	 * from the remote server. This function is implemented in postgres_fdw
	 * and uses the FDW connection infrastructure.
	 *
	 * Format: SELECT * FROM postgres_fdw_get_locks(serverOid)
	 *
	 * Note: We set search_path to include 'public' where postgres_fdw 
	 * functions are typically installed. This ensures the function can be
	 * found even if called from a context with a restricted search_path
	 * (like during deadlock detection).
	 */
	
	/* Set search_path to ensure postgres_fdw_get_locks can be found */
	SPI_execute("SET LOCAL search_path = pg_catalog, public", false, 0);
	
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT "
					 "  waiter_pid, "
					 "  blocker_pid, "
					 "  waiter_xid, "
					 "  blocker_xid, "
					 "  object_oid, "
					 "  lock_mode "
					 "FROM postgres_fdw_get_locks(%u)",
					 serverOid);

	ret = SPI_execute(query.data, true, 0);
	pfree(query.data);

	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		return graph;
	}

	/* Process the results */
	if (SPI_processed > 0)
	{
		int			i;

		graph->numEdges = SPI_processed;
		graph->edges = (DistWaitEdge *) MemoryContextAlloc(oldcxt, graph->numEdges * sizeof(DistWaitEdge));

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple	tuple = SPI_tuptable->vals[i];
			bool		isnull;
			Datum		val;

			strncpy(graph->edges[i].waiter.cluster_name, cluster_name, NAMEDATALEN);

			val = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
			graph->edges[i].waiter.backendPid = isnull ? 0 : DatumGetInt32(val);

			val = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 3, &isnull);
			graph->edges[i].waiter.xid = isnull ? InvalidTransactionId : DatumGetTransactionId(val);

			strncpy(graph->edges[i].blocker.cluster_name, cluster_name, NAMEDATALEN);

			val = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 2, &isnull);
			graph->edges[i].blocker.backendPid = isnull ? 0 : DatumGetInt32(val);

			val = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 4, &isnull);
			graph->edges[i].blocker.xid = isnull ? InvalidTransactionId : DatumGetTransactionId(val);

			val = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 5, &isnull);
			graph->edges[i].lockOid = isnull ? InvalidOid : DatumGetObjectId(val);

			/* For simplicity, we'll use a placeholder for lock mode */
			graph->edges[i].lockMode = AccessExclusiveLock;
			
			elog(DEBUG2, "Remote edge from %s: waiter(%s,%d) -> blocker(%s,%d) on lock %u mode %d",
				 cluster_name,
				 graph->edges[i].waiter.cluster_name,
				 graph->edges[i].waiter.backendPid,
				 graph->edges[i].blocker.cluster_name,
				 graph->edges[i].blocker.backendPid,
				 graph->edges[i].lockOid,
				 graph->edges[i].lockMode);
		}
	}

	SPI_finish();

	return graph;
}

/*
 * QueryRemoteFdwConnections
 *		Query a remote server for its FDW connection mappings
 *
 * This function queries postgres_connections() on a remote server to get
 * FDW connection mappings from that server, then creates dependency edges.
 *
 * Returns a DistLockGraph containing dependency edges (remote_backend → local_backend)
 */
static DistLockGraph *
QueryRemoteFdwConnections(const char *cluster_name)
{
	DistLockGraph *graph;
	int			ret;
	StringInfoData query;
	MemoryContext oldcxt = CurrentMemoryContext;
	Oid			serverOid;
	ForeignServer *server;

	/* Find the server OID by cluster name */
	server = GetForeignServerByName(cluster_name, false);
	if (server == NULL)
		elog(ERROR, "foreign server \"%s\" not found", cluster_name);

	serverOid = server->serverid;

	/* Allocate result structure */
	graph = (DistLockGraph *) palloc0(sizeof(DistLockGraph));
	strncpy(graph->cluster_name, cluster_name, NAMEDATALEN);
	graph->numEdges = 0;
	graph->edges = NULL;

	/* Start SPI */
	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "SPI_connect failed: %d", ret);

	/*
	 * Query postgres_connections() on the remote server via dblink-style
	 * function call. We use postgres_fdw infrastructure to execute this query.
	 *
	 * Format: SELECT * FROM postgres_fdw_connections(serverOid)
	 * Returns: (cluster_name, local_pid, remote_backend_pid)
	 *
	 * Note: This gets the FDW connections that exist on the remote server,
	 * showing which local PIDs (on that remote server) have connections to
	 * other servers' remote backends.
	 */
	
	/* Set search_path to ensure function can be found */
	SPI_execute("SET LOCAL search_path = pg_catalog, public", false, 0);
	
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT "
					 "  cluster_name, "
					 "  local_pid, "
					 "  remote_backend_pid "
					 "FROM postgres_fdw_connections(%u)",
					 serverOid);

	ret = SPI_execute(query.data, true, 0);
	pfree(query.data);

	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		return graph;
	}

	/* Process the results */
	if (SPI_processed > 0)
	{
		int			i;

		graph->numEdges = SPI_processed;
		graph->edges = (DistWaitEdge *) MemoryContextAlloc(oldcxt, graph->numEdges * sizeof(DistWaitEdge));

		for (i = 0; i < SPI_processed; i++)
		{
			Datum		cluster_name_datum;
			Datum		local_pid_datum;
			Datum		remote_pid_datum;
			bool		isnull;
			char	   *fdw_cluster_name;
			int			local_pid;
			int			remote_pid;

			/* Extract values from the result tuple */
			cluster_name_datum = SPI_getbinval(SPI_tuptable->vals[i],
											  SPI_tuptable->tupdesc,
											  1, &isnull);
			if (isnull)
				continue;
			fdw_cluster_name = TextDatumGetCString(cluster_name_datum);

			local_pid_datum = SPI_getbinval(SPI_tuptable->vals[i],
											SPI_tuptable->tupdesc,
											2, &isnull);
			if (isnull)
				continue;
			local_pid = DatumGetInt32(local_pid_datum);

			remote_pid_datum = SPI_getbinval(SPI_tuptable->vals[i],
											 SPI_tuptable->tupdesc,
											 3, &isnull);
			if (isnull)
				continue;
			remote_pid = DatumGetInt32(remote_pid_datum);

			/*
			 * Create dependency edge: remote_backend → local_backend
			 * This represents that the remote backend (on fdw_cluster_name) is
			 * serving an FDW request from local_pid (on cluster_name).
			 */
			strncpy(graph->edges[i].blocker.cluster_name, fdw_cluster_name, NAMEDATALEN);
			graph->edges[i].blocker.backendPid = remote_pid;
			graph->edges[i].blocker.xid = 0;

			strncpy(graph->edges[i].waiter.cluster_name, cluster_name, NAMEDATALEN);
			graph->edges[i].waiter.backendPid = local_pid;
			graph->edges[i].waiter.xid = 0;

			graph->edges[i].lockOid = 0;
			graph->edges[i].lockMode = 0;
			
			elog(DEBUG2, "FDW edge from %s: waiter(%s,%d) -> blocker(%s,%d)",
				 cluster_name,
				 graph->edges[i].waiter.cluster_name,
				 graph->edges[i].waiter.backendPid,
				 graph->edges[i].blocker.cluster_name,
				 graph->edges[i].blocker.backendPid);
		}
	}

	SPI_finish();
	return graph;
}

/*
 * CollectFdwConnectionDependencies
 *		Collect FDW connection dependency edges from local server
 *
 * This function queries postgres_connections() locally to get mappings between
 * local backends and remote backends, then creates dependency edges to
 * represent that the remote backend's work depends on the local backend.
 *
 * Returns a DistLockGraph containing dependency edges (remote_backend → local_backend)
 */
static DistLockGraph *
CollectFdwConnectionDependencies(void)
{
	DistLockGraph *graph;
	int			ret;
	StringInfoData query;
	MemoryContext oldcxt = CurrentMemoryContext;

	/* Allocate result structure */
	graph = (DistLockGraph *) palloc0(sizeof(DistLockGraph));
	graph->numEdges = 0;
	graph->edges = NULL;

	/* Start SPI */
	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "SPI_connect failed: %d", ret);

	/*
	 * Call postgres_connections() which returns active FDW connection
	 * mappings. This function is implemented in postgres_fdw and uses
	 * PQbackendPID() to get remote backend PIDs from the connection cache.
	 *
	 * Format: SELECT * FROM postgres_connections()
	 * Returns: (cluster_name, local_pid, remote_backend_pid)
	 */
	
	/* Set search_path to ensure postgres_connections can be found */
	SPI_execute("SET LOCAL search_path = pg_catalog, public", false, 0);
	
	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT "
					 "  cluster_name, "
					 "  local_pid, "
					 "  remote_backend_pid "
					 "FROM postgres_connections()");

	ret = SPI_execute(query.data, true, 0);
	pfree(query.data);

	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		return graph;
	}

	/* Process the results */
	if (SPI_processed > 0)
	{
		int			i;

		graph->numEdges = SPI_processed;

		graph->edges = (DistWaitEdge *) MemoryContextAlloc(oldcxt, graph->numEdges * sizeof(DistWaitEdge));

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple	tuple = SPI_tuptable->vals[i];
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			char	   *cluster_name_str;
			int			local_pid;
			int			remote_backend_pid;
			bool		isnull;

			/* Extract values */
			cluster_name_str = TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 1, &isnull));
			if (isnull) continue;
			
			local_pid = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, &isnull));
			if (isnull) continue;
			
			remote_backend_pid = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 3, &isnull));
			if (isnull) continue;

			/*
			 * Create dependency edge: remote_backend → local_backend
			 * This represents that the remote backend (serving the FDW connection)
			 * depends on the local backend (the client).
			 *
			 * The "waiter" is the local backend, "blocker" is the remote backend
			 * (meaning remote backend is waiting for/depends on local backend)
			 */
			strncpy(graph->edges[i].blocker.cluster_name, cluster_name_str, NAMEDATALEN);
			graph->edges[i].blocker.backendPid = remote_backend_pid;
			graph->edges[i].blocker.xid = InvalidTransactionId;

			strncpy(graph->edges[i].waiter.cluster_name, cluster_name, NAMEDATALEN);
			graph->edges[i].waiter.backendPid = local_pid;
			graph->edges[i].waiter.xid = InvalidTransactionId;

			graph->edges[i].lockOid = InvalidOid;
			graph->edges[i].lockMode = 0;
			
			elog(DEBUG2, "FDW edge: waiter(%s,%d) -> blocker(%s,%d)",
				 graph->edges[i].waiter.cluster_name,
				 graph->edges[i].waiter.backendPid,
				 graph->edges[i].blocker.cluster_name,
				 graph->edges[i].blocker.backendPid);

			pfree(cluster_name_str);
		}
	}

	SPI_finish();

	return graph;
}

/*
 * MergeLockGraphs
 *		Merge multiple lock graphs into a single unified graph
 */
DistLockGraph *
MergeLockGraphs(DistLockGraph **graphs, int numGraphs)
{
	DistLockGraph *merged;
	int			totalEdges = 0;
	int			i, j;
	int			edgeIndex = 0;

	/* Calculate total number of edges */
	for (i = 0; i < numGraphs; i++)
		totalEdges += graphs[i]->numEdges;

	/* Allocate merged graph */
	merged = (DistLockGraph *) palloc(sizeof(DistLockGraph));
	strncpy(merged->cluster_name, cluster_name, NAMEDATALEN);
	merged->numEdges = totalEdges;
	merged->edges = (DistWaitEdge *) palloc(totalEdges * sizeof(DistWaitEdge));

	/* Copy all edges from all graphs */
	for (i = 0; i < numGraphs; i++)
	{
		for (j = 0; j < graphs[i]->numEdges; j++)
		{
			elog(DEBUG2, "Merging edge: waiter(%s,%d) -> blocker(%s,%d)",
				 graphs[i]->edges[j].waiter.cluster_name,
				 graphs[i]->edges[j].waiter.backendPid,
				 graphs[i]->edges[j].blocker.cluster_name,
				 graphs[i]->edges[j].blocker.backendPid);
			merged->edges[edgeIndex++] = graphs[i]->edges[j];
		}
	}

	return merged;
}

/*
 * GetClusterNameFromServerOid
 *		Get the cluster name (srvname) for a given server OID
 *
 * Returns a palloc'd string with the cluster name from pg_foreign_server.srvname
 * Returns NULL if the server OID is not found.
 */
char *
GetClusterNameFromServerOid(Oid serverOid)
{
	ForeignServer *server;

	if (!OidIsValid(serverOid))
		return NULL;

	server = GetForeignServer(serverOid);
	if (server == NULL)
		return NULL;

	return pstrdup(server->servername);
}

/*
 * Helper function: Check if two nodes are equal
 */
static bool
NodesEqual(DistWaitNode *n1, DistWaitNode *n2)
{
	return (strcmp(n1->cluster_name, n2->cluster_name) == 0 &&
			n1->backendPid == n2->backendPid);
}

/*
 * DetectCycleRecurse
 *		Recursive helper for cycle detection using DFS
 *
 * Returns the path length if a cycle is found, 0 otherwise.
 * The cycleStart parameter is set to the index where the cycle begins.
 *
 * Note: This implementation has O(V*E) time complexity where V is the number
 * of nodes and E is the number of edges, because we scan all edges for each
 * recursive call. For better performance with large graphs, consider using
 * an adjacency list representation which would reduce this to O(V+E).
 */
static int
DetectCycleRecurse(DistWaitNode *currentNode,
				   DistLockGraph *graph,
				   DistWaitNode *path,
				   int pathLen,
				   int *cycleStart)
{
	int			i;

	/* Check if we've seen this node before in the current path */
	for (i = 0; i < pathLen; i++)
	{
		if (NodesEqual(&path[i], currentNode))
		{
			/* Found a cycle! */
			*cycleStart = i;
			return pathLen;		/* Return current path length */
		}
	}

	/* Add current node to path */
	if (pathLen >= maxVisitedNodes)
		elog(ERROR, "path too long in distributed deadlock detection");

	path[pathLen] = *currentNode;
	pathLen++;

	/* Follow all outgoing edges from this node */
	for (i = 0; i < graph->numEdges; i++)
	{
		if (NodesEqual(&graph->edges[i].waiter, currentNode))
		{
			int			result;

			/* This is an outgoing edge, follow it */
			result = DetectCycleRecurse(&graph->edges[i].blocker, graph, path, pathLen, cycleStart);
			if (result > 0)
				return result;	/* Pass cycle length back up */
		}
	}

	return 0;					/* No cycle found */
}

/*
 * DetectDistributedDeadlock
 *		Check for cycles in the merged distributed lock graph
 *
 * Uses depth-first search to find cycles in the wait-for graph.
 */
DistDeadlockInfo *
DetectDistributedDeadlock(DistLockGraph *mergedGraph)
{
	DistDeadlockInfo *info;
	DistWaitNode *path;
	int			i;
	int			cycleStart = -1;

	/* Allocate result structure */
	info = (DistDeadlockInfo *) palloc0(sizeof(DistDeadlockInfo));
	info->deadlockFound = false;
	info->cycleLength = 0;
	info->cycleNodes = NULL;
	info->cycleEdges = NULL;

	if (mergedGraph->numEdges == 0)
		return info;

	/* Allocate path for DFS */
	path = (DistWaitNode *) palloc(maxVisitedNodes * sizeof(DistWaitNode));

	/*
	 * Try starting DFS from each node that appears as a waiter. This ensures
	 * we check all potential starting points for cycles.
	 */
	for (i = 0; i < mergedGraph->numEdges; i++)
	{
		DistWaitNode *startNode = &mergedGraph->edges[i].waiter;
		int			pathLength;

		pathLength = DetectCycleRecurse(startNode, mergedGraph, path, 0, &cycleStart);
		if (pathLength > 0)
		{
			/* Found a cycle! Extract it from the path */
			int			cycleLen;
			int			j;

			info->deadlockFound = true;
			cycleLen = pathLength - cycleStart;	/* Length from cycle start to end */

			info->cycleLength = cycleLen;
			info->cycleNodes = (DistWaitNode *) palloc(cycleLen * sizeof(DistWaitNode));

			/* Copy cycle nodes */
			for (j = 0; j < cycleLen; j++)
			{
				info->cycleNodes[j] = path[cycleStart + j];
			}

			pfree(path);
			return info;
		}
	}

	pfree(path);
	return info;
}

/*
 * PerformGlobalDistributedDeadlockCheck
 *		Perform distributed deadlock detection across ALL shard groups
 *
 * This function performs distributed deadlock detection by checking all
 * shard groups in the system, not just a specific one. This is necessary
 * because deadlocks can span multiple shard groups.
 *
 * Algorithm:
 * 1. Get all shard groups in the system
 * 2. Collect all unique cluster names from all shard groups
 * 3. Query each unique server for its lock graph
 * 4. Merge all graphs into a single global graph
 * 5. Detect cycles in the global graph
 */
DistDeadlockInfo *
PerformGlobalDistributedDeadlockCheck(void)
{
	List	   *allServers = NIL;
	List	   *uniqueClusterNames = NIL;
	ListCell   *lc;
	DistLockGraph **graphs;
	int			numGraphs;
	int			graphIndex;
	int			i;
	DistLockGraph *mergedGraph;
	DistDeadlockInfo *result;
	Relation	sgrel;
	SysScanDesc scan;
	HeapTuple	tuple;

	/*
	 * Scan pg_shardgroups to get all shard groups, then collect all their
	 * members
	 */
	sgrel = table_open(ShardGroupRelationId, AccessShareLock);
	scan = systable_beginscan(sgrel, InvalidOid, false, NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_shardgroups sgform = (Form_pg_shardgroups) GETSTRUCT(tuple);
		Oid			sgid = sgform->oid;
		List	   *members;

		/* Get members of this shard group */
		members = get_shardgroup_members(sgid);

		/*
		 * Add all members to our list. Note: list_concat() modifies and
		 * returns the first list, so we don't need to free members separately.
		 */
		allServers = list_concat(allServers, members);
	}

	systable_endscan(scan);
	table_close(sgrel, AccessShareLock);

	/*
	 * Convert server OIDs to cluster names and deduplicate.
	 */
	foreach(lc, allServers)
	{
		Oid			serverOid = lfirst_oid(lc);
		char	   *cluster_name;
		bool		found = false;
		ListCell   *lc2;

		if (!OidIsValid(serverOid))
			continue;

		cluster_name = GetClusterNameFromServerOid(serverOid);
		if (cluster_name == NULL)
			continue;

		/* Check if already in list */
		foreach(lc2, uniqueClusterNames)
		{
			char *existing = (char *) lfirst(lc2);
			if (strcmp(existing, cluster_name) == 0)
			{
				found = true;
				pfree(cluster_name);
				break;
			}
		}

		if (!found)
			uniqueClusterNames = lappend(uniqueClusterNames, cluster_name);
	}

	/*
	 * Calculate number of graphs:
	 * - 1 for local lock graph
	 * - 1 for local FDW connection dependencies
	 * - list_length(uniqueClusterNames) for remote lock graphs
	 * - list_length(uniqueClusterNames) for remote FDW connection dependencies
	 */
	numGraphs = 2 + (list_length(uniqueClusterNames) * 2);

	/* Allocate array of graph pointers */
	graphs = (DistLockGraph **) palloc(numGraphs * sizeof(DistLockGraph *));

	/* Collect local graph first */
	graphIndex = 0;
	graphs[graphIndex++] = CollectLocalLockGraph();
	
	/* Collect local FDW connection dependencies */
	graphs[graphIndex++] = CollectFdwConnectionDependencies();

	/* Query each unique remote server for its lock graph AND FDW connections */
	foreach(lc, uniqueClusterNames)
	{
		char	   *cluster_name = (char *) lfirst(lc);

		/* Get lock graph from remote server */
		graphs[graphIndex++] = QueryRemoteLockGraph(cluster_name);
		
		/* Get FDW connection dependencies from remote server */
		graphs[graphIndex++] = QueryRemoteFdwConnections(cluster_name);
	}

	/*
	 * Merge all graphs from all shard groups. At this point, graphIndex
	 * equals numGraphs since we successfully added all graphs.
	 */
	mergedGraph = MergeLockGraphs(graphs, graphIndex);

	/* Detect deadlocks in the global merged graph */
	result = DetectDistributedDeadlock(mergedGraph);

	/* Clean up intermediate structures */
	for (i = 0; i < graphIndex; i++)
		FreeDistLockGraph(graphs[i]);
	FreeDistLockGraph(mergedGraph);
	pfree(graphs);

	/* Clean up lists */
	list_free(allServers);
	list_free_deep(uniqueClusterNames);

	return result;
}

/*
 * FreeDistLockGraph
 *		Free a distributed lock graph structure
 */
void
FreeDistLockGraph(DistLockGraph *graph)
{
	if (graph == NULL)
		return;

	if (graph->edges != NULL)
		pfree(graph->edges);

	pfree(graph);
}

/*
 * FreeDistDeadlockInfo
 *		Free a distributed deadlock info structure
 */
void
FreeDistDeadlockInfo(DistDeadlockInfo *info)
{
	if (info == NULL)
		return;

	if (info->cycleNodes != NULL)
		pfree(info->cycleNodes);

	if (info->cycleEdges != NULL)
		pfree(info->cycleEdges);

	pfree(info);
}
