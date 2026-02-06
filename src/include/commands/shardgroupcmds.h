/*-------------------------------------------------------------------------
 *
 * shardgroupcmds.h
 *	  prototypes for shardgroupcmds.c.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/shardgroupcmds.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHARDGROUPCMDS_H
#define SHARDGROUPCMDS_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"
#include "utils/guc.h"

extern ObjectAddress CreateShardGroup(CreateShardGroupStmt *stmt);
extern ObjectAddress AlterShardGroup(AlterShardGroupStmt *stmt);
/* AlterTableSetShardGroup removed - not fully implemented */

/* Helper functions for shard group management */
extern Oid get_shardgroup_oid(const char *sgname, bool missing_ok);
extern Oid get_database_default_shardgroup(Oid dbid);
extern void SetRelationShardGroup(Oid relid, Oid sgid);
extern List *get_shardgroup_members(Oid sgid);

/* Consistent hashing for partition placement */
extern Oid GetPartitionTargetMember(Oid sgid, const char *partition_name, Oid *exclude_member, bool *found);

/* Partition bounds deparsing */
extern char *PartitionBoundsSpecToString(PartitionBoundSpec *partbound);

/* Flag to track if DDL is being executed from a remote server (to prevent infinite recursion) */
extern bool executing_remote_ddl;

/* GUC check hook for executing_remote_ddl */
extern bool check_executing_remote_ddl(bool *newval, void **extra, GucSource source);

#endif							/* SHARDGROUPCMDS_H */
