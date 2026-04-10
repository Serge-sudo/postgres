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

extern ObjectAddress CreateShardGroup(CreateShardGroupStmt *stmt);
extern ObjectAddress AlterShardGroup(AlterShardGroupStmt *stmt);
extern void AlterTableSetShardGroup(AlterTableSetShardGroupStmt *stmt);

/* Helper functions for shard group management */
extern Oid get_shardgroup_oid(const char *sgname, bool missing_ok);
extern Oid get_database_default_shardgroup(Oid dbid);
extern void SetRelationShardGroup(Oid relid, Oid sgid);
extern List *get_shardgroup_members(Oid sgid);

#endif							/* SHARDGROUPCMDS_H */
