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
extern void AlterDatabaseSetShardGroup(AlterDatabaseSetShardGroupStmt *stmt);
extern void AlterTableSetShardGroup(AlterTableSetShardGroupStmt *stmt);

#endif							/* SHARDGROUPCMDS_H */
