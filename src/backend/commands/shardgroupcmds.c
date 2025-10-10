/*-------------------------------------------------------------------------
 *
 * shardgroupcmds.c
 *	  shard group creation/manipulation commands
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/shardgroupcmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_shardgroups.h"
#include "catalog/pg_shardmembers.h"
#include "catalog/pg_db_shardgroup.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_database.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/*
 * CREATE SHARD GROUP
 *
 * TODO: Add full implementation with options processing
 */
ObjectAddress
CreateShardGroup(CreateShardGroupStmt *stmt)
{
	ObjectAddress address;
	
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("CREATE SHARD GROUP not yet implemented")));
	
	/* TODO: Implement shard group creation */
	memset(&address, 0, sizeof(address));
	return address;
}

/*
 * ALTER SHARD GROUP
 *
 * TODO: Add full implementation for ADD/DROP MEMBER
 */
ObjectAddress
AlterShardGroup(AlterShardGroupStmt *stmt)
{
	ObjectAddress address;
	
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("ALTER SHARD GROUP not yet implemented")));
	
	/* TODO: Implement shard group alteration */
	memset(&address, 0, sizeof(address));
	return address;
}

/*
 * ALTER DATABASE SET DEFAULT SHARD GROUP
 *
 * TODO: Add full implementation
 */
void
AlterDatabaseSetShardGroup(AlterDatabaseSetShardGroupStmt *stmt)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("ALTER DATABASE SET DEFAULT SHARD GROUP not yet implemented")));
	
	/* TODO: Implement default shard group setting */
}

/*
 * ALTER TABLE SET SHARD GROUP
 *
 * TODO: Add full implementation
 */
void
AlterTableSetShardGroup(AlterTableSetShardGroupStmt *stmt)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("ALTER TABLE SET SHARD GROUP not yet implemented")));
	
	/* TODO: Implement table shard group assignment */
}
