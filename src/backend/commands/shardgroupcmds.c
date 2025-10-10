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

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_shardgroups.h"
#include "catalog/pg_shardmembers.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/shardgroupcmds.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/* Helper functions */
extern Oid get_shardgroup_oid(const char *sgname, bool missing_ok);
extern Oid get_database_default_shardgroup(Oid dbid);
extern void SetRelationShardGroup(Oid relid, Oid sgid);


/*
 * CREATE SHARD GROUP
 */
ObjectAddress
CreateShardGroup(CreateShardGroupStmt *stmt)
{
	Relation	rel;
	HeapTuple	tuple;
	Datum		values[Natts_pg_shardgroups];
	bool		nulls[Natts_pg_shardgroups];
	NameData	sgname;
	Oid			sgoid;
	Oid			ownerId = GetUserId();
	ObjectAddress address;
	
	/* Check permissions - only superuser can create shard groups */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create shard group"),
				 errhint("Must be superuser to create shard groups.")));
	
	/* Check if shard group already exists */
	if (OidIsValid(get_shardgroup_oid(stmt->sgname, true)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("shard group \"%s\" already exists", stmt->sgname)));
	
	/* Build the tuple */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	
	namestrcpy(&sgname, stmt->sgname);
	values[Anum_pg_shardgroups_sgname - 1] = NameGetDatum(&sgname);
	values[Anum_pg_shardgroups_sgowner - 1] = ObjectIdGetDatum(ownerId);
	
	/* Process options if any */
	if (stmt->options != NIL)
	{
		/* TODO: Process and validate options, store as JSON/text */
		/* For now, we'll store a simple text representation */
		nulls[Anum_pg_shardgroups_sgoptions - 1] = true;
	}
	else
	{
		nulls[Anum_pg_shardgroups_sgoptions - 1] = true;
	}
	
	/* Insert tuple into pg_shardgroups */
	rel = table_open(ShardGroupRelationId, RowExclusiveLock);
	
	sgoid = GetNewOidWithIndex(rel, ShardGroupOidIndexId,
							   Anum_pg_shardgroups_oid);
	values[Anum_pg_shardgroups_oid - 1] = ObjectIdGetDatum(sgoid);
	
	tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tuple);
	
	heap_freetuple(tuple);
	
	/* Record dependency on owner */
	recordDependencyOnOwner(ShardGroupRelationId, sgoid, ownerId);
	
	/* Post creation hook for extensions */
	InvokeObjectPostCreateHook(ShardGroupRelationId, sgoid, 0);
	
	table_close(rel, RowExclusiveLock);
	
	ObjectAddressSet(address, ShardGroupRelationId, sgoid);
	return address;
}

/*
 * ALTER SHARD GROUP
 */
ObjectAddress
AlterShardGroup(AlterShardGroupStmt *stmt)
{
	Oid			sgoid;
	Oid			srvoid;
	ObjectAddress address;
	
	/* Check permissions */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to alter shard group"),
				 errhint("Must be superuser to alter shard groups.")));
	
	/* Get the shard group OID */
	sgoid = get_shardgroup_oid(stmt->sgname, false);
	
	if (strcmp(stmt->action, "ADD") == 0)
	{
		Relation	rel;
		HeapTuple	tuple;
		Datum		values[Natts_pg_shardmembers];
		bool		nulls[Natts_pg_shardmembers];
		Oid			memberoid;
		int32		priority = 100;	/* default priority */
		char		state = SHARD_MEMBER_STATE_UP;	/* default state */
		ListCell   *cell;
		ObjectAddress shardgroupAddr, serverAddr, memberAddr;
		
		/* Get the foreign server OID */
		srvoid = get_foreign_server_oid(stmt->servername, false);
		
		/* Check if this server is already a member */
		{
			Relation	memberrel;
			SysScanDesc scan;
			ScanKeyData key[2];
			HeapTuple	tmptuple;
			
			memberrel = table_open(ShardMemberRelationId, AccessShareLock);
			
			ScanKeyInit(&key[0],
						Anum_pg_shardmembers_sgid,
						BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(sgoid));
			ScanKeyInit(&key[1],
						Anum_pg_shardmembers_srvid,
						BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(srvoid));
			
			scan = systable_beginscan(memberrel, ShardMemberSgidSrvidIndexId, true,
									  NULL, 2, key);
			tmptuple = systable_getnext(scan);
			
			if (HeapTupleIsValid(tmptuple))
			{
				systable_endscan(scan);
				table_close(memberrel, AccessShareLock);
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("server \"%s\" is already a member of shard group \"%s\"",
								stmt->servername, stmt->sgname)));
			}
			
			systable_endscan(scan);
			table_close(memberrel, AccessShareLock);
		}
		
		/* Process member options */
		foreach(cell, stmt->options)
		{
			DefElem    *defel = (DefElem *) lfirst(cell);
			
			if (strcmp(defel->defname, "priority") == 0)
				priority = defGetInt32(defel);
			else if (strcmp(defel->defname, "state") == 0)
			{
				char	   *statestr = defGetString(defel);
				if (strcmp(statestr, "up") == 0)
					state = SHARD_MEMBER_STATE_UP;
				else if (strcmp(statestr, "down") == 0)
					state = SHARD_MEMBER_STATE_DOWN;
				else if (strcmp(statestr, "readonly") == 0 || strcmp(statestr, "read-only") == 0)
					state = SHARD_MEMBER_STATE_READONLY;
				else
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid member state \"%s\"", statestr),
							 errhint("Valid states are: up, down, readonly")));
			}
			else
				ereport(WARNING,
						(errmsg("unrecognized option: %s", defel->defname)));
		}
		
		/* Build the tuple */
		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));
		
		values[Anum_pg_shardmembers_sgid - 1] = ObjectIdGetDatum(sgoid);
		values[Anum_pg_shardmembers_srvid - 1] = ObjectIdGetDatum(srvoid);
		values[Anum_pg_shardmembers_smpriority - 1] = Int32GetDatum(priority);
		values[Anum_pg_shardmembers_smstate - 1] = CharGetDatum(state);
		nulls[Anum_pg_shardmembers_smoptions - 1] = true;	/* No options for now */
		
		/* Insert tuple into pg_shardmembers */
		rel = table_open(ShardMemberRelationId, RowExclusiveLock);
		
		memberoid = GetNewOidWithIndex(rel, ShardMemberOidIndexId,
									   Anum_pg_shardmembers_oid);
		values[Anum_pg_shardmembers_oid - 1] = ObjectIdGetDatum(memberoid);
		
		tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
		CatalogTupleInsert(rel, tuple);
		
		heap_freetuple(tuple);
		
		/* Record dependencies */
		ObjectAddressSet(shardgroupAddr, ShardGroupRelationId, sgoid);
		ObjectAddressSet(serverAddr, ForeignServerRelationId, srvoid);
		ObjectAddressSet(memberAddr, ShardMemberRelationId, memberoid);
		
		recordDependencyOn(&memberAddr, &shardgroupAddr, DEPENDENCY_AUTO);
		recordDependencyOn(&memberAddr, &serverAddr, DEPENDENCY_NORMAL);
		
		/* Post creation hook */
		InvokeObjectPostCreateHook(ShardMemberRelationId, memberoid, 0);
		
		table_close(rel, RowExclusiveLock);
		
		ObjectAddressSet(address, ShardMemberRelationId, memberoid);
	}
	else if (strcmp(stmt->action, "DROP") == 0)
	{
		Relation	rel;
		HeapTuple	tuple;
		Oid			memberoid;
		
		/* Get the foreign server OID */
		srvoid = get_foreign_server_oid(stmt->servername, false);
		
		/* Find the member tuple */
		rel = table_open(ShardMemberRelationId, RowExclusiveLock);
		
		{
			SysScanDesc scan;
			ScanKeyData key[2];
			
			ScanKeyInit(&key[0],
						Anum_pg_shardmembers_sgid,
						BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(sgoid));
			ScanKeyInit(&key[1],
						Anum_pg_shardmembers_srvid,
						BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(srvoid));
			
			scan = systable_beginscan(rel, ShardMemberSgidSrvidIndexId, true,
									  NULL, 2, key);
			tuple = systable_getnext(scan);
			
			if (!HeapTupleIsValid(tuple))
			{
				systable_endscan(scan);
				table_close(rel, RowExclusiveLock);
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("server \"%s\" is not a member of shard group \"%s\"",
								stmt->servername, stmt->sgname)));
			}
			
			memberoid = ((Form_pg_shardmembers) GETSTRUCT(tuple))->oid;
			
			systable_endscan(scan);
		}
		
		table_close(rel, RowExclusiveLock);
		
		/* Perform deletion via dependency system */
		{
			ObjectAddress memberAddr;
			ObjectAddressSet(memberAddr, ShardMemberRelationId, memberoid);
			performDeletion(&memberAddr, DROP_RESTRICT, 0);
		}
		
		ObjectAddressSet(address, ShardMemberRelationId, memberoid);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("unrecognized ALTER SHARD GROUP action: %s", stmt->action)));
	}
	
	return address;
}

/*
 * ALTER TABLE SET SHARD GROUP
 *
 * This handles the ALTER TABLE ... SET SHARD GROUP command.
 */
void
AlterTableSetShardGroup(AlterTableSetShardGroupStmt *stmt)
{
	Oid			relid;
	Oid			sgid;
	Relation	rel;
	
	/* Look up the relation */
	relid = RangeVarGetRelidExtended(stmt->relation, AccessExclusiveLock,
									 0, NULL, NULL);
	
	/* Open the relation to validate it */
	rel = table_open(relid, NoLock);
	
	/* Validate that the relation is a distributed or worldwide table */
	if (rel->rd_rel->relkind != RELKIND_DISTRIBUTED_TABLE &&
		rel->rd_rel->relkind != RELKIND_WORLDWIDE_TABLE)
	{
		table_close(rel, AccessExclusiveLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" is not a distributed or worldwide table",
						RelationGetRelationName(rel)),
				 errhint("Only distributed ('D') and worldwide ('W') tables can be assigned to shard groups.")));
	}
	
	/* Get the shard group OID */
	sgid = get_shardgroup_oid(stmt->sgname, false);
	
	/* TODO: Validate that all partitions can be routed to the new shard group */
	/* TODO: For worldwide tables, ensure replication contract is satisfiable */
	
	/* Set the shard group for the relation */
	SetRelationShardGroup(relid, sgid);
	
	table_close(rel, NoLock);
}

/*
 * Helper function to get shard group OID by name
 * Returns InvalidOid if not found (when missing_ok is true)
 */
Oid
get_shardgroup_oid(const char *sgname, bool missing_ok)
{
	Oid			sgid;
	HeapTuple	tuple;
	
	tuple = SearchSysCache1(SHARDGROUPNAME, CStringGetDatum(sgname));
	
	if (!HeapTupleIsValid(tuple))
	{
		if (!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("shard group \"%s\" does not exist", sgname)));
		return InvalidOid;
	}
	
	sgid = ((Form_pg_shardgroups) GETSTRUCT(tuple))->oid;
	ReleaseSysCache(tuple);
	
	return sgid;
}

/*
 * Helper function to get default shard group for a database
 * Returns InvalidOid if no default is set
 */
Oid
get_database_default_shardgroup(Oid dbid)
{
	Oid			sgid = InvalidOid;
	HeapTuple	tuple;
	
	tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbid));
	
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_database dbform = (Form_pg_database) GETSTRUCT(tuple);
		sgid = dbform->datshardgroup;
		ReleaseSysCache(tuple);
	}
	
	return sgid;
}

/*
 * Helper function to set the shard group for a relation
 * This updates pg_class.relsgid
 */
void
SetRelationShardGroup(Oid relid, Oid sgid)
{
	Relation	rel;
	HeapTuple	tuple;
	Form_pg_class classForm;
	
	rel = table_open(RelationRelationId, RowExclusiveLock);
	
	tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	
	classForm = (Form_pg_class) GETSTRUCT(tuple);
	classForm->relsgid = sgid;
	
	CatalogTupleUpdate(rel, &tuple->t_self, tuple);
	
	heap_freetuple(tuple);
	table_close(rel, RowExclusiveLock);
	
	/* Invalidate relcache entry */
	CacheInvalidateRelcacheByRelid(relid);
}
