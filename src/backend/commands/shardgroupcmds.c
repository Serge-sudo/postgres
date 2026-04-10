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

#include "executor/spi.h"
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
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_partitioned_table.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_shardgroups.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_shardmembers.h"
#include "catalog/partition.h"
#include "commands/dbcommands.h"
#include "common/hashfn.h"
#include "commands/defrem.h"
#include "commands/shardgroupcmds.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "partitioning/partdefs.h"
#include "partitioning/partdesc.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"
#include "foreign/foreign.h"
			  
/* Helper functions */
extern Oid get_shardgroup_oid(const char *sgname, bool missing_ok);
extern Oid get_database_default_shardgroup(Oid dbid);
extern void SetRelationShardGroup(Oid relid, Oid sgid);

/* Forward declaration of helper function for syncing tables on new shard member */
static void SyncTablesOnNewShardMember(Oid sgid, Oid newsrvoid);
static void SyncShardGroupMetadataToMember(Oid sgid, Oid newsrvoid);
static void NotifyExistingMembersAboutNewMember(Oid sgid, Oid newsrvoid);
static void ExecuteDDLOnRemoteServer(Oid serveroid, const char *sql);

/* Consistent hashing functions */
static uint32 hash_string_to_uint32(const char *str);
static void ReshardShardGroup(Oid sgid);
static void DetachShardMember(Oid sgid, Oid srvoid);

/* Helper functions for partition migration */
static void MigratePartitionToMember(Oid partitionOid, Oid fromServer, Oid toServer, Oid sgid);
static Oid FindPartitionHostMember(Oid partitionOid, Oid sgid);
static List *GetShardGroupPartitions(Oid sgid);
static void CopyFileToRemoteFDW(Oid src_serverid, Oid dest_serverid, const char *src_nspname, const char *src_relname, const char *dest_nspname, const char *dest_relname);

/* Number of virtual nodes per shard member for consistent hashing */
#define VIRTUAL_NODES_PER_MEMBER 150
/* Maximum length for virtual node key strings */
#define VNODE_KEY_MAX_LEN 512

/* 
 * GUC variable to track if DDL is being executed from a remote server.
 * This prevents infinite recursion when DDL operations are replicated.
 * When postgres_fdw executes DDL on a remote server, it prefixes the DDL with
 * "SET LOCAL shardgroup.executing_remote_ddl = true;" to set this flag on the remote server.
 */
bool executing_remote_ddl = false;

/*
 * check_executing_remote_ddl
 *		GUC check hook for shardgroup.executing_remote_ddl
 *
 * This hook ensures that the GUC can only be set to true via SET LOCAL within
 * a transaction, which is how postgres_fdw sets it during DDL replication.
 * This prevents users from manually setting the flag through interactive sessions.
 */
bool
check_executing_remote_ddl(bool *newval, void **extra, GucSource source)
{
	/*
	 * Allow setting to false from any source (resetting is safe).
	 */
	if (*newval == false)
		return true;
	
	/*
	 * When setting to true, only allow it if:
	 * 1. We're in a transaction block (postgres_fdw uses SET LOCAL)
	 * 2. The source is from a session SET command (not from config file)
	 * 
	 * This combination means it's likely from postgres_fdw's remote DDL execution.
	 * We reject attempts from:
	 * - Configuration files (PGC_S_FILE, PGC_S_OVERRIDE)
	 * - ALTER DATABASE/ROLE commands (PGC_S_DATABASE_USER, etc.)
	 * - Command line options (PGC_S_ARGV)
	 * 
	 * We also reject if NOT in a transaction, which would be a user trying to
	 * set it from psql or similar without using SET LOCAL in a transaction.
	 */
	if (source == PGC_S_SESSION)
	{
		/*
		 * Check if we're in a transaction. postgres_fdw always uses SET LOCAL
		 * which requires being in a transaction block.
		 */
		if (!IsTransactionState() || !IsTransactionBlock())
		{
			GUC_check_errdetail("shardgroup.executing_remote_ddl can only be set within a transaction block.");
			return false;
		}
		
		/* Allow if in transaction - this is the postgres_fdw case */
		return true;
	}
	
	/*
	 * Reject all other sources when trying to set to true
	 */
	GUC_check_errdetail("shardgroup.executing_remote_ddl can only be set by postgres_fdw during DDL replication.");
	return false;
}

/*
 * CopyFileToRemoteFDW
 *		Use FDW routine hook if available; otherwise fallback to direct libpq copy
 */
static void
CopyFileToRemoteFDW(Oid src_serverid, Oid dest_serverid, const char *src_nspname, const char *src_relname, const char *dest_nspname, const char *dest_relname)
{
	FdwRoutine *fdwroutine;
	ForeignServer * server;
	ForeignDataWrapper * fdw;
	
	if (src_serverid == InvalidOid)
	{
		server = GetForeignServer(dest_serverid);
		fdw = GetForeignDataWrapper(server->fdwid);

		/* Get the FDW routine */
		fdwroutine = GetFdwRoutine(fdw->fdwhandler);
	}
	else
	{
		server = GetForeignServer(src_serverid);
		fdw = GetForeignDataWrapper(server->fdwid);

		/* Get the FDW routine */
		fdwroutine = GetFdwRoutine(fdw->fdwhandler);
	}

	if (fdwroutine != NULL && fdwroutine->ExecCopyStream != NULL)
	{
		fdwroutine->ExecCopyStream(src_serverid, dest_serverid, src_nspname, src_relname, dest_nspname, dest_relname);
	}
}

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
	{
		if (stmt->if_not_exists)
		{
			ereport(NOTICE,
					(errmsg("shard group \"%s\" already exists, skipping",
							stmt->sgname)));
			return InvalidObjectAddress;
		}
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("shard group \"%s\" already exists", stmt->sgname)));
	}
	
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
				if (stmt->if_not_exists)
				{
					ereport(NOTICE,
							(errmsg("server \"%s\" is already a member of shard group \"%s\", skipping",
									stmt->servername, stmt->sgname)));
					return InvalidObjectAddress;
				}
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
		
		/* Sync shard group metadata and members info to the new shard member */
		/* Skip this if skip_sync is true to prevent infinite recursion */
		if (!executing_remote_ddl)
		{
			SyncShardGroupMetadataToMember(sgoid, srvoid);
			
			/* Sync existing tables to the new shard member */
			SyncTablesOnNewShardMember(sgoid, srvoid);
			
			/* Notify existing members about the new member */
			NotifyExistingMembersAboutNewMember(sgoid, srvoid);
		}
		
		ObjectAddressSet(address, ShardMemberRelationId, memberoid);
	}
	else if (strcmp(stmt->action, "DROP") == 0)
	{
		Relation	rel;
		HeapTuple	tuple;
		Oid			memberoid;
		HeapTuple	sgtuple;
		Form_pg_shardgroups sgform;
		char	   *sgname;
		List	   *all_members;
		ListCell   *lc;
		StringInfoData drop_ddl;
		
		/* Get the foreign server OID */
		srvoid = get_foreign_server_oid(stmt->servername, false);
		
		/* Get shard group name for DDL generation */
		sgtuple = SearchSysCache1(SHARDGROUPOID, ObjectIdGetDatum(sgoid));
		if (!HeapTupleIsValid(sgtuple))
			elog(ERROR, "cache lookup failed for shard group %u", sgoid);
		sgform = (Form_pg_shardgroups) GETSTRUCT(sgtuple);
		sgname = pstrdup(NameStr(sgform->sgname));
		ReleaseSysCache(sgtuple);
		
		/* Check if any foreign tables in this shard group depend on this member */
		{
			Relation	classrel;
			SysScanDesc scan;
			ScanKeyData key[1];
			HeapTuple	classtuple;
			bool		has_dependencies = false;
			StringInfoData dep_tables;
			
			initStringInfo(&dep_tables);
			
			/* Scan pg_class for all tables in this shard group */
			classrel = table_open(RelationRelationId, AccessShareLock);
			
			ScanKeyInit(&key[0],
						Anum_pg_class_relsgid,
						BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(sgoid));
			
			scan = systable_beginscan(classrel, InvalidOid, false, NULL, 1, key);
			
			while (HeapTupleIsValid(classtuple = systable_getnext(scan)))
			{
				Form_pg_class classForm = (Form_pg_class) GETSTRUCT(classtuple);
				Oid			relid = classForm->oid;
				
				/* Check if this is a foreign table */
				if (classForm->relkind == RELKIND_FOREIGN_TABLE)
				{
					HeapTuple	fttuple;
					Form_pg_foreign_table ftform;
					
					/* Get the foreign table entry to check target server */
					fttuple = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
					if (HeapTupleIsValid(fttuple))
					{
						ftform = (Form_pg_foreign_table) GETSTRUCT(fttuple);
						
						/* Check if this foreign table points to the member being removed */
						if (ftform->ftserver == srvoid)
						{
							char *relname = NameStr(classForm->relname);
							char *nspname = get_namespace_name(classForm->relnamespace);
							
							if (has_dependencies)
								appendStringInfoString(&dep_tables, ", ");
							appendStringInfo(&dep_tables, "%s.%s", nspname, relname);
							has_dependencies = true;
						}
						
						ReleaseSysCache(fttuple);
					}
				}
			}
			
			systable_endscan(scan);
			table_close(classrel, AccessShareLock);
			
			/* If dependencies found, prevent the drop */
			if (has_dependencies)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
						 errmsg("cannot drop shard member \"%s\" from shard group \"%s\"",
								stmt->servername, stmt->sgname),
						 errdetail("Foreign tables depend on this member: %s", dep_tables.data),
						 errhint("Drop or reassign the dependent foreign tables first.")));
			}
			
			pfree(dep_tables.data);
		}
		
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

		if (!executing_remote_ddl)
		{
			/* Get all members before deletion to notify them */
			all_members = get_shardgroup_members(sgoid);
			
			/* Build DDL to drop member from shard group on remote servers */
			initStringInfo(&drop_ddl);
			appendStringInfo(&drop_ddl,
							"ALTER SHARD GROUP %s DROP MEMBER %s",
							quote_identifier(sgname),
							quote_identifier(stmt->servername));
			
			/* Notify all OTHER members to delete this server from their metadata */
			foreach(lc, all_members)
			{
				Oid			member_srvoid = lfirst_oid(lc);
				
				/* Skip the server being removed */
				if (member_srvoid == srvoid)
					continue;
				
				ereport(DEBUG1,
						(errmsg("notifying shard member about DROP MEMBER"),
						errdetail("Executing on server %u: %s",
								member_srvoid, drop_ddl.data)));
				
				ExecuteDDLOnRemoteServer(member_srvoid, drop_ddl.data);
			}
			
			/* Send message to the server being removed to clean up its metadata */
			{
				StringInfoData drop_sg_ddl;
				
				/* 
				* On the removed server, we should drop the shard group itself
				* (not just the membership). This will CASCADE and drop all 
				* tables associated with the shard group on that server.
				*/
				initStringInfo(&drop_sg_ddl);
				appendStringInfo(&drop_sg_ddl,
								"DROP SHARD GROUP IF EXISTS %s CASCADE",
								quote_identifier(sgname));
				
				ereport(DEBUG1,
						(errmsg("instructing removed server to drop shard group"),
						errdetail("Executing on server %u: %s",
								srvoid, drop_sg_ddl.data)));
				
				ExecuteDDLOnRemoteServer(srvoid, drop_sg_ddl.data);
				
				pfree(drop_sg_ddl.data);
			}
		
		}
		
		/* Perform deletion via dependency system */
		{
			ObjectAddress memberAddr;
			ObjectAddressSet(memberAddr, ShardMemberRelationId, memberoid);
			performDeletion(&memberAddr, DROP_RESTRICT, 0);
		}
		
		pfree(sgname);
		pfree(drop_ddl.data);
		
		ObjectAddressSet(address, ShardMemberRelationId, memberoid);
	}
	else if (strcmp(stmt->action, "RESHARD") == 0)
	{
		/* Redistribute partitions across shard members using consistent hashing */
		ReshardShardGroup(sgoid);
		ObjectAddressSet(address, ShardGroupRelationId, sgoid);
	}
	else if (strcmp(stmt->action, "DETACH") == 0)
	{
		/* Move all real tables from specified member to other members */
		/* if it is our local node */
		if (strcmp(stmt->servername, cluster_name) == 0)
		{
			srvoid = InvalidOid; /* will be set below */
		}
		else
			srvoid = get_foreign_server_oid(stmt->servername, false);
		DetachShardMember(sgoid, srvoid);
		ObjectAddressSet(address, ShardMemberRelationId, InvalidOid);
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

/*
 * Helper function to get list of shard members for a shard group
 * Returns a list of server OIDs (as Datum)
 */
List *
get_shardgroup_members(Oid sgid)
{
	List	   *members = NIL;
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[1];
	HeapTuple	tuple;
	
	if (!OidIsValid(sgid))
		return NIL;
	
	rel = table_open(ShardMemberRelationId, AccessShareLock);
	
	ScanKeyInit(&key[0],
				Anum_pg_shardmembers_sgid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(sgid));
	
	scan = systable_beginscan(rel, InvalidOid, false, NULL, 1, key);
	
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_shardmembers memberForm = (Form_pg_shardmembers) GETSTRUCT(tuple);
		members = lappend_oid(members, memberForm->srvid);
	}
	
	systable_endscan(scan);
	table_close(rel, AccessShareLock);
	
	return members;
}

/*
 * ExecuteDDLOnRemoteServer
 *		Execute DDL command on a remote server using FDW callback
 *
 * This is a simplified version of the function in tablecmds.c for use
 * in shardgroupcmds.c. It uses the ExecForeignDDL callback if available.
 */
static void
ExecuteDDLOnRemoteServer(Oid serveroid, const char *sql)
{
	ForeignServer *server;
	ForeignDataWrapper *fdw;
	FdwRoutine *fdwroutine;

	/* Get server and FDW information */
	server = GetForeignServer(serveroid);
	fdw = GetForeignDataWrapper(server->fdwid);

	/* Get the FDW routine */
	fdwroutine = GetFdwRoutine(fdw->fdwhandler);

	/* Check if FDW supports DDL execution */
	if (fdwroutine->ExecForeignDDL != NULL)
	{
		/* Execute DDL using FDW callback - this participates in 2PC */
		fdwroutine->ExecForeignDDL(serveroid, sql);
		
		ereport(DEBUG1,
				(errmsg("executed DDL on foreign server \"%s\" using FDW callback",
						server->servername)));
	}
	else
	{
		/* FDW doesn't support DDL execution, show NOTICE */
		ereport(NOTICE,
				(errmsg("table should be created on shard member \"%s\"",
						server->servername),
				 errdetail("Execute: %s", sql),
				 errhint("The FDW for this server does not support automatic DDL execution. "
						 "Execute this DDL manually on the remote server.")));
	}
}

/*
 * PartitionBoundsSpecToString
 *		Deparse a PartitionBoundSpec into a FOR VALUES clause.
 *
 * Caller must pfree the returned string.
 */
char *
PartitionBoundsSpecToString(PartitionBoundSpec *partbound)
{
	StringInfo partition_bounds;
	char * result;

	Assert(partbound != NULL);

	partition_bounds = makeStringInfo();

	if (partbound->is_default)
	{
		appendStringInfoString(partition_bounds, "DEFAULT");
	}
	else
	{
		switch (partbound->strategy)
		{
			case PARTITION_STRATEGY_HASH:
				appendStringInfo(partition_bounds,
								 "FOR VALUES WITH (modulus %d, remainder %d)",
								 partbound->modulus, partbound->remainder);
				break;

			case PARTITION_STRATEGY_LIST:
				{
					ListCell   *cell;
					const char *sep = "";

					appendStringInfoString(partition_bounds, "FOR VALUES IN (");
					foreach(cell, partbound->listdatums)
					{
						A_Const    *aconst = lfirst(cell);

						appendStringInfoString(partition_bounds, sep);

						if (aconst->isnull)
						{
							appendStringInfoString(partition_bounds, "NULL");
						}
						else
						{
							switch (nodeTag(&aconst->val.node))
							{
								case T_Integer:
									appendStringInfo(partition_bounds, "%d",
													 castNode(Integer, &aconst->val.node)->ival);
									break;
								case T_Float:
									appendStringInfoString(partition_bounds,
														   castNode(Float, &aconst->val.node)->fval);
									break;
								case T_Boolean:
									appendStringInfoString(partition_bounds,
														   castNode(Boolean, &aconst->val.node)->boolval ? "true" : "false");
									break;
								case T_String:
									appendStringInfo(partition_bounds, "'%s'",
													 castNode(String, &aconst->val.node)->sval);
									break;
								case T_BitString:
									appendStringInfo(partition_bounds, "B'%s'",
													 castNode(BitString, &aconst->val.node)->bsval);
									break;
								default:
									elog(ERROR, "unrecognized node type in partition bound: %d",
										 (int) nodeTag(&aconst->val.node));
									break;
							}
						}

						sep = ", ";
					}
					appendStringInfoChar(partition_bounds, ')');
				}
				break;

			case PARTITION_STRATEGY_RANGE:
				{
					char	   *lower_str;
					char	   *upper_str;

					lower_str = deparse_expression((Node *) partbound->lowerdatums,
												   NIL, false, false);
					upper_str = deparse_expression((Node *) partbound->upperdatums,
												   NIL, false, false);
					appendStringInfo(partition_bounds, "FOR VALUES FROM (%s) TO (%s)",
									 lower_str, upper_str);
					pfree(lower_str);
					pfree(upper_str);
				}
				break;

			default:
				elog(ERROR, "unrecognized partition strategy: %d",
					 (int) partbound->strategy);
				break;
		}
	}

	result = pstrdup(partition_bounds->data);
	destroyStringInfo(partition_bounds);
	return result;
}

/*
 * SyncTablesOnNewShardMember
 *		Create tables on a newly added shard member
 *
 * When a new shard member is added to a shard group, this function ensures
 * all existing tables in that shard group are replicated to the new member.
 *
 * For worldwide tables (relreplident != 'd'):
 *   - Create foreign tables on the new member pointing to the local table
 *
 * For distributed tables:
 *   - Create regular tables for non-partitioned distributed tables
 *   - Create partitioned parent tables as regular partitioned tables
 *   - Create partitions as foreign tables pointing to local partitions
 */
static void
SyncTablesOnNewShardMember(Oid sgid, Oid newsrvoid)
{
	Relation	classrel;
	SysScanDesc scan;
	ScanKeyData key[1];
	HeapTuple	tuple;
	List	   *processed_tables = NIL;
	ForeignServer *server;
	extern char *cluster_name;
	char	   *sgname;
	HeapTuple	sgtuple;
	Form_pg_shardgroups sgform;
	bool	first_scan = true;

	/* Check if cluster_name is set */
	if (!cluster_name || cluster_name[0] == '\0')
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("cluster_name is not set, cannot sync tables to new shard member"),
				 errhint("Set cluster_name in postgresql.conf to enable automatic table synchronization.")));
		return;
	}
	
	/* Get the shard group name */
	sgtuple = SearchSysCache1(SHARDGROUPOID, ObjectIdGetDatum(sgid));
	if (!HeapTupleIsValid(sgtuple))
		elog(ERROR, "cache lookup failed for shard group %u", sgid);
	
	sgform = (Form_pg_shardgroups) GETSTRUCT(sgtuple);
	sgname = pstrdup(NameStr(sgform->sgname));
	ReleaseSysCache(sgtuple);

	/* Get the foreign server name for logging */
	server = GetForeignServer(newsrvoid);

	ereport(DEBUG1,
			(errmsg("syncing tables in shard group to new member \"%s\"",
					server->servername)));

	/* Scan pg_class for all tables with this shard group */
	classrel = table_open(RelationRelationId, AccessShareLock);
scan_again:
	ScanKeyInit(&key[0],
				Anum_pg_class_relsgid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(sgid));

	scan = systable_beginscan(classrel, InvalidOid, false, NULL, 1, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);
		Oid			relid = classForm->oid;
		Oid			parentRelid;
		Relation	rel;
		char	   *relname;
		char	   *nspname;
		TupleDesc	tupdesc;
		StringInfoData ddl;
		int			i;
		bool		is_partition;
		bool		is_partitioned;
		bool		is_foreign_table;
		PartitionBoundSpec *partbound = NULL;

		/* Skip non-table relations */
		if (first_scan)
		{
			/* On the first scan, we only want to process partitioned tables, 
			  so that we create the parent tables before the partitions. 
			  On the second scan, we process the partitions. */
			if (classForm->relkind != RELKIND_PARTITIONED_TABLE)
				continue;
		}
		
		if (classForm->relkind != RELKIND_RELATION &&
			classForm->relkind != RELKIND_PARTITIONED_TABLE &&
			classForm->relkind != RELKIND_FOREIGN_TABLE)
			continue;

		/* Skip if already processed (shouldn't happen, but be safe) */
		if (list_member_oid(processed_tables, relid))
			continue;

		/* Open the relation */
		rel = table_open(relid, AccessShareLock);
		
		relname = RelationGetRelationName(rel);
		nspname = get_namespace_name(RelationGetNamespace(rel));
		tupdesc = RelationGetDescr(rel);

		/* Determine table type */
		is_partition = (classForm->relispartition);
		is_partitioned = (classForm->relkind == RELKIND_PARTITIONED_TABLE);
		is_foreign_table = (classForm->relkind == RELKIND_FOREIGN_TABLE);

		initStringInfo(&ddl);

		if (is_partition)
		{
			char * cluster_ = NULL;
			Relation	parentRel;

			/* partition of distributed table on foreign server */
			if (is_foreign_table)
			{
				/* For foreign tables: create foreign table on new member
				 * pointing to the same foreign server as the source table
				 */
				HeapTuple	fttuple;
				Form_pg_foreign_table ftform;
				ForeignServer *target_server;
				
				/* Look up the foreign table entry to get the target server */
				fttuple = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
				if (!HeapTupleIsValid(fttuple))
					elog(ERROR, "cache lookup failed for foreign table %u", relid);
				
				ftform = (Form_pg_foreign_table) GETSTRUCT(fttuple);
				target_server = GetForeignServer(ftform->ftserver);	
				pg_sprintf(cluster_, "%s", target_server->servername);

				ReleaseSysCache(fttuple);
			}
			else
			{
				/* For partitions of distributed tables on our local server */
				cluster_ = cluster_name;
			}
			
			/*
			 * For partitions of distributed tables: create foreign table
			 * on new member pointing to the partition on the local server
			 */
			appendStringInfo(&ddl, "CREATE FOREIGN TABLE IF NOT EXISTS %s.%s ",
							 quote_identifier(nspname),
							 quote_identifier(relname));
			
			/* add PARTITION OF clause */
			parentRelid = get_parent_rel_oid(relid);
			
			parentRel = table_open(parentRelid, AccessShareLock);
			
			appendStringInfo(&ddl, "PARTITION OF %s.%s ",
								quote_identifier(get_namespace_name(RelationGetNamespace(parentRel))),
								quote_identifier(RelationGetRelationName(parentRel)));
						
			partbound = RelationGetPartitionBoundSpec(parentRel, relid);
			
			{
				char *partition_bounds = PartitionBoundsSpecToString(partbound);

				appendStringInfo(&ddl, "%s ", partition_bounds);
				pfree(partition_bounds);
			}
	
			table_close(parentRel, AccessShareLock);
							 
			appendStringInfo(&ddl, "SERVER %s ",
					quote_identifier(cluster_));

			ereport(DEBUG1,
					(errmsg("creating foreign table for partition \"%s.%s\" on shard member \"%s\"",
							nspname, relname, server->servername)));
		}
		else if (is_partitioned)
		{
			PartitionKey partkey;
			Assert(!is_foreign_table);
			
			partkey = RelationGetPartitionKey(rel);
			
			/*
			 * For regular distributed tables (including partitioned tables):
			 * create actual table on new member
			 */
			appendStringInfo(&ddl, "CREATE TABLE IF NOT EXISTS %s.%s (",
							 quote_identifier(nspname),
							 quote_identifier(relname));

			/* Add column definitions */
			for (i = 0; i < tupdesc->natts; i++)
			{
				Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

				if (attr->attisdropped)
					continue;

				if (i > 0)
					appendStringInfoString(&ddl, ", ");

				appendStringInfo(&ddl, "%s %s",
								 quote_identifier(NameStr(attr->attname)),
								 format_type_with_typemod(attr->atttypid, attr->atttypmod));

				if (attr->attnotnull)
					appendStringInfoString(&ddl, " NOT NULL");
			}

			appendStringInfoChar(&ddl, ')');
			
			if (partkey != NULL)
			{
				char *partkey_str = pg_get_partkeydef_columns(relid, false);
				
				appendStringInfo(&ddl, " DISTRIBUTED BY %s (%s)",
									partkey->strategy == PARTITION_STRATEGY_HASH ? "HASH" :
									partkey->strategy == PARTITION_STRATEGY_LIST ? "LIST" :
									partkey->strategy == PARTITION_STRATEGY_RANGE ? "RANGE" : "UNKNOWN",
									partkey_str);
				pfree(partkey_str);
			}
			
			/* Add SHARD GROUP clause */
			appendStringInfo(&ddl, " SHARD GROUP %s", quote_identifier(sgname));

			ereport(DEBUG1,
					(errmsg("creating partitioned table \"%s.%s\" on shard member \"%s\"",
							nspname, relname, server->servername)));
		}
		else
		{
			/*
			 * For foreign tables: create foreign table on new member
			 * pointing to the same foreign server as the source table
			 */
			HeapTuple	fttuple;
			Form_pg_foreign_table ftform;
			ForeignServer *target_server;
			char * cluster_ = NULL;
			
			Assert(!is_partition && !is_partitioned);
			
			if (is_foreign_table)
			{
				/* Look up the foreign table entry to get the target server */
				fttuple = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
				if (!HeapTupleIsValid(fttuple))
					elog(ERROR, "cache lookup failed for foreign table %u", relid);
				
				ftform = (Form_pg_foreign_table) GETSTRUCT(fttuple);
				target_server = GetForeignServer(ftform->ftserver);
				pg_sprintf(cluster_, "%s", target_server->servername);
				
				ReleaseSysCache(fttuple);
			}
			else
			{
				/* For worldwide tables on our local server */
				cluster_ = cluster_name;
			}
			
			appendStringInfo(&ddl, "CREATE FOREIGN TABLE IF NOT EXISTS %s.%s (",
							 quote_identifier(nspname),
							 quote_identifier(relname));

			/* Add column definitions */
			for (i = 0; i < tupdesc->natts; i++)
			{
				Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

				if (attr->attisdropped)
					continue;

				if (i > 0)
					appendStringInfoString(&ddl, ", ");

				appendStringInfo(&ddl, "%s %s",
								 quote_identifier(NameStr(attr->attname)),
								 format_type_with_typemod(attr->atttypid, attr->atttypmod));

				if (attr->attnotnull)
					appendStringInfoString(&ddl, " NOT NULL");
			}

			appendStringInfo(&ddl, ") SERVER %s",
							 quote_identifier(cluster_));
							 
			appendStringInfo(&ddl, " SHARD GROUP %s", quote_identifier(sgname));

			ereport(DEBUG1,
					(errmsg("creating foreign table \"%s.%s\" on shard member \"%s\" pointing to server \"%s\"",
							nspname, relname, server->servername, cluster_)));
		}

		/* Execute the DDL on the new shard member */
		ExecuteDDLOnRemoteServer(newsrvoid, ddl.data);

		/* Mark as processed */
		processed_tables = lappend_oid(processed_tables, relid);

		/* Cleanup */
		pfree(ddl.data);
		pfree(nspname);
		table_close(rel, AccessShareLock);
	}

	systable_endscan(scan);
	
	/* If we just processed partitioned tables, we need to scan again to process partitions */
	if (first_scan)
	{
		first_scan = false;
		goto scan_again;
	}
	
	table_close(classrel, AccessShareLock);
	
	if (list_length(processed_tables) > 0)
	{
		ereport(DEBUG1,
				(errmsg("successfully synced %d table(s) to shard member \"%s\"",
						list_length(processed_tables), server->servername)));
	}
	else
	{
		ereport(DEBUG1,
				(errmsg("no tables found in shard group to sync to shard member \"%s\"",
						server->servername)));
	}

	list_free(processed_tables);
	pfree(sgname);
}

/*
 * SyncShardGroupMetadataToMember
 *		Sync shard group and member information to a newly added shard member
 *
 * When a new shard member is added to a shard group, this function ensures
 * the shard group metadata and all member information is replicated to the 
 * new member, so it knows the full layout of the shard group.
 *
 * This creates the shard group if it doesn't exist on the remote server,
 * and adds all shard members (including itself) to pg_shardmembers on the
 * remote server.
 */
static void
SyncShardGroupMetadataToMember(Oid sgid, Oid newsrvoid)
{
	HeapTuple	sgtuple;
	Form_pg_shardgroups sgform;
	char	   *sgname;
	Relation	memberrel;
	SysScanDesc scan;
	ScanKeyData key[1];
	HeapTuple	tuple;
	StringInfoData ddl;
	ForeignServer *server;
	List	   *member_servers = NIL;
	ListCell   *lc;

	/* Get the shard group name and details */
	sgtuple = SearchSysCache1(SHARDGROUPOID, ObjectIdGetDatum(sgid));
	if (!HeapTupleIsValid(sgtuple))
		elog(ERROR, "cache lookup failed for shard group %u", sgid);
	
	sgform = (Form_pg_shardgroups) GETSTRUCT(sgtuple);
	sgname = NameStr(sgform->sgname);
	
	/* Get the foreign server name for logging */
	server = GetForeignServer(newsrvoid);
	
	ereport(DEBUG1,
			(errmsg("syncing shard group metadata \"%s\" to member \"%s\"",
					sgname, server->servername)));
	
	initStringInfo(&ddl);
	
	/*
	 * Step 1: Create the shard group on the remote server if it doesn't exist
	 * Use proper DDL command instead of direct catalog insert
	 */
	appendStringInfo(&ddl, "CREATE SHARD GROUP IF NOT EXISTS %s;",
					 quote_identifier(sgname));
	
	ExecuteDDLOnRemoteServer(newsrvoid, ddl.data);
	
	/*
	 * Step 2: Get all current shard members in this shard group
	 */
	memberrel = table_open(ShardMemberRelationId, AccessShareLock);
	
	ScanKeyInit(&key[0],
				Anum_pg_shardmembers_sgid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(sgid));
	
	scan = systable_beginscan(memberrel, ShardMemberSgidSrvidIndexId, true,
							  NULL, 1, key);
	
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_shardmembers smform = (Form_pg_shardmembers) GETSTRUCT(tuple);
		ForeignServer *member_server = GetForeignServer(smform->srvid);
		
		/* Store member information */
		member_servers = lappend(member_servers, member_server);
	}
	
	systable_endscan(scan);
	table_close(memberrel, AccessShareLock);
	
	/*
	 * Step 3: Sync all shard members to the new member's pg_shardmembers
	 * Use proper DDL command instead of direct catalog insert
	 * 
	 * IMPORTANT: We use the internal skip_sync option here to prevent
	 * the remote server from triggering SyncShardGroupMetadataToMember again,
	 * which would cause infinite distributed recursion.
	 */
	foreach(lc, member_servers)
	{
		ForeignServer *member_server = (ForeignServer *) lfirst(lc);
		
		/* Reset DDL buffer */
		resetStringInfo(&ddl);
		
		/* 
		 * Use ALTER SHARD GROUP ADD MEMBER command with skip_sync option
		 * to prevent recursion. The skip_sync option is internal-only
		 * and gets passed through the WITH clause syntax.
		 */
		appendStringInfo(&ddl,
						 "ALTER SHARD GROUP %s ADD MEMBER IF NOT EXISTS %s;",
						 quote_identifier(sgname),
						 quote_identifier(member_server->servername));
		
		ExecuteDDLOnRemoteServer(newsrvoid, ddl.data);
		
		ereport(DEBUG1,
				(errmsg("synced shard member \"%s\" info to shard member \"%s\"",
						member_server->servername, server->servername)));
	}

	/* Add itself as a member */
	{
		resetStringInfo(&ddl);
		appendStringInfo(&ddl,
						 "ALTER SHARD GROUP %s ADD MEMBER IF NOT EXISTS %s;",
						 quote_identifier(sgname),
						 quote_identifier(cluster_name));
		ExecuteDDLOnRemoteServer(newsrvoid, ddl.data);
		
		ereport(DEBUG1,
				(errmsg("added self as shard member \"%s\" to shard group \"%s\" on member \"%s\"",
						cluster_name, sgname, server->servername)));
	}
	
	ereport(DEBUG1,
			(errmsg("successfully synced shard group \"%s\" with %d member(s) to shard member \"%s\"",
					sgname, list_length(member_servers), server->servername)));
	
	/* Cleanup */
	pfree(ddl.data);
	list_free(member_servers);
	ReleaseSysCache(sgtuple);
}

/*
 * NotifyExistingMembersAboutNewMember
 *		Notify all existing shard members about a newly added shard member
 *
 * When a new shard member is added to a shard group, this function informs
 * all existing shard members about the new member so they can update their
 * local catalogs to include information about the new member.
 *
 * This complements SyncShardGroupMetadataToMember which syncs TO the new member.
 * This function syncs FROM the new member information TO all existing members.
 */
static void
NotifyExistingMembersAboutNewMember(Oid sgid, Oid newsrvoid)
{
	Relation	memberrel;
	SysScanDesc scan;
	ScanKeyData key[1];
	HeapTuple	tuple;
	ForeignServer *newserver;
	StringInfoData ddl;
	HeapTuple	sgtuple;
	Form_pg_shardgroups sgform;
	char	   *sgname;
	
	/* Get the shard group name */
	sgtuple = SearchSysCache1(SHARDGROUPOID, ObjectIdGetDatum(sgid));
	if (!HeapTupleIsValid(sgtuple))
		elog(ERROR, "cache lookup failed for shard group %u", sgid);
	
	sgform = (Form_pg_shardgroups) GETSTRUCT(sgtuple);
	sgname = NameStr(sgform->sgname);
	
	/* Get the new server information */
	newserver = GetForeignServer(newsrvoid);
	
	ereport(DEBUG1,
			(errmsg("notifying existing members about new shard member \"%s\" in shard group \"%s\"",
					newserver->servername, sgname)));
	
	initStringInfo(&ddl);
	
	/*
	 * Iterate through all existing shard members (excluding the new one)
	 * and notify them about the new member
	 */
	memberrel = table_open(ShardMemberRelationId, AccessShareLock);
	
	ScanKeyInit(&key[0],
				Anum_pg_shardmembers_sgid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(sgid));
	
	scan = systable_beginscan(memberrel, ShardMemberSgidSrvidIndexId, true,
							  NULL, 1, key);
	
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_shardmembers smform = (Form_pg_shardmembers) GETSTRUCT(tuple);
		ForeignServer *existing_server;
		
		/* Skip the newly added member itself */
		if (smform->srvid == newsrvoid)
			continue;
		
		existing_server = GetForeignServer(smform->srvid);
		
		/* Reset DDL buffer */
		resetStringInfo(&ddl);
		
		/*
		 * Execute ALTER SHARD GROUP ADD MEMBER on the existing member
		 * to add information about the new member to its catalog.
		 * 
		 * Use skip_sync option to prevent the existing member from
		 * triggering its own sync operations, which would cause
		 * unnecessary distributed operations.
		 */
		appendStringInfo(&ddl,
						 "ALTER SHARD GROUP %s ADD MEMBER IF NOT EXISTS %s;",
						 quote_identifier(sgname),
						 quote_identifier(newserver->servername));
		
		ExecuteDDLOnRemoteServer(smform->srvid, ddl.data);
		
		ereport(DEBUG1,
				(errmsg("notified shard member \"%s\" about new member \"%s\"",
						existing_server->servername, newserver->servername)));
	}
	
	systable_endscan(scan);
	table_close(memberrel, AccessShareLock);
	
	/* Cleanup */
	pfree(ddl.data);
	ReleaseSysCache(sgtuple);
	
	ereport(DEBUG1,
			(errmsg("successfully notified existing members about new shard member \"%s\"",
					newserver->servername)));
}

/*
 * hash_string_to_uint32
 *		Simple hash function to convert a string to a 32-bit unsigned integer
 *
 */
static uint32
hash_string_to_uint32(const char *str)
{	
	return string_hash(str, strlen(str) + 1);
}

/*
 * GetPartitionTargetMember
 *		Determine which shard member should host a partition using consistent hashing
 *
 * Uses ring hashing: each member is placed on a hash ring multiple times
 * (virtual nodes), and the partition is assigned to the member whose
 * virtual node is closest in the ring.
 *
 * If exclude_member is valid, that member will be excluded from consideration,
 * which is useful during DETACH operations to ensure partitions aren't assigned
 * to the member being detached.
 *
 * Returns the OID of the foreign server that should host the partition.
 */
Oid
GetPartitionTargetMember(Oid sgid, const char *partition_name, Oid *exclude_member, bool *found)
{
	Relation	memberrel;
	SysScanDesc scan;
	ScanKeyData key[1];
	HeapTuple	tuple;
	List	   *members = NIL;
	ListCell   *lc;
	uint32		partition_hash;
	uint32		min_distance = UINT32_MAX;
	Oid			target_member = InvalidOid;
	
	*found = false;
	
	/* Get all shard members */
	memberrel = table_open(ShardMemberRelationId, AccessShareLock);
	
	ScanKeyInit(&key[0],
				Anum_pg_shardmembers_sgid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(sgid));
	
	scan = systable_beginscan(memberrel, ShardMemberSgidSrvidIndexId, true,
							  NULL, 1, key);
	
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_shardmembers smform = (Form_pg_shardmembers) GETSTRUCT(tuple);
		members = lappend_oid(members, smform->srvid);
	}
	
	/* add itself as a member (local server is represented by InvalidOid) */
	members = lappend_oid(members, InvalidOid);
	
	systable_endscan(scan);
	table_close(memberrel, AccessShareLock);
	
	/* Hash the partition name */
	partition_hash = hash_string_to_uint32(partition_name);
	
	/* Find the closest virtual node using ring hashing */
	foreach(lc, members)
	{
		Oid			serveroid = lfirst_oid(lc);
		ForeignServer *server;
		int			i;
		char * server_name;
		
		/* Skip the excluded member if specified */
		if (exclude_member && serveroid == *exclude_member)
			continue;
			
		*found = true;

		if (serveroid == InvalidOid)
		{
			/* Local server - use cluster_name */
			extern char *cluster_name;
			if (!cluster_name || cluster_name[0] == '\0')
				elog(ERROR, "cluster_name is not set, cannot use local server as shard member");
			server_name = cluster_name;
		}
		else
		{
			/* Get foreign server name */
			server = GetForeignServer(serveroid);
			server_name = server->servername;
		}
		
		/* Create multiple virtual nodes for this member */
		for (i = 0; i < VIRTUAL_NODES_PER_MEMBER; i++)
		{
			char		vnode_key[VNODE_KEY_MAX_LEN];
			uint32		vnode_hash;
			uint32		distance;
			
			/* 
			 * Create virtual node identifier using underscore separator
			 * to avoid conflicts with potential colons in server names
			 */
			snprintf(vnode_key, sizeof(vnode_key), "%s_%d", 
					 server_name, i);
			
			vnode_hash = hash_string_to_uint32(vnode_key);
			
			/* 
			 * Calculate distance on the ring (clockwise from partition to vnode)
			 * Handle wraparound correctly to avoid overflow.
			 * Use uint64_t for intermediate calculation to prevent overflow.
			 */
			if (vnode_hash >= partition_hash)
				distance = vnode_hash - partition_hash;
			else
			{
				/* Calculate wraparound distance using uint64_t to avoid overflow */
				uint64_t wrap_distance = (uint64_t)UINT32_MAX - partition_hash + vnode_hash + 1;
				distance = (uint32_t)wrap_distance;
			}
			
			/* Track the closest virtual node */
			if (distance < min_distance)
			{
				min_distance = distance;
				target_member = serveroid;
			}
		}
	}
	
	list_free(members);
	
	return target_member;
}

/*
 * GetShardGroupPartitions
 *		Get all partition OIDs that belong to tables in the shard group
 *
 * Returns a list of partition relation OIDs
 */
static List *
GetShardGroupPartitions(Oid sgid)
{
	Relation	classrel;
	SysScanDesc scan;
	HeapTuple	tuple;
	List	   *partitions = NIL;
	
	/* Scan pg_class for partitions in this shard group */
	classrel = table_open(RelationRelationId, AccessShareLock);
	scan = systable_beginscan(classrel, InvalidOid, false, NULL, 0, NULL);
	
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_class classform = (Form_pg_class) GETSTRUCT(tuple);
		
		/* Only interested in partition tables */
		if (classform->relkind != RELKIND_RELATION &&
			classform->relkind != RELKIND_FOREIGN_TABLE)
			continue;
		
		/* Check if this relation belongs to our shard group */
		if (classform->relsgid == sgid)
		{
			/* Check if this is a partition (has a parent) */
			if (classform->relispartition)
			{
				partitions = lappend_oid(partitions, classform->oid);
			}
		}
	}
	
	systable_endscan(scan);
	table_close(classrel, AccessShareLock);
	
	return partitions;
}

/*
 * FindPartitionHostMember
 *		Find which shard member currently hosts the real table for a partition
 *
 * Returns the member OID that hosts the real table for the partition:
 * - If the partition is a real table on the local server, returns the server OID
 *   representing the local cluster in the shard group
 * - If the partition is a foreign table on the local server, returns the server OID
 *   that the foreign table points to (where the real table is hosted)
 * - Returns InvalidOid if unable to determine the host
 */
static Oid
FindPartitionHostMember(Oid partitionOid, Oid sgid)
{
	Relation	rel;
	Oid			result = InvalidOid;
	extern char *cluster_name;
	
	/* Open the relation */
	rel = table_open(partitionOid, AccessShareLock);
	
	if (rel->rd_rel->relkind == RELKIND_RELATION)
	{
		/* This is a real table on the local server */
		result = InvalidOid; /* Default to InvalidOid */
	}
	else if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
	{
		/* This is a foreign table - get the server it points to */
		ForeignTable *ft = GetForeignTable(partitionOid);
		result = ft->serverid;
	}
	
	table_close(rel, AccessShareLock);
	
	return result;
}

/*
 * MigratePartitionToMember
 *		Migrate a partition from one member to another
 *
 * Steps:
 * 1. Copy data from source to destination using foreign table access
 * 2. Drop the real table on source
 * 3. Create foreign table on source pointing to destination
 * 4. Update foreign tables on other members to point to destination
 */
static void
MigratePartitionToMember(Oid partitionOid, Oid fromServer, Oid toServer, Oid sgid)
{
	Relation	rel;
	char	   *relname;
	char	   *nspname;
	char	   *sgname;
	char*			toServerName;
	char*			fromServerName;
	StringInfoData ddl;
	Relation	parent;
	char	   *parentname;
	char	   *parentnspname;
	List	   *members;
	ListCell   *lc;
	extern char *cluster_name;
	PartitionBoundSpec * partbound;
	char	   *partition_bounds;
	HeapTuple	sgtuple;
	Form_pg_shardgroups sgform;
	
	/* Get the shard group name */
	sgtuple = SearchSysCache1(SHARDGROUPOID, ObjectIdGetDatum(sgid));
	if (!HeapTupleIsValid(sgtuple))
		elog(ERROR, "cache lookup failed for shard group %u", sgid);
	
	sgform = (Form_pg_shardgroups) GETSTRUCT(sgtuple);
	sgname = pstrdup(NameStr(sgform->sgname));
	ReleaseSysCache(sgtuple);
	
	/* Open the partition relation */
	rel = table_open(partitionOid, AccessShareLock);
	relname = pstrdup(RelationGetRelationName(rel));
	nspname = get_namespace_name(RelationGetNamespace(rel));
	
	/* Get parent table info */
	if (rel->rd_rel->relispartition)
	{
		Oid parentOid = get_partition_parent(partitionOid, false);
		parent = table_open(parentOid, AccessShareLock);
		parentname = pstrdup(RelationGetRelationName(parent));
		parentnspname = get_namespace_name(RelationGetNamespace(parent));
		partbound = RelationGetPartitionBoundSpec(parent, partitionOid);
		table_close(parent, AccessShareLock);
	}
	else
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relation \"%s\" is not a partition", relname)));
	}
	
	table_close(rel, AccessShareLock);
	
	if (toServer == InvalidOid)
	{
		toServerName = cluster_name;
	}
	else
	{
		ForeignServer *toSrv = GetForeignServer(toServer);
		toServerName = toSrv->servername;
	}
	
	if (fromServer == InvalidOid)
	{
		fromServerName = cluster_name;
	}
	else
	{
		ForeignServer *fromSrv = GetForeignServer(fromServer);
		fromServerName = fromSrv->servername;
	}
	
	initStringInfo(&ddl);
	
	ereport(NOTICE,
			(errmsg("migrating partition \"%s.%s\" from \"%s\" to \"%s\"",
					nspname, relname, fromServerName, toServerName)));
	
	/*
	 * Step 1: Create real table on destination and stream data
	 */
	partition_bounds = PartitionBoundsSpecToString(partbound);
	
	
	if (toServer != InvalidOid)
	{
		char * copy_relname;
		/* Detach partition from parent to allow creating real table */
		resetStringInfo(&ddl);
		appendStringInfo(&ddl,
						"ALTER TABLE %s.%s DETACH PARTITION %s.%s;",
						quote_identifier(parentnspname),
						quote_identifier(parentname),
						quote_identifier(nspname),
						quote_identifier(relname));
		ExecuteDDLOnRemoteServer(toServer, ddl.data);
		
		/* Create the partition structure on destination */
		/* Note: Partitions inherit relsgid from parent, so no SHARD GROUP clause needed */
		resetStringInfo(&ddl);

		appendStringInfo(&ddl, 
						"CREATE TABLE IF NOT EXISTS %s.%s_temp PARTITION OF %s.%s %s WITH (no_rel_sync = true);",
						quote_identifier(nspname),
						quote_identifier(relname),
						quote_identifier(parentnspname),
						quote_identifier(parentname),
						partition_bounds);
		ExecuteDDLOnRemoteServer(toServer, ddl.data);
			
		/* Copy data */
		copy_relname = psprintf("%s_temp", quote_identifier(relname));
		CopyFileToRemoteFDW(fromServer, toServer, nspname, relname, nspname, copy_relname);
		
		/* Drop temporary foreign table */
		resetStringInfo(&ddl);
		appendStringInfo(&ddl, "DROP FOREIGN TABLE %s.%s;",
						quote_identifier(nspname),
						quote_identifier(relname));
		
		ExecuteDDLOnRemoteServer(toServer, ddl.data);
		
		/* Rename temp table to actual partition name */
		resetStringInfo(&ddl);
		appendStringInfo(&ddl,
						"ALTER TABLE %s.%s_temp RENAME TO %s;",
						quote_identifier(nspname),
						quote_identifier(relname),
						quote_identifier(relname));
		
		ExecuteDDLOnRemoteServer(toServer, ddl.data);	
	}
	else
	{
		char * copy_relname;
		/* use SPI */
		SPI_connect();
		
		/* Detach partition from parent to allow creating real table */
		resetStringInfo(&ddl);
		appendStringInfo(&ddl,
						"ALTER TABLE %s.%s DETACH PARTITION %s.%s;",
						quote_identifier(parentnspname),
						quote_identifier(parentname),
						quote_identifier(nspname),
						quote_identifier(relname));
		SPI_execute(ddl.data, false, 0);
		
		/* Create the partition structure on destination */
		/* Note: Partitions inherit relsgid from parent, so no SHARD GROUP clause needed */
		resetStringInfo(&ddl);
		
		executing_remote_ddl = true; /* flag to indicate we're want it to execute only on local server and not propagate to others */
		appendStringInfo(&ddl, 
						"CREATE TABLE IF NOT EXISTS %s.%s_temp PARTITION OF %s.%s %s WITH (no_rel_sync = true);",
						quote_identifier(nspname),
						quote_identifier(relname),
						quote_identifier(parentnspname),
						quote_identifier(parentname),
						partition_bounds);
		if (SPI_execute(ddl.data, false, 0) < 0)
			elog(ERROR, "SPI_execute failed: %s", ddl.data);

		/* Copy data */
		copy_relname = psprintf("%s_temp", quote_identifier(relname));
		CopyFileToRemoteFDW(fromServer, toServer, nspname, relname, nspname, copy_relname);
		
		/* Drop temporary foreign table */
		resetStringInfo(&ddl);
		appendStringInfo(&ddl, "DROP FOREIGN TABLE %s.%s;",
						quote_identifier(nspname),
						quote_identifier(relname));
		
		SPI_execute(ddl.data, false, 0);
		
		executing_remote_ddl = false; /* reset flag */
		
		/* Rename temp table to actual partition name */
		resetStringInfo(&ddl);
		appendStringInfo(&ddl,
						"ALTER TABLE %s.%s_temp RENAME TO %s;",
						quote_identifier(nspname),
						quote_identifier(relname),
						quote_identifier(relname));
		
		SPI_execute(ddl.data, false, 0);
		
		SPI_finish();
	}
	
	/*
	 * Step 2: Drop real table on source and create foreign table
	 */
	if (fromServer != InvalidOid)
	{
		resetStringInfo(&ddl);
		appendStringInfo(&ddl, "DROP TABLE IF EXISTS %s.%s CASCADE;",
						quote_identifier(nspname),
						quote_identifier(relname));
		
		ExecuteDDLOnRemoteServer(fromServer, ddl.data);
		
		/* Create foreign table on source pointing to destination */
		resetStringInfo(&ddl);
		appendStringInfo(&ddl, 
						"CREATE FOREIGN TABLE IF NOT EXISTS %s.%s PARTITION OF %s.%s %s SERVER %s;",
						quote_identifier(nspname),
						quote_identifier(relname),
						quote_identifier(parentnspname),
						quote_identifier(parentname),
						partition_bounds,
						quote_identifier(toServerName));
		
		ExecuteDDLOnRemoteServer(fromServer, ddl.data);
	}
	else
	{
		/* use SPI */
		SPI_connect();
		
		executing_remote_ddl = true; /* flag to indicate we're want it to execute only on local server and not propagate to others */
		resetStringInfo(&ddl);
		appendStringInfo(&ddl, "DROP TABLE IF EXISTS %s.%s CASCADE;",
						quote_identifier(nspname),
						quote_identifier(relname));

		SPI_execute(ddl.data, false, 0);
		executing_remote_ddl = false; 

		resetStringInfo(&ddl);
		appendStringInfo(&ddl, 
						"CREATE FOREIGN TABLE IF NOT EXISTS %s.%s PARTITION OF %s.%s %s SERVER %s;",
						quote_identifier(nspname),
						quote_identifier(relname),
						quote_identifier(parentnspname),
						quote_identifier(parentname),
						partition_bounds,
						quote_identifier(toServerName));
						
		SPI_execute(ddl.data, false, 0);
		
		SPI_finish();	
	}

	/*
	 * Step 3: Update foreign tables on all other members
	 */
	members = get_shardgroup_members(sgid);
	members = lappend_oid(members, InvalidOid); /* include local server */
	foreach(lc, members)
	{
		Oid serveroid = lfirst_oid(lc);
		
		/* Skip source and destination */
		if (serveroid == fromServer || serveroid == toServer)
			continue;
		
		if (serveroid == InvalidOid)
		{
			/* use SPI */
			SPI_connect();
			
			/* Drop and recreate foreign table pointing to destination */
			resetStringInfo(&ddl);
			
			executing_remote_ddl = true; /* flag to indicate we're want it to execute only on local server and not propagate to others */
			appendStringInfo(&ddl, "DROP FOREIGN TABLE IF EXISTS %s.%s CASCADE;",
							 quote_identifier(nspname),
							 quote_identifier(relname));
			
			SPI_execute(ddl.data, false, 0);
			
			resetStringInfo(&ddl);
			appendStringInfo(&ddl, 
							 "CREATE FOREIGN TABLE IF NOT EXISTS %s.%s PARTITION OF %s.%s %s SERVER %s;",
							 quote_identifier(nspname),
							 quote_identifier(relname),
							 quote_identifier(parentnspname),
							 quote_identifier(parentname),
							 partition_bounds,
							 quote_identifier(toServerName));
		
			SPI_execute(ddl.data, false, 0);
			
			executing_remote_ddl = false; /* reset flag */
			
			SPI_finish();
		}
		else
		{
			/* Drop and recreate foreign table pointing to destination */
			resetStringInfo(&ddl);
			appendStringInfo(&ddl, "DROP FOREIGN TABLE IF EXISTS %s.%s CASCADE;",
							quote_identifier(nspname),
							quote_identifier(relname));
			
			ExecuteDDLOnRemoteServer(serveroid, ddl.data);
			
			resetStringInfo(&ddl);
			appendStringInfo(&ddl, 
							"CREATE FOREIGN TABLE IF NOT EXISTS %s.%s PARTITION OF %s.%s %s SERVER %s;",
							quote_identifier(nspname),
							quote_identifier(relname),
							quote_identifier(parentnspname),
							quote_identifier(parentname),
							partition_bounds,
							quote_identifier(toServerName));
		
			
			ExecuteDDLOnRemoteServer(serveroid, ddl.data);
		}
	}
	
	list_free(members);
	pfree(ddl.data);
	pfree(relname);
	pfree(nspname);
	pfree(parentname);
	pfree(partition_bounds);
	pfree(parentnspname);
	pfree(sgname);
	
	ereport(DEBUG1,
			(errmsg("successfully migrated partition \"%s.%s\" to \"%s\"",
					nspname, relname, toServerName)));
}

/*
 * ReshardShardGroup
 *		Redistribute partitions across shard members using consistent hashing
 *
 * This command moves partitions that are currently real tables to their
 * correct positions according to consistent hashing. Partitions that are
 * already on the correct member are left alone.
 */
static void
ReshardShardGroup(Oid sgid)
{
	List	   *partitions;
	ListCell   *lc;
	int			moved_count = 0;
	int			total_count = 0;
	
	ereport(NOTICE,
			(errmsg("resharding shard group partitions using consistent hashing")));
	
	/* Get all partitions in the shard group */
	partitions = GetShardGroupPartitions(sgid);
	
	if (partitions == NIL)
	{
		ereport(NOTICE,
				(errmsg("no partitions found in shard group")));
		return;
	}
	
	/* For each partition, check if it's on the correct member */
	foreach(lc, partitions)
	{
		Oid			partitionOid = lfirst_oid(lc);
		Relation	rel;
		char	   *relname;
		Oid			currentHost;
		Oid			targetHost;
		bool		found;
		
		total_count++;
		
		/* Get partition name */
		rel = table_open(partitionOid, AccessShareLock);
		relname = pstrdup(RelationGetRelationName(rel));
		table_close(rel, AccessShareLock);
		
		/* Find current host */
		currentHost = FindPartitionHostMember(partitionOid, sgid);
		
		/* Determine target host using consistent hashing */
		targetHost = GetPartitionTargetMember(sgid, relname, NULL, &found);
		
		if (!found)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not determine target member for partition \"%s\"", relname)));
		}
		
		/* If partition is on wrong member, migrate it */
		if (currentHost != targetHost)
		{
			MigratePartitionToMember(partitionOid, currentHost, targetHost, sgid);
			moved_count++;
		}
		
		pfree(relname);
	}
	
	list_free(partitions);
	
	ereport(WARNING,
			(errmsg("reshard complete: %d of %d partitions migrated",
					moved_count, total_count)));
}

/*
 * DetachShardMember
 *		Move all real tables from a shard member to other members
 *
 * After this operation, the specified member will only have foreign table
 * references, allowing it to be safely dropped from the shard group.
 */
static void
DetachShardMember(Oid sgid, Oid srvoid)
{
	List	   *partitions;
	ListCell   *lc;
	int			moved_count = 0;
	int			total_count = 0;
	ForeignServer *server = NULL;
	
	if (OidIsValid(srvoid))
		server = GetForeignServer(srvoid);
	
	ereport(NOTICE,
			(errmsg("detaching shard member \"%s\" - moving all partitions to other members",
					server ? server->servername : "local server")));
	
	/* Get all partitions in the shard group */
	partitions = GetShardGroupPartitions(sgid);
	
	if (partitions == NIL)
	{
		ereport(NOTICE,
				(errmsg("no partitions found in shard group")));
		return;
	}
	
	/* For each partition, check if it's hosted on the member being detached */
	foreach(lc, partitions)
	{
		Oid			partitionOid = lfirst_oid(lc);
		Relation	rel;
		char	   *relname;
		Oid			currentHost;
		Oid			targetHost;
		bool		found;
		
		/* Get partition name */
		rel = table_open(partitionOid, AccessShareLock);
		relname = pstrdup(RelationGetRelationName(rel));
		table_close(rel, AccessShareLock);
		
		/* Find current host */
		currentHost = FindPartitionHostMember(partitionOid, sgid);

		/* Only process partitions hosted on the member being detached */
		if (currentHost == srvoid)
		{
			total_count++;
			
			/* 
			 * Determine target host using consistent hashing,
			 * excluding the member being detached to ensure we find
			 * a different target.
			 */
			targetHost = GetPartitionTargetMember(sgid, relname, &srvoid, &found);
			if (!found)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("could not determine target member for partition \"%s\"", relname)));
			}
			
			/* Migrate the partition */
			MigratePartitionToMember(partitionOid, currentHost, targetHost, sgid);
			moved_count++;
		}
		
		pfree(relname);
	}
	
	list_free(partitions);
	
	if (total_count == 0)
	{
		ereport(NOTICE,
				(errmsg("member \"%s\" had no partitions to detach", server->servername)));
	}
	else
	{
		ereport(NOTICE,
				(errmsg("detach complete: %d partitions moved from \"%s\"",
						moved_count, server->servername)));
	}
}
