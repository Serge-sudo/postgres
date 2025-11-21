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
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_partitioned_table.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_shardgroups.h"
#include "catalog/pg_shardmembers.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/shardgroupcmds.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "partitioning/partdefs.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"

/* Helper functions */
extern Oid get_shardgroup_oid(const char *sgname, bool missing_ok);
extern Oid get_database_default_shardgroup(Oid dbid);
extern void SetRelationShardGroup(Oid relid, Oid sgid);

/* Forward declaration of helper function for syncing tables on new shard member */
static void SyncTablesOnNewShardMember(Oid sgid, Oid newsrvoid);
static void SyncShardGroupMetadataToMember(Oid sgid, Oid newsrvoid);
static void NotifyExistingMembersAboutNewMember(Oid sgid, Oid newsrvoid);
static void ExecuteDDLOnRemoteServer(Oid serveroid, const char *sql);


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
			else if (strcmp(defel->defname, "skip_sync") == 0)
			{
				/*
				 * The skip_sync option is internal-only and is used to prevent
				 * infinite recursion during metadata synchronization to remote
				 * shard members. When adding a new shard member, we need to sync
				 * metadata to it, but we don't want the remote member to recursively
				 * sync back to us.
				 * 
				 * This option should not be used manually as it will skip important
				 * synchronization steps. It's automatically added by the
				 * SyncShardGroupMetadataToMember function.
				 */
				char	   *skip_sync_str = defGetString(defel);
				if (strcmp(skip_sync_str, "true") == 0 || strcmp(skip_sync_str, "1") == 0)
				{
					stmt->skip_sync = true;
					ereport(DEBUG1,
							(errmsg("skip_sync option enabled for ALTER SHARD GROUP ADD MEMBER"),
							 errhint("This option is for internal use to prevent metadata sync recursion.")));
				}
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
		if (!stmt->skip_sync)
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
		
		/* Get the foreign server OID */
		srvoid = get_foreign_server_oid(stmt->servername, false);
		
		/* Check if any foreign tables in this shard group depend on this member */
		{
			Relation	classrel;
			Relation	ftrel;
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

	/* Check if cluster_name is set */
	if (!cluster_name || cluster_name[0] == '\0')
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("cluster_name is not set, cannot sync tables to new shard member"),
				 errhint("Set cluster_name in postgresql.conf to enable automatic table synchronization.")));
		return;
	}

	/* Get the foreign server name for logging */
	server = GetForeignServer(newsrvoid);

	ereport(DEBUG1,
			(errmsg("syncing tables in shard group to new member \"%s\"",
					server->servername)));

	/* Scan pg_class for all tables with this shard group */
	classrel = table_open(RelationRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_class_relsgid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(sgid));

	scan = systable_beginscan(classrel, InvalidOid, false, NULL, 1, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);
		Oid			relid = classForm->oid;
		Relation	rel;
		char	   *relname;
		char	   *nspname;
		TupleDesc	tupdesc;
		StringInfoData ddl;
		int			i;
		bool		is_worldwide;
		bool		is_partition;
		bool		is_partitioned;
		bool		is_foreign_table;

		/* Skip non-table relations */
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
			char * cluster_;

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
				
				appendStringInfo(&ddl, " PARTITION BY %s (%s)",
									partkey->strategy == PARTITION_STRATEGY_HASH ? "HASH" :
									partkey->strategy == PARTITION_STRATEGY_LIST ? "LIST" :
									partkey->strategy == PARTITION_STRATEGY_RANGE ? "RANGE" : "UNKNOWN",
									partkey_str);
				pfree(partkey_str);
			}

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
			char * cluster_;
			
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
						 "ALTER SHARD GROUP %s ADD MEMBER IF NOT EXISTS %s WITH (skip_sync 'true');",
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
						 "ALTER SHARD GROUP %s ADD MEMBER IF NOT EXISTS %s WITH (skip_sync 'true');",
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
	HeapTuple	sgtuple;
	Form_pg_shardgroups sgform;
	char	   *sgname;
	ForeignServer *newserver;
	StringInfoData ddl;
	
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
						 "ALTER SHARD GROUP %s ADD MEMBER IF NOT EXISTS %s WITH (skip_sync 'true');",
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
