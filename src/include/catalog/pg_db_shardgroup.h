/*-------------------------------------------------------------------------
 *
 * pg_db_shardgroup.h
 *	  definition of the "database default shard group" system catalog (pg_db_shardgroup)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_db_shardgroup.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_DB_SHARDGROUP_H
#define PG_DB_SHARDGROUP_H

#include "catalog/genbki.h"
#include "catalog/pg_db_shardgroup_d.h"

/* ----------------
 *		pg_db_shardgroup definition.  cpp turns this into
 *		typedef struct FormData_pg_db_shardgroup
 * ----------------
 */
CATALOG(pg_db_shardgroup,8801,DbShardGroupRelationId) BKI_ROWTYPE_OID(8802,DbShardGroupRelation_Rowtype_Id)
{
	Oid			dbid BKI_LOOKUP(pg_database);	/* references pg_database.oid */
	Oid			sgid;			/* references pg_shardgroups.oid */
} FormData_pg_db_shardgroup;

/* ----------------
 *		Form_pg_db_shardgroup corresponds to a pointer to a tuple with
 *		the format of pg_db_shardgroup relation.
 * ----------------
 */
typedef FormData_pg_db_shardgroup *Form_pg_db_shardgroup;

DECLARE_UNIQUE_INDEX_PKEY(pg_db_shardgroup_dbid_index, 8803, DbShardGroupDbidIndexId, pg_db_shardgroup, btree(dbid oid_ops));

MAKE_SYSCACHE(DBSHARDGROUPDBID, pg_db_shardgroup_dbid_index, 4);

#endif							/* PG_DB_SHARDGROUP_H */
