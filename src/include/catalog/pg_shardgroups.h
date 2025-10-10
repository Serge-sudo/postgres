/*-------------------------------------------------------------------------
 *
 * pg_shardgroups.h
 *	  definition of the "shard groups" system catalog (pg_shardgroups)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_shardgroups.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SHARDGROUPS_H
#define PG_SHARDGROUPS_H

#include "catalog/genbki.h"
#include "catalog/pg_shardgroups_d.h"

/* ----------------
 *		pg_shardgroups definition.  cpp turns this into
 *		typedef struct FormData_pg_shardgroups
 * ----------------
 */
CATALOG(pg_shardgroups,6100,ShardGroupRelationId) BKI_ROWTYPE_OID(6101,ShardGroupRelation_Rowtype_Id)
{
	Oid			oid;			/* oid */
	NameData	sgname;			/* shard group name */
	Oid			sgowner BKI_LOOKUP(pg_authid);	/* owner */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		sgoptions[1];	/* group-wide options (JSON/text) */
#endif
} FormData_pg_shardgroups;

/* ----------------
 *		Form_pg_shardgroups corresponds to a pointer to a tuple with
 *		the format of pg_shardgroups relation.
 * ----------------
 */
typedef FormData_pg_shardgroups *Form_pg_shardgroups;

DECLARE_TOAST(pg_shardgroups, 6102, 6103);

DECLARE_UNIQUE_INDEX_PKEY(pg_shardgroups_oid_index, 6104, ShardGroupOidIndexId, pg_shardgroups, btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_shardgroups_name_index, 6105, ShardGroupNameIndexId, pg_shardgroups, btree(sgname name_ops));

MAKE_SYSCACHE(SHARDGROUPOID, pg_shardgroups_oid_index, 4);
MAKE_SYSCACHE(SHARDGROUPNAME, pg_shardgroups_name_index, 4);

#endif							/* PG_SHARDGROUPS_H */
