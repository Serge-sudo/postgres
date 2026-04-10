/*-------------------------------------------------------------------------
 *
 * pg_shardmembers.h
 *	  definition of the "shard members" system catalog (pg_shardmembers)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_shardmembers.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SHARDMEMBERS_H
#define PG_SHARDMEMBERS_H

#include "catalog/genbki.h"
#include "catalog/pg_shardmembers_d.h"

/* ----------------
 *		pg_shardmembers definition.  cpp turns this into
 *		typedef struct FormData_pg_shardmembers
 * ----------------
 */
CATALOG(pg_shardmembers,8795,ShardMemberRelationId) BKI_ROWTYPE_OID(8796,ShardMemberRelation_Rowtype_Id)
{
	Oid			oid;			/* oid */
	Oid			sgid;			/* references pg_shardgroups.oid */
	Oid			srvid BKI_LOOKUP(pg_foreign_server);	/* references pg_foreign_server.oid */
	int32		smpriority;		/* routing priority (lower = higher priority) */
	char		smstate;		/* 'u' up, 'd' down, 'r' read-only */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		smoptions[1];	/* per-member options (JSON/text) */
#endif
} FormData_pg_shardmembers;

/* ----------------
 *		Form_pg_shardmembers corresponds to a pointer to a tuple with
 *		the format of pg_shardmembers relation.
 * ----------------
 */
typedef FormData_pg_shardmembers *Form_pg_shardmembers;

DECLARE_TOAST(pg_shardmembers, 8797, 8798);

DECLARE_UNIQUE_INDEX_PKEY(pg_shardmembers_oid_index, 8799, ShardMemberOidIndexId, pg_shardmembers, btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_shardmembers_sgid_srvid_index, 8800, ShardMemberSgidSrvidIndexId, pg_shardmembers, btree(sgid oid_ops, srvid oid_ops));

MAKE_SYSCACHE(SHARDMEMBEROID, pg_shardmembers_oid_index, 4);

/* Member state constants */
#define SHARD_MEMBER_STATE_UP		'u'
#define SHARD_MEMBER_STATE_DOWN		'd'
#define SHARD_MEMBER_STATE_READONLY	'r'

#endif							/* PG_SHARDMEMBERS_H */
