/*-------------------------------------------------------------------------
 *
 * virtual_catalog.h
 *	  prototypes and data structures for virtual catalog system
 *
 * Virtual catalog provides in-memory storage for temporary table metadata,
 * working alongside the traditional disk-based catalog system.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/virtual_catalog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VIRTUAL_CATALOG_H
#define VIRTUAL_CATALOG_H

#include "postgres.h"
#include "catalog/pg_class.h"
#include "catalog/pg_attribute.h"
#include "utils/hsearch.h"
#include "access/htup.h"

/*
 * Virtual catalog entry types
 */
typedef enum VirtualCatalogType
{
	VIRTUAL_CATALOG_CLASS,		/* pg_class entries */
	VIRTUAL_CATALOG_ATTRIBUTE,	/* pg_attribute entries */
	VIRTUAL_CATALOG_INDEX,		/* pg_index entries */
	VIRTUAL_CATALOG_TYPE,		/* pg_type entries */
	VIRTUAL_CATALOG_NAMESPACE	/* pg_namespace entries */
} VirtualCatalogType;

/*
 * Hash table key structure for virtual catalog entries
 */
typedef struct VirtualCatalogKey
{
	Oid			oid;			/* Primary key (relation OID, attribute OID, etc.) */
	VirtualCatalogType catalog_type;	/* Type of catalog entry */
	int			secondary_key;	/* Secondary key (e.g., attnum for attributes) */
} VirtualCatalogKey;

/*
 * Virtual catalog entry structure
 */
typedef struct VirtualCatalogEntry
{
	VirtualCatalogKey key;		/* Hash table key */
	HeapTuple	tuple;			/* The actual catalog tuple */
	bool		deleted;		/* Mark for deletion */
} VirtualCatalogEntry;

/*
 * Virtual catalog state structure (per backend)
 */
typedef struct VirtualCatalogState
{
	HTAB	   *hash_table;		/* Hash table for entries */
	bool		initialized;	/* Whether virtual catalog is initialized */
	MemoryContext memcxt;		/* Memory context for virtual catalog */
} VirtualCatalogState;

/* Global state */
extern VirtualCatalogState *virtual_catalog_state;

/* Function prototypes */
extern void InitVirtualCatalog(void);
extern void ResetVirtualCatalog(void);
extern bool IsVirtualCatalogEnabled(void);

/* Tuple manipulation functions */
extern void VirtualCatalogInsertTuple(VirtualCatalogType catalog_type, 
									  HeapTuple tuple, Oid primary_oid, 
									  int secondary_key);
extern HeapTuple VirtualCatalogSearchTuple(VirtualCatalogType catalog_type,
											Oid primary_oid, int secondary_key);
extern void VirtualCatalogDeleteTuple(VirtualCatalogType catalog_type,
									  Oid primary_oid, int secondary_key);
extern bool VirtualCatalogTupleExists(VirtualCatalogType catalog_type,
									  Oid primary_oid, int secondary_key);

/* Relation-specific functions */
extern void VirtualCatalogInsertClass(HeapTuple tuple, Oid relid);
extern HeapTuple VirtualCatalogSearchClass(Oid relid);
extern void VirtualCatalogDeleteClass(Oid relid);

extern void VirtualCatalogInsertAttribute(HeapTuple tuple, Oid relid, int attnum);
extern HeapTuple VirtualCatalogSearchAttribute(Oid relid, int attnum);
extern void VirtualCatalogDeleteAttribute(Oid relid, int attnum);

/* Utility functions */
extern bool IsTemporaryRelation(Oid relid);
extern bool ShouldUseVirtualCatalog(Oid relid);

#endif							/* VIRTUAL_CATALOG_H */