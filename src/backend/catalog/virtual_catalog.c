/*-------------------------------------------------------------------------
 *
 * virtual_catalog.c
 *	  Virtual catalog system for temporary table metadata storage
 *
 * This module provides in-memory storage for temporary table catalog metadata,
 * allowing temp table metadata to be stored in backend memory rather than
 * on disk, while maintaining compatibility with the existing catalog system.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/catalog/virtual_catalog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/virtual_catalog.h"
#include "catalog/pg_class.h"
#include "catalog/namespace.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "common/hashfn.h"
#include "access/htup_details.h"
#include "storage/lmgr.h"
#include "miscadmin.h"

/* Global state */
VirtualCatalogState *virtual_catalog_state = NULL;

/* Memory context for virtual catalog */
static MemoryContext VirtualCatalogMemoryContext = NULL;

/* Hash table parameters */
#define VIRTUAL_CATALOG_INIT_SIZE	256

/*
 * Hash function for virtual catalog keys
 */
static uint32
virtual_catalog_hash(const void *key, Size keysize)
{
	const VirtualCatalogKey *vkey = (const VirtualCatalogKey *) key;
	
	/* Combine OID, catalog type, and secondary key for hash */
	return hash_combine(
		hash_combine(DatumGetUInt32(hash_uint32((uint32) vkey->oid)),
					 (uint32) vkey->catalog_type),
		(uint32) vkey->secondary_key);
}

/*
 * Match function for virtual catalog keys
 */
static int
virtual_catalog_match(const void *key1, const void *key2, Size keysize)
{
	const VirtualCatalogKey *vkey1 = (const VirtualCatalogKey *) key1;
	const VirtualCatalogKey *vkey2 = (const VirtualCatalogKey *) key2;
	
	if (vkey1->oid == vkey2->oid &&
		vkey1->catalog_type == vkey2->catalog_type &&
		vkey1->secondary_key == vkey2->secondary_key)
		return 0;
	else
		return 1;
}

/*
 * InitVirtualCatalog
 *		Initialize the virtual catalog system for this backend
 */
void
InitVirtualCatalog(void)
{
	HASHCTL		hash_ctl;
	
	/* Create memory context if not already created */
	if (VirtualCatalogMemoryContext == NULL)
	{
		VirtualCatalogMemoryContext = AllocSetContextCreate(TopMemoryContext,
															"Virtual Catalog Context",
															ALLOCSET_DEFAULT_SIZES);
	}
	
	/* Allocate virtual catalog state */
	if (virtual_catalog_state == NULL)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(VirtualCatalogMemoryContext);
		
		virtual_catalog_state = palloc0(sizeof(VirtualCatalogState));
		virtual_catalog_state->memcxt = VirtualCatalogMemoryContext;
		
		/* Initialize hash table */
		MemSet(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(VirtualCatalogKey);
		hash_ctl.entrysize = sizeof(VirtualCatalogEntry);
		hash_ctl.hash = virtual_catalog_hash;
		hash_ctl.match = virtual_catalog_match;
		hash_ctl.hcxt = VirtualCatalogMemoryContext;
		
		virtual_catalog_state->hash_table = hash_create("Virtual Catalog Hash",
														VIRTUAL_CATALOG_INIT_SIZE,
														&hash_ctl,
														HASH_ELEM | HASH_FUNCTION | 
														HASH_COMPARE | HASH_CONTEXT);
		
		virtual_catalog_state->initialized = true;
		
		MemoryContextSwitchTo(oldcontext);
	}
}

/*
 * ResetVirtualCatalog
 *		Reset the virtual catalog (called at transaction end)
 */
void
ResetVirtualCatalog(void)
{
	if (virtual_catalog_state != NULL && virtual_catalog_state->initialized)
	{
		/* Remove deleted entries and clean up */
		HASH_SEQ_STATUS hash_seq;
		VirtualCatalogEntry *entry;
		
		hash_seq_init(&hash_seq, virtual_catalog_state->hash_table);
		while ((entry = (VirtualCatalogEntry *) hash_seq_search(&hash_seq)) != NULL)
		{
			if (entry->deleted)
			{
				if (entry->tuple)
					heap_freetuple(entry->tuple);
				hash_search(virtual_catalog_state->hash_table, 
						   &entry->key, HASH_REMOVE, NULL);
			}
		}
	}
}

/*
 * IsVirtualCatalogEnabled
 *		Check if virtual catalog is initialized and enabled
 */
bool
IsVirtualCatalogEnabled(void)
{
	return (virtual_catalog_state != NULL && virtual_catalog_state->initialized);
}

/*
 * VirtualCatalogInsertTuple
 *		Insert a tuple into the virtual catalog
 */
void
VirtualCatalogInsertTuple(VirtualCatalogType catalog_type, HeapTuple tuple, 
						  Oid primary_oid, int secondary_key)
{
	VirtualCatalogKey key;
	VirtualCatalogEntry *entry;
	bool		found;
	MemoryContext oldcontext;
	
	if (!IsVirtualCatalogEnabled())
		InitVirtualCatalog();
	
	/* Setup key */
	key.oid = primary_oid;
	key.catalog_type = catalog_type;
	key.secondary_key = secondary_key;
	
	oldcontext = MemoryContextSwitchTo(virtual_catalog_state->memcxt);
	
	/* Find or create entry */
	entry = (VirtualCatalogEntry *) hash_search(virtual_catalog_state->hash_table,
												&key, HASH_ENTER, &found);
	
	if (found && entry->tuple)
	{
		/* Replace existing tuple */
		heap_freetuple(entry->tuple);
	}
	
	/* Copy tuple to virtual catalog memory context */
	entry->tuple = heap_copytuple(tuple);
	entry->deleted = false;
	
	MemoryContextSwitchTo(oldcontext);
}

/*
 * VirtualCatalogSearchTuple
 *		Search for a tuple in the virtual catalog
 */
HeapTuple
VirtualCatalogSearchTuple(VirtualCatalogType catalog_type, Oid primary_oid, 
						  int secondary_key)
{
	VirtualCatalogKey key;
	VirtualCatalogEntry *entry;
	
	if (!IsVirtualCatalogEnabled())
		return NULL;
	
	/* Setup key */
	key.oid = primary_oid;
	key.catalog_type = catalog_type;
	key.secondary_key = secondary_key;
	
	/* Search for entry */
	entry = (VirtualCatalogEntry *) hash_search(virtual_catalog_state->hash_table,
												&key, HASH_FIND, NULL);
	
	if (entry && !entry->deleted && entry->tuple)
		return heap_copytuple(entry->tuple);
	
	return NULL;
}

/*
 * VirtualCatalogDeleteTuple
 *		Mark a tuple as deleted in the virtual catalog
 */
void
VirtualCatalogDeleteTuple(VirtualCatalogType catalog_type, Oid primary_oid, 
						  int secondary_key)
{
	VirtualCatalogKey key;
	VirtualCatalogEntry *entry;
	
	if (!IsVirtualCatalogEnabled())
		return;
	
	/* Setup key */
	key.oid = primary_oid;
	key.catalog_type = catalog_type;
	key.secondary_key = secondary_key;
	
	/* Find entry and mark as deleted */
	entry = (VirtualCatalogEntry *) hash_search(virtual_catalog_state->hash_table,
												&key, HASH_FIND, NULL);
	
	if (entry)
		entry->deleted = true;
}

/*
 * VirtualCatalogTupleExists
 *		Check if a tuple exists in the virtual catalog
 */
bool
VirtualCatalogTupleExists(VirtualCatalogType catalog_type, Oid primary_oid, 
						  int secondary_key)
{
	VirtualCatalogKey key;
	VirtualCatalogEntry *entry;
	
	if (!IsVirtualCatalogEnabled())
		return false;
	
	/* Setup key */
	key.oid = primary_oid;
	key.catalog_type = catalog_type;
	key.secondary_key = secondary_key;
	
	/* Search for entry */
	entry = (VirtualCatalogEntry *) hash_search(virtual_catalog_state->hash_table,
												&key, HASH_FIND, NULL);
	
	return (entry && !entry->deleted && entry->tuple);
}

/*
 * Relation-specific convenience functions
 */

/*
 * VirtualCatalogInsertClass
 *		Insert a pg_class tuple into virtual catalog
 */
void
VirtualCatalogInsertClass(HeapTuple tuple, Oid relid)
{
	VirtualCatalogInsertTuple(VIRTUAL_CATALOG_CLASS, tuple, relid, 0);
}

/*
 * VirtualCatalogSearchClass
 *		Search for a pg_class tuple in virtual catalog
 */
HeapTuple
VirtualCatalogSearchClass(Oid relid)
{
	return VirtualCatalogSearchTuple(VIRTUAL_CATALOG_CLASS, relid, 0);
}

/*
 * VirtualCatalogDeleteClass
 *		Delete a pg_class tuple from virtual catalog
 */
void
VirtualCatalogDeleteClass(Oid relid)
{
	VirtualCatalogDeleteTuple(VIRTUAL_CATALOG_CLASS, relid, 0);
}

/*
 * VirtualCatalogInsertAttribute
 *		Insert a pg_attribute tuple into virtual catalog
 */
void
VirtualCatalogInsertAttribute(HeapTuple tuple, Oid relid, int attnum)
{
	VirtualCatalogInsertTuple(VIRTUAL_CATALOG_ATTRIBUTE, tuple, relid, attnum);
}

/*
 * VirtualCatalogSearchAttribute
 *		Search for a pg_attribute tuple in virtual catalog
 */
HeapTuple
VirtualCatalogSearchAttribute(Oid relid, int attnum)
{
	return VirtualCatalogSearchTuple(VIRTUAL_CATALOG_ATTRIBUTE, relid, attnum);
}

/*
 * VirtualCatalogDeleteAttribute
 *		Delete a pg_attribute tuple from virtual catalog
 */
void
VirtualCatalogDeleteAttribute(Oid relid, int attnum)
{
	VirtualCatalogDeleteTuple(VIRTUAL_CATALOG_ATTRIBUTE, relid, attnum);
}

/*
 * IsTemporaryRelation
 *		Check if a relation OID corresponds to a temporary relation
 */
bool
IsTemporaryRelation(Oid relid)
{
	HeapTuple	tuple;
	Form_pg_class classtup;
	bool		is_temp = false;
	
	/* First check virtual catalog */
	tuple = VirtualCatalogSearchClass(relid);
	if (HeapTupleIsValid(tuple))
	{
		classtup = (Form_pg_class) GETSTRUCT(tuple);
		is_temp = (classtup->relpersistence == RELPERSISTENCE_TEMP);
		heap_freetuple(tuple);
		return is_temp;
	}
	
	/* Fall back to regular syscache lookup */
	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (HeapTupleIsValid(tuple))
	{
		classtup = (Form_pg_class) GETSTRUCT(tuple);
		is_temp = (classtup->relpersistence == RELPERSISTENCE_TEMP);
		ReleaseSysCache(tuple);
	}
	
	return is_temp;
}

/*
 * ShouldUseVirtualCatalog
 *		Determine if we should use virtual catalog for this relation
 */
bool
ShouldUseVirtualCatalog(Oid relid)
{
	return IsTemporaryRelation(relid);
}