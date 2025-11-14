/*-------------------------------------------------------------------------
 *
 * pg_collation.c
 *	  routines to support manipulation of the pg_collation relation
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_collation.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/*
 * CollationCreate
 *
 * Add a new tuple to pg_collation.
 *
 * if_not_exists: if true, don't fail on duplicate name, just print a notice
 * and return InvalidOid.
 * quiet: if true, don't fail on duplicate name, just silently return
 * InvalidOid (overrides if_not_exists).
 */
Oid
CollationCreate(const char *collname, Oid collnamespace,
				Oid collowner,
				char collprovider,
				bool collisdeterministic,
				int32 collencoding,
				const char *collcollate, const char *collctype,
				const char *colllocale,
				const char *collicurules,
				const char *collversion,
				bool if_not_exists,
				bool quiet)
{
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_pg_collation] = {0};
	bool		nulls[Natts_pg_collation] = {false};
	NameData	name_name;
	Oid			oid;
	ObjectAddress myself,
				referenced;

	Assert(collname);
	Assert(collnamespace);
	Assert(collowner);
	Assert((collprovider == COLLPROVIDER_LIBC &&
			collcollate && collctype && !colllocale) ||
		   (collprovider != COLLPROVIDER_LIBC &&
			!collcollate && !collctype && colllocale));

	/*
	 * Make sure there is no existing collation of same name & encoding.
	 *
	 * This would be caught by the unique index anyway; we're just giving a
	 * friendlier error message.  The unique index provides a backstop against
	 * race conditions.
	 */
	oid = GetSysCacheOid3(COLLNAMEENCNSP,
						  Anum_pg_collation_oid,
						  PointerGetDatum(collname),
						  Int32GetDatum(collencoding),
						  ObjectIdGetDatum(collnamespace));
	if (OidIsValid(oid))
	{
		if (quiet)
			return InvalidOid;
		else if (if_not_exists)
		{
			/*
			 * If we are in an extension script, insist that the pre-existing
			 * object be a member of the extension, to avoid security risks.
			 */
			ObjectAddressSet(myself, CollationRelationId, oid);
			checkMembershipInCurrentExtension(&myself);

			/* OK to skip */
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 collencoding == -1
					 ? errmsg("collation \"%s\" already exists, skipping",
							  collname)
					 : errmsg("collation \"%s\" for encoding \"%s\" already exists, skipping",
							  collname, pg_encoding_to_char(collencoding))));
			return InvalidOid;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 collencoding == -1
					 ? errmsg("collation \"%s\" already exists",
							  collname)
					 : errmsg("collation \"%s\" for encoding \"%s\" already exists",
							  collname, pg_encoding_to_char(collencoding))));
	}

	/* open pg_collation; see below about the lock level */
	rel = table_open(CollationRelationId, ShareRowExclusiveLock);

	/*
	 * Also forbid a specific-encoding collation shadowing an any-encoding
	 * collation, or an any-encoding collation being shadowed (see
	 * get_collation_name()).  This test is not backed up by the unique index,
	 * so we take a ShareRowExclusiveLock earlier, to protect against
	 * concurrent changes fooling this check.
	 */
	if (collencoding == -1)
		oid = GetSysCacheOid3(COLLNAMEENCNSP,
							  Anum_pg_collation_oid,
							  PointerGetDatum(collname),
							  Int32GetDatum(GetDatabaseEncoding()),
							  ObjectIdGetDatum(collnamespace));
	else
		oid = GetSysCacheOid3(COLLNAMEENCNSP,
							  Anum_pg_collation_oid,
							  PointerGetDatum(collname),
							  Int32GetDatum(-1),
							  ObjectIdGetDatum(collnamespace));
	if (OidIsValid(oid))
	{
		if (quiet)
		{
			table_close(rel, NoLock);
			return InvalidOid;
		}
		else if (if_not_exists)
		{
			/*
			 * If we are in an extension script, insist that the pre-existing
			 * object be a member of the extension, to avoid security risks.
			 */
			ObjectAddressSet(myself, CollationRelationId, oid);
			checkMembershipInCurrentExtension(&myself);

			/* OK to skip */
			table_close(rel, NoLock);
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("collation \"%s\" already exists, skipping",
							collname)));
			return InvalidOid;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("collation \"%s\" already exists",
							collname)));
	}

	tupDesc = RelationGetDescr(rel);

	/* form a tuple */
	memset(nulls, 0, sizeof(nulls));

	namestrcpy(&name_name, collname);
	oid = GetNewOidWithIndex(rel, CollationOidIndexId,
							 Anum_pg_collation_oid);
	HeapTupleSetValue(pg_collation, oid, ObjectIdGetDatum(oid), values);
	HeapTupleSetValue(pg_collation, collname, NameGetDatum(&name_name), values);
	HeapTupleSetValue(pg_collation, collnamespace, ObjectIdGetDatum(collnamespace), values);
	HeapTupleSetValue(pg_collation, collowner, ObjectIdGetDatum(collowner), values);
	HeapTupleSetValue(pg_collation, collprovider, CharGetDatum(collprovider), values);
	HeapTupleSetValue(pg_collation, collisdeterministic, BoolGetDatum(collisdeterministic), values);
	HeapTupleSetValue(pg_collation, collencoding, Int32GetDatum(collencoding), values);
	if (collcollate)
		HeapTupleSetValue(pg_collation, collcollate, CStringGetTextDatum(collcollate), values);
	else
		HeapTupleSetValueNull(pg_collation, collcollate, values, nulls);
	if (collctype)
		HeapTupleSetValue(pg_collation, collctype, CStringGetTextDatum(collctype), values);
	else
		HeapTupleSetValueNull(pg_collation, collctype, values, nulls);
	if (colllocale)
		HeapTupleSetValue(pg_collation, colllocale, CStringGetTextDatum(colllocale), values);
	else
		HeapTupleSetValueNull(pg_collation, colllocale, values, nulls);
	if (collicurules)
		HeapTupleSetValue(pg_collation, collicurules, CStringGetTextDatum(collicurules), values);
	else
		HeapTupleSetValueNull(pg_collation, collicurules, values, nulls);
	if (collversion)
		HeapTupleSetValue(pg_collation, collversion, CStringGetTextDatum(collversion), values);
	else
		HeapTupleSetValueNull(pg_collation, collversion, values, nulls);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* insert a new tuple */
	CatalogTupleInsert(rel, tup, NULL);
	Assert(OidIsValid(oid));

	/* set up dependencies for the new collation */
	myself.classId = CollationRelationId;
	myself.objectId = oid;
	myself.objectSubId = 0;

	/* create dependency on namespace */
	referenced.classId = NamespaceRelationId;
	referenced.objectId = collnamespace;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* create dependency on owner */
	recordDependencyOnOwner(CollationRelationId, oid, collowner);

	/* dependency on extension */
	recordDependencyOnCurrentExtension(&myself, false);

	/* Post creation hook for new collation */
	InvokeObjectPostCreateHook(CollationRelationId, oid, 0);

	heap_freetuple(tup);
	table_close(rel, NoLock);

	return oid;
}
