/*-------------------------------------------------------------------------
 *
 * pg_conversion.c
 *	  routines to support manipulation of the pg_conversion relation
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_conversion.c
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
#include "catalog/pg_conversion.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * ConversionCreate
 *
 * Add a new tuple to pg_conversion.
 */
ObjectAddress
ConversionCreate(const char *conname, Oid connamespace,
				 Oid conowner,
				 int32 conforencoding, int32 contoencoding,
				 Oid conproc, bool def)
{
	int			i;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Oid			oid;
	Datum		values[Natts_pg_conversion] = {0};
	bool		nulls[Natts_pg_conversion] = {false};
	NameData	cname;
	ObjectAddress myself,
				referenced;

	/* sanity checks */
	if (!conname)
		elog(ERROR, "no conversion name supplied");

	/* make sure there is no existing conversion of same name */
	if (SearchSysCacheExists2(CONNAMENSP,
							  PointerGetDatum(conname),
							  ObjectIdGetDatum(connamespace)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("conversion \"%s\" already exists", conname)));

	if (def)
	{
		/*
		 * make sure there is no existing default <for encoding><to encoding>
		 * pair in this name space
		 */
		if (FindDefaultConversion(connamespace,
								  conforencoding,
								  contoencoding))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("default conversion for %s to %s already exists",
							pg_encoding_to_char(conforencoding),
							pg_encoding_to_char(contoencoding))));
	}

	/* open pg_conversion */
	rel = table_open(ConversionRelationId, RowExclusiveLock);
	tupDesc = rel->rd_att;

	/* initialize nulls and values */
	for (i = 0; i < Natts_pg_conversion; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) 0;
	}

	/* form a tuple */
	namestrcpy(&cname, conname);
	oid = GetNewOidWithIndex(rel, ConversionOidIndexId,
							 Anum_pg_conversion_oid);
	HeapTupleSetValue(pg_conversion, oid, ObjectIdGetDatum(oid), values);
	HeapTupleSetValue(pg_conversion, conname, NameGetDatum(&cname), values);
	HeapTupleSetValue(pg_conversion, connamespace, ObjectIdGetDatum(connamespace), values);
	HeapTupleSetValue(pg_conversion, conowner, ObjectIdGetDatum(conowner), values);
	HeapTupleSetValue(pg_conversion, conforencoding, Int32GetDatum(conforencoding), values);
	HeapTupleSetValue(pg_conversion, contoencoding, Int32GetDatum(contoencoding), values);
	HeapTupleSetValue(pg_conversion, conproc, ObjectIdGetDatum(conproc), values);
	HeapTupleSetValue(pg_conversion, condefault, BoolGetDatum(def), values);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* insert a new tuple */
	CatalogTupleInsert(rel, tup, NULL);

	myself.classId = ConversionRelationId;
	myself.objectId = oid;
	myself.objectSubId = 0;

	/* create dependency on conversion procedure */
	referenced.classId = ProcedureRelationId;
	referenced.objectId = conproc;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* create dependency on namespace */
	referenced.classId = NamespaceRelationId;
	referenced.objectId = connamespace;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* create dependency on owner */
	recordDependencyOnOwner(ConversionRelationId, oid, conowner);

	/* dependency on extension */
	recordDependencyOnCurrentExtension(&myself, false);

	/* Post creation hook for new conversion */
	InvokeObjectPostCreateHook(ConversionRelationId, oid, 0);

	heap_freetuple(tup);
	table_close(rel, RowExclusiveLock);

	return myself;
}

/*
 * FindDefaultConversion
 *
 * Find "default" conversion proc by for_encoding and to_encoding in the
 * given namespace.
 *
 * If found, returns the procedure's oid, otherwise InvalidOid.  Note that
 * you get the procedure's OID not the conversion's OID!
 */
Oid
FindDefaultConversion(Oid name_space, int32 for_encoding, int32 to_encoding)
{
	CatCList   *catlist;
	HeapTuple	tuple;
	Form_pg_conversion body;
	Oid			proc = InvalidOid;
	int			i;

	catlist = SearchSysCacheList3(CONDEFAULT,
								  ObjectIdGetDatum(name_space),
								  Int32GetDatum(for_encoding),
								  Int32GetDatum(to_encoding));

	for (i = 0; i < catlist->n_members; i++)
	{
		tuple = &catlist->members[i]->tuple;
		body = (Form_pg_conversion) GETSTRUCT(tuple);
		if (body->condefault)
		{
			proc = body->conproc;
			break;
		}
	}
	ReleaseSysCacheList(catlist);
	return proc;
}
