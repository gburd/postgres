/*-------------------------------------------------------------------------
 *
 * matview_ivm.c
 *	  incremental maintenance of materialized views
 *
 * A materialized view created WITH (incremental_view_maintenance) has a row
 * in pg_matview_ivm holding the maintenance program compiled from its query.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/matview_ivm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/pg_matview_ivm.h"
#include "commands/matview_ivm.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/*
 * RemoveMatViewIvmEntry
 *		Delete the pg_matview_ivm row of a materialized view being dropped,
 *		if it has one.
 */
void
RemoveMatViewIvmEntry(Oid matviewOid)
{
	Relation	rel;
	HeapTuple	tuple;

	tuple = SearchSysCache1(MATVIEWIVMRELID, ObjectIdGetDatum(matviewOid));
	if (!HeapTupleIsValid(tuple))
		return;

	rel = table_open(MatViewIvmRelationId, RowExclusiveLock);
	CatalogTupleDelete(rel, &tuple->t_self);
	ReleaseSysCache(tuple);
	table_close(rel, RowExclusiveLock);
}
