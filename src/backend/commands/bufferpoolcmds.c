/*-------------------------------------------------------------------------
 *
 * bufferpoolcmds.c
 *	  Routines for buffer pool DDL commands.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/bufferpoolcmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_bufferpool.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "commands/bufferpoolcmds.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "storage/bufpool.h"
#include "storage/bufpool_internals.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * Look up the handler function for a buffer pool.
 *
 * The handler must accept one internal argument and return internal.
 */
static Oid
lookup_bufferpool_handler_func(List *handler_name)
{
	Oid			handlerOid;
	Oid			funcargtypes[1] = {INTERNALOID};

	if (handler_name == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("handler function is not specified")));

	/* handlers have one argument of type internal */
	handlerOid = LookupFuncName(handler_name, 1, funcargtypes, false);

	/* Check return type is internal */
	if (get_func_rettype(handlerOid) != INTERNALOID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("function %s must return type %s",
						NameListToString(handler_name),
						"internal")));

	return handlerOid;
}

/*
 * CreateBufferPool
 *		Implements CREATE BUFFER POOL.
 */
ObjectAddress
CreateBufferPool(CreateBufferPoolStmt *stmt)
{
	Relation	rel;
	ObjectAddress myself;
	ObjectAddress referenced;
	Oid			bpoid;
	Oid			bphandler;
	bool		nulls[Natts_pg_bufferpool];
	Datum		values[Natts_pg_bufferpool];
	HeapTuple	tup;
	int64		bpsize;

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
				 errmsg("cannot create buffer pool during recovery")));

	rel = table_open(BufferPoolRelationId, RowExclusiveLock);

	/* Must be superuser */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create buffer pool \"%s\"",
						stmt->poolname),
				 errhint("Must be superuser to create a buffer pool.")));

	/* Check if name is already used */
	if (SearchSysCacheExists1(BUFFERPOOLNAME,
							  CStringGetDatum(stmt->poolname)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("buffer pool \"%s\" already exists",
						stmt->poolname)));
	}

	/* Resolve handler function */
	bphandler = lookup_bufferpool_handler_func(stmt->handler_name);

	/* Parse size */
	bpsize = DatumGetInt64(DirectFunctionCall1(int8in,
											   CStringGetDatum(stmt->size)));
	if (bpsize <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("buffer pool size must be positive")));

	/* Insert tuple into pg_bufferpool */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	bpoid = GetNewOidWithIndex(rel, BufferPoolOidIndexId,
							   Anum_pg_bufferpool_oid);
	values[Anum_pg_bufferpool_oid - 1] = ObjectIdGetDatum(bpoid);
	values[Anum_pg_bufferpool_bpname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->poolname));
	values[Anum_pg_bufferpool_bphandler - 1] = ObjectIdGetDatum(bphandler);
	values[Anum_pg_bufferpool_bpsize - 1] = Int64GetDatum(bpsize);

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	myself.classId = BufferPoolRelationId;
	myself.objectId = bpoid;
	myself.objectSubId = 0;

	/* Record dependency on handler function */
	referenced.classId = ProcedureRelationId;
	referenced.objectId = bphandler;
	referenced.objectSubId = 0;

	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	recordDependencyOnCurrentExtension(&myself, false);

	InvokeObjectPostCreateHook(BufferPoolRelationId, bpoid, 0);

	table_close(rel, RowExclusiveLock);

	/*
	 * Now that the catalog entry is committed, create the actual dynamic
	 * buffer pool with DSM-backed memory.  Resolve the handler function to
	 * get the BufferPoolRoutine vtable.
	 */
	{
		const BufferPoolRoutine *routine;
		Datum		datum;
		int			pool_nbuffers;

		datum = OidFunctionCall0(bphandler);
		routine = (const BufferPoolRoutine *) DatumGetPointer(datum);

		pool_nbuffers = (int) (bpsize / BLCKSZ);
		if (pool_nbuffers < 16)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("buffer pool size must be at least %d bytes",
							16 * BLCKSZ)));

		CreateDynamicBufferPool(bpoid, stmt->poolname, pool_nbuffers, routine,
								bphandler);
	}

	return myself;
}

/*
 * AlterBufferPool
 *		Implements ALTER BUFFER POOL ... SET SIZE / SET ( options ).
 */
ObjectAddress
AlterBufferPool(AlterBufferPoolStmt *stmt)
{
	Oid			bpoid;
	ObjectAddress address;

	/* Must be superuser */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to alter buffer pool \"%s\"",
						stmt->poolname),
				 errhint("Must be superuser to alter a buffer pool.")));

	/* Look up the pool by name */
	bpoid = get_bufferpool_oid(stmt->poolname, false);

	/* Cannot alter the default pool */
	if (bpoid == DEFAULT_BUFFERPOOL_OID)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot alter the default buffer pool")));

	/* Handle SET SIZE */
	if (stmt->size != NULL)
	{
		int64		newsize;

		newsize = DatumGetInt64(DirectFunctionCall1(int8in,
													CStringGetDatum(stmt->size)));
		if (newsize <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("buffer pool size must be positive")));

		if (newsize / BLCKSZ < 16)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("buffer pool size must be at least %d bytes",
							16 * BLCKSZ)));

		/*
		 * ALTER BUFFER POOL ... SET SIZE is implemented as drop-and-recreate:
		 * DSM has no resize API, so we tear down the old segment, create a
		 * new one at the desired size, and update the catalog.  The pool's
		 * cached pages are evicted in the process (the pool is a cache;
		 * on-disk data is unaffected), so this is not an online resize --
		 * users should expect a cold cache on the pool immediately after.
		 */
		{
			BufferPoolDesc *pool;
			int			new_nbuffers;

			pool = GetBufferPoolByOid(bpoid);
			if (pool == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("buffer pool \"%s\" is not active in shared memory",
								stmt->poolname)));

			if (!PoolIsDynamic(pool))
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("cannot resize buffer pool \"%s\": not a dynamic pool",
								stmt->poolname)));

			new_nbuffers = (int) (newsize / BLCKSZ);

			/* No-op if size unchanged */
			if (new_nbuffers == pool->bp_nbuffers)
			{
				ereport(NOTICE,
						(errmsg("buffer pool \"%s\" already has %d buffers, no resize needed",
								stmt->poolname, new_nbuffers)));
			}
			else
			{
				ResizeDynamicBufferPool(pool, new_nbuffers);

				/* Update catalog with new size */
				{
					Relation	bprel;
					HeapTuple	oldtup;
					HeapTuple	newtup;
					Datum		values[Natts_pg_bufferpool];
					bool		nulls[Natts_pg_bufferpool];
					bool		replaces[Natts_pg_bufferpool];

					bprel = table_open(BufferPoolRelationId, RowExclusiveLock);

					oldtup = SearchSysCache1(BUFFERPOOLOID, ObjectIdGetDatum(bpoid));
					if (!HeapTupleIsValid(oldtup))
						elog(ERROR, "cache lookup failed for buffer pool %u", bpoid);

					memset(values, 0, sizeof(values));
					memset(nulls, false, sizeof(nulls));
					memset(replaces, false, sizeof(replaces));

					values[Anum_pg_bufferpool_bpsize - 1] = Int64GetDatum(newsize);
					replaces[Anum_pg_bufferpool_bpsize - 1] = true;

					newtup = heap_modify_tuple(oldtup, RelationGetDescr(bprel),
											   values, nulls, replaces);
					CatalogTupleUpdate(bprel, &oldtup->t_self, newtup);

					ReleaseSysCache(oldtup);
					heap_freetuple(newtup);
					table_close(bprel, RowExclusiveLock);
				}
			}
		}
	}

	/* Handle SET ( options ) */
	if (stmt->options != NIL)
	{
		BufferPoolDesc *pool;
		ListCell   *lc;

		pool = GetBufferPoolByOid(bpoid);
		if (pool == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("buffer pool \"%s\" is not active in shared memory",
							stmt->poolname)));

		foreach(lc, stmt->options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "direct_io") == 0)
			{
				pool->bp_use_direct_io = defGetBoolean(def);
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unrecognized buffer pool option \"%s\"",
								def->defname)));
			}
		}
	}

	InvokeObjectPostAlterHook(BufferPoolRelationId, bpoid, 0);

	ObjectAddressSet(address, BufferPoolRelationId, bpoid);
	return address;
}

/*
 * RenameBufferPool
 *		Implements ALTER BUFFER POOL ... RENAME TO.
 *
 * Updates both the pg_bufferpool catalog entry and the in-memory
 * BufferPoolDesc shared-memory descriptor.
 */
ObjectAddress
RenameBufferPool(const char *oldname, const char *newname)
{
	Oid			bpoid;
	Relation	rel;
	HeapTuple	tup;
	HeapTuple	newtup;
	Datum		values[Natts_pg_bufferpool];
	bool		nulls[Natts_pg_bufferpool];
	bool		replaces[Natts_pg_bufferpool];
	NameData	newnameattrdata;
	BufferPoolDesc *pool;
	ObjectAddress address;

	/* Must be superuser */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to rename buffer pool \"%s\"",
						oldname),
				 errhint("Must be superuser to rename a buffer pool.")));

	/* Look up the pool by name */
	bpoid = get_bufferpool_oid(oldname, false);

	/* Cannot rename the default pool */
	if (bpoid == DEFAULT_BUFFERPOOL_OID)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot rename the default buffer pool")));

	/* Check that the new name doesn't already exist */
	if (OidIsValid(get_bufferpool_oid(newname, true)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("buffer pool \"%s\" already exists", newname)));

	/* Update catalog entry */
	rel = table_open(BufferPoolRelationId, RowExclusiveLock);

	tup = SearchSysCache1(BUFFERPOOLOID, ObjectIdGetDatum(bpoid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for buffer pool %u", bpoid);

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	namestrcpy(&newnameattrdata, newname);
	values[Anum_pg_bufferpool_bpname - 1] = NameGetDatum(&newnameattrdata);
	replaces[Anum_pg_bufferpool_bpname - 1] = true;

	newtup = heap_modify_tuple(tup, RelationGetDescr(rel),
							   values, nulls, replaces);
	CatalogTupleUpdate(rel, &tup->t_self, newtup);

	ReleaseSysCache(tup);
	heap_freetuple(newtup);

	/* Update in-memory BufferPoolDesc name */
	pool = GetBufferPoolByOid(bpoid);
	if (pool != NULL)
		namestrcpy(&pool->bp_name, newname);

	InvokeObjectPostAlterHook(BufferPoolRelationId, bpoid, 0);

	table_close(rel, RowExclusiveLock);

	ObjectAddressSet(address, BufferPoolRelationId, bpoid);
	return address;
}

/*
 * DropBufferPoolById -- drop a buffer pool given its OID.
 *
 * Called from doDeletion() during DROP BUFFER POOL.
 * Destroys the dynamic pool (DSM segment, trickle writer) and removes
 * the catalog row.
 */
void
DropBufferPoolById(Oid bpoid)
{
	BufferPoolDesc *pool;
	Relation	rel;
	HeapTuple	tup;

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
				 errmsg("cannot drop buffer pool during recovery")));

	/* Don't allow dropping the default pool */
	if (bpoid == DEFAULT_BUFFERPOOL_OID)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot drop the default buffer pool")));

	/*
	 * Check whether any relations reference this buffer pool via the
	 * buffer_pool reloption.  If so, refuse the DROP.
	 *
	 * We look up the pool name from the catalog, build the expected reloption
	 * string "buffer_pool=<name>", then scan pg_class for any tuple whose
	 * reloptions array contains a matching entry.
	 */
	{
		HeapTuple	bptup;
		Form_pg_bufferpool bpform;
		char	   *bpname;
		char		optprefix[NAMEDATALEN + 13];	/* "buffer_pool=" + name */
		Relation	classRel;
		TableScanDesc scan;
		HeapTuple	classtup;

		bptup = SearchSysCache1(BUFFERPOOLOID, ObjectIdGetDatum(bpoid));
		if (!HeapTupleIsValid(bptup))
			elog(ERROR, "cache lookup failed for buffer pool %u", bpoid);
		bpform = (Form_pg_bufferpool) GETSTRUCT(bptup);
		bpname = NameStr(bpform->bpname);

		snprintf(optprefix, sizeof(optprefix), "buffer_pool=%s", bpname);

		classRel = table_open(RelationRelationId, AccessShareLock);
		scan = table_beginscan_catalog(classRel, 0, NULL);

		while ((classtup = heap_getnext(scan, ForwardScanDirection)) != NULL)
		{
			Datum		optionsDatum;
			bool		isNull;

			optionsDatum = heap_getattr(classtup,
										Anum_pg_class_reloptions,
										RelationGetDescr(classRel),
										&isNull);
			if (!isNull)
			{
				ArrayType  *arr = DatumGetArrayTypeP(optionsDatum);
				Datum	   *elems;
				int			nelems;
				int			i;

				deconstruct_array_builtin(arr, TEXTOID, &elems, NULL, &nelems);
				for (i = 0; i < nelems; i++)
				{
					char	   *optstr = TextDatumGetCString(elems[i]);

					if (strcmp(optstr, optprefix) == 0)
					{
						Form_pg_class classForm = (Form_pg_class) GETSTRUCT(classtup);

						table_endscan(scan);
						table_close(classRel, AccessShareLock);
						ReleaseSysCache(bptup);
						ereport(ERROR,
								(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
								 errmsg("cannot drop buffer pool \"%s\" because relation \"%s\" depends on it",
										bpname, NameStr(classForm->relname)),
								 errhint("Remove the buffer_pool reloption from dependent relations first.")));
					}
					pfree(optstr);
				}
				pfree(elems);
			}
		}

		table_endscan(scan);
		table_close(classRel, AccessShareLock);
		ReleaseSysCache(bptup);
	}

	/* Destroy the dynamic pool if it exists in shared memory */
	pool = GetBufferPoolByOid(bpoid);
	if (pool != NULL)
		DestroyDynamicBufferPool(pool);

	/* Remove the catalog row */
	rel = table_open(BufferPoolRelationId, RowExclusiveLock);

	tup = SearchSysCache1(BUFFERPOOLOID, ObjectIdGetDatum(bpoid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for buffer pool %u", bpoid);

	CatalogTupleDelete(rel, &tup->t_self);
	ReleaseSysCache(tup);

	table_close(rel, RowExclusiveLock);
}

/*
 * get_bufferpool_oid - given a buffer pool name, look up the OID
 *
 * If missing_ok is false, throw an error if the pool is not found.
 * If true, just return InvalidOid.
 */
Oid
get_bufferpool_oid(const char *bpname, bool missing_ok)
{
	Oid			oid;

	oid = GetSysCacheOid1(BUFFERPOOLNAME, Anum_pg_bufferpool_oid,
						  CStringGetDatum(bpname));
	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("buffer pool \"%s\" does not exist", bpname)));
	return oid;
}
