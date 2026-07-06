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
#include "catalog/pg_type.h"
#include "commands/bufferpoolcmds.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "storage/bufpool.h"
#include "storage/bufpool_internals.h"
#include "storage/lmgr.h"
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
 * ComputeRemainderPoolSize
 *		Calculate the size for a REMAINDER pool.
 *
 * In the current architecture, dynamic pools (USER, RECYCLE) each allocate
 * their own DSM segments, while the DEFAULT pool owns shared_buffers.
 * The REMAINDER pool is sized to the portion of shared_buffers that is not
 * claimed by any user-created dynamic pool target.  The DEFAULT pool's own
 * target is excluded from the "claimed" tally since it represents the base
 * shared_buffers allocation.
 *
 * Returns the number of buffers available for the remainder pool.
 * Errors out if oversubscribed pools prevent creation.
 */
static int
ComputeRemainderPoolSize(void)
{
	int			total_budget = NBuffers;
	int			user_claimed = 0;
	int			i;

	for (i = 0; i < NBufferPools; i++)
	{
		BufferPoolDesc *pool = &BufferPoolDescs[i];

		if (!pool->bp_active)
			continue;

		/*
		 * Skip the DEFAULT pool -- its target is the base shared_buffers
		 * allocation, not a claim against the budget.  Also skip RECYCLE
		 * since it has its own dedicated GUC-sized allocation.
		 */
		if (pool->bp_kind == BUFPOOL_DEFAULT || pool->bp_kind == BUFPOOL_RECYCLE)
			continue;

		user_claimed += pool->bp_target_buffers;

		/* Reject if any user pool is oversubscribed */
		if (pool->bp_oversubscribed)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot create REMAINDER pool while pool \"%s\" is oversubscribed",
							NameStr(pool->bp_name)),
					 errhint("Wait for the trickle writer to reduce oversubscribed pools to their target size.")));
	}

	if (user_claimed >= total_budget)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no unclaimed buffer space available for REMAINDER pool"),
				 errdetail("Total budget is %d buffers, but user pools claim %d.",
						   total_budget, user_claimed)));

	return total_budget - user_claimed;
}

/*
 * CheckNoRemainderPoolExists
 *		Ensure no REMAINDER pool already exists.  Only one is allowed.
 *
 * A REMAINDER pool is identified by having bpsize == 0 in the catalog
 * (since its size is computed dynamically).
 */
static void
CheckNoRemainderPoolExists(void)
{
	Relation	bprel;
	TableScanDesc scan;
	HeapTuple	tup;

	bprel = table_open(BufferPoolRelationId, AccessShareLock);
	scan = table_beginscan_catalog(bprel, 0, NULL);

	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_bufferpool bpform = (Form_pg_bufferpool) GETSTRUCT(tup);

		/* Skip the default pool -- its bpsize=0 means "all of shared_buffers" */
		if (bpform->oid == DEFAULT_BUFFERPOOL_OID)
			continue;

		if (bpform->bpsize == 0)
		{
			table_endscan(scan);
			table_close(bprel, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("a REMAINDER pool already exists: \"%s\"",
							NameStr(bpform->bpname)),
					 errhint("Drop the existing REMAINDER pool first, or re-create it with the same name to recalculate its size.")));
		}
	}

	table_endscan(scan);
	table_close(bprel, AccessShareLock);
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

	/* Compute or parse size */
	if (stmt->is_remainder)
	{
		int			remainder_bufs;

		/*
		 * REMAINDER pool: size is automatically computed from the unclaimed
		 * buffer budget.  Only one REMAINDER pool is allowed at a time.
		 */
		CheckNoRemainderPoolExists();
		remainder_bufs = ComputeRemainderPoolSize();
		bpsize = (int64) remainder_bufs * BLCKSZ;

		ereport(NOTICE,
				(errmsg("REMAINDER pool \"%s\" sized to %d buffers (%lld bytes)",
						stmt->poolname, remainder_bufs,
						(long long) bpsize)));
	}
	else
	{
		bpsize = DatumGetInt64(DirectFunctionCall1(int8in,
												   CStringGetDatum(stmt->size)));
		if (bpsize <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("buffer pool size must be positive")));
	}

	/* Insert tuple into pg_bufferpool */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	bpoid = GetNewOidWithIndex(rel, BufferPoolOidIndexId,
							   Anum_pg_bufferpool_oid);
	values[Anum_pg_bufferpool_oid - 1] = ObjectIdGetDatum(bpoid);
	values[Anum_pg_bufferpool_bpname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->poolname));
	values[Anum_pg_bufferpool_bphandler - 1] = ObjectIdGetDatum(bphandler);

	/*
	 * Store bpsize=0 in catalog for REMAINDER pools as a marker so they can
	 * be identified.  The actual computed size is used only for the DSM
	 * allocation below.
	 */
	values[Anum_pg_bufferpool_bpsize - 1] =
		Int64GetDatum(stmt->is_remainder ? 0 : bpsize);

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
		bool		use_huge_pages = false;

		/* Parse WITH options understood at create time. */
		if (stmt->options != NIL)
		{
			ListCell   *lc;

			foreach(lc, stmt->options)
			{
				DefElem    *def = (DefElem *) lfirst(lc);

				if (strcmp(def->defname, "huge_pages") == 0)
					use_huge_pages = defGetBoolean(def);
				else if (strcmp(def->defname, "direct_io") == 0)
					 /* handled post-create via the descriptor; ignore here */ ;
				else
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("unrecognized buffer pool option \"%s\"",
									def->defname)));
			}
		}

		datum = OidFunctionCall0(bphandler);
		routine = (const BufferPoolRoutine *) DatumGetPointer(datum);

		pool_nbuffers = (int) (bpsize / BLCKSZ);
		if (pool_nbuffers < 16)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("buffer pool size must be at least %d bytes",
							16 * BLCKSZ)));

		CreateDynamicBufferPool(bpoid, stmt->poolname, pool_nbuffers, routine,
								bphandler, use_huge_pages);
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
			else if (strcmp(def->defname, "handler") == 0)
			{
				/*
				 * Online replacement-algorithm swap.  Resolve the new handler
				 * function, then quiesce + destroy + recreate the pool under
				 * the new routine (SwapDynamicBufferPoolAlgorithm).  Safe
				 * because DestroyDynamicBufferPool emits the detach barrier
				 * and waits for every backend to drop the pool before the old
				 * strategy state goes away -- the unsafe in-place memset that
				 * an earlier draft used (no quiescence) is gone.  The pool's
				 * cached pages are dropped (it is a cache), so expect a cold
				 * pool right after the swap.
				 */
				char	   *newhandler = defGetString(def);
				List	   *hname = list_make1(makeString(newhandler));
				Oid			funcargtypes[1] = {INTERNALOID};
				Oid			newhoid;

				if (!PoolIsDynamic(pool))
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("cannot change algorithm of buffer pool \"%s\": not a dynamic pool",
									stmt->poolname)));

				/*
				 * Handlers take one argument of type internal (see
				 * CreateBufferPool).
				 */
				newhoid = LookupFuncName(hname, 1, funcargtypes, false);
				if (get_func_rettype(newhoid) != INTERNALOID)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("function %s must return type %s",
									newhandler, "internal")));

				SwapDynamicBufferPoolAlgorithm(pool, newhoid);

				/* Update pg_bufferpool.bphandler to the new handler. */
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
					values[Anum_pg_bufferpool_bphandler - 1] = ObjectIdGetDatum(newhoid);
					replaces[Anum_pg_bufferpool_bphandler - 1] = true;
					newtup = heap_modify_tuple(oldtup, RelationGetDescr(bprel),
											   values, nulls, replaces);
					CatalogTupleUpdate(bprel, &newtup->t_self, newtup);
					ReleaseSysCache(oldtup);
					heap_freetuple(newtup);
					table_close(bprel, RowExclusiveLock);
				}
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
	 * Take an exclusive lock on the buffer pool object for the duration of
	 * the drop.  This serializes concurrent DROPs of the same pool and gives
	 * the reloption-dependency scan below a stable view: no other DROP can
	 * commit a change to this pool's catalog row or destroy its DSM while we
	 * hold it.  Combined with the PROCSIGNAL_BARRIER_BUFPOOL_DETACH
	 * quiescence inside DestroyDynamicBufferPool (which guarantees no backend
	 * is still using the pool's DSM when we tear it down), this closes the
	 * use-after-detach race.
	 */
	LockDatabaseObject(BufferPoolRelationId, bpoid, 0, AccessExclusiveLock);

	/*
	 * Check whether any relations reference this buffer pool via the
	 * buffer_pool reloption.  If so, refuse the DROP.
	 *
	 * ponytail: interim dependency check via a pg_class reloptions scan.  The
	 * fully idiomatic fix is a real pg_depend entry recorded when the
	 * buffer_pool reloption is set (in heap_create_with_catalog and
	 * ATExecSetRelOptions), which would let the standard dependency walker
	 * drive DROP and DROP ... CASCADE and would also catch a relation created
	 * concurrently with this DROP (a window this scan still leaves open).
	 * Upgrade path: record DEPENDENCY_NORMAL relation->pool and delete this
	 * scan.
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
