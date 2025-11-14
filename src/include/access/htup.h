/*-------------------------------------------------------------------------
 *
 * htup.h
 *	  POSTGRES heap tuple definitions.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/htup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HTUP_H
#define HTUP_H

#include "catalog/pg_aggregate_d.h"
#include "nodes/bitmapset.h"
#include "storage/itemptr.h"

/* typedefs and forward declarations for structs defined in htup_details.h */

typedef struct HeapTupleHeaderData HeapTupleHeaderData;

typedef HeapTupleHeaderData *HeapTupleHeader;

typedef struct MinimalTupleData MinimalTupleData;

typedef MinimalTupleData *MinimalTuple;


/*
 * HeapTupleData is an in-memory data structure that points to a tuple.
 *
 * There are several ways in which this data structure is used:
 *
 * * Pointer to a tuple in a disk buffer: t_data points directly into the
 *	 buffer (which the code had better be holding a pin on, but this is not
 *	 reflected in HeapTupleData itself).
 *
 * * Pointer to nothing: t_data is NULL.  This is used as a failure indication
 *	 in some functions.
 *
 * * Part of a palloc'd tuple: the HeapTupleData itself and the tuple
 *	 form a single palloc'd chunk.  t_data points to the memory location
 *	 immediately following the HeapTupleData struct (at offset HEAPTUPLESIZE).
 *	 This is the output format of heap_form_tuple and related routines.
 *
 * * Separately allocated tuple: t_data points to a palloc'd chunk that
 *	 is not adjacent to the HeapTupleData.  (This case is deprecated since
 *	 it's difficult to tell apart from case #1.  It should be used only in
 *	 limited contexts where the code knows that case #1 will never apply.)
 *
 * * Separately allocated minimal tuple: t_data points MINIMAL_TUPLE_OFFSET
 *	 bytes before the start of a MinimalTuple.  As with the previous case,
 *	 this can't be told apart from case #1 by inspection; code setting up
 *	 or destroying this representation has to know what it's doing.
 *
 * t_len should always be valid, except in the pointer-to-nothing case.
 * t_self and t_tableOid should be valid if the HeapTupleData points to
 * a disk buffer, or if it represents a copy of a tuple on disk.  They
 * should be explicitly set invalid in manufactured tuples.
 */
typedef struct HeapTupleData
{
	uint32		t_len;			/* length of *t_data */
	ItemPointerData t_self;		/* SelfItemPointer */
	Oid			t_tableOid;		/* table the tuple came from */
#define FIELDNO_HEAPTUPLEDATA_DATA 3
	HeapTupleHeader t_data;		/* -> tuple header and data */
} HeapTupleData;

typedef HeapTupleData *HeapTuple;

#define HEAPTUPLESIZE	MAXALIGN(sizeof(HeapTupleData))

/*
 * Accessor macros to be used with HeapTuple pointers.
 */
#define HeapTupleIsValid(tuple) ((tuple) != NULL)

#define HeapTupleSetValue(table_name, field, value, values) \
	(values)[Anum_##table_name##_##field - 1] = (value)

#define HeapTupleSetValueNull(table_name, field, values, nulls) \
	do { \
		(values)[Anum_##table_name##_##field - 1] = (Datum) 0; \
		(nulls)[Anum_##table_name##_##field - 1] = true; \
	} while(0)

/*
 * The following should be used when manipulating CatalogTuples for
 * insert or update.
 */

#define CatalogInsertValuesDecl(table_name, var) \
	struct _cat_##table_name##_##var##_ins_vals { \
		CatalogIndexState	idx; \
		bool	nulls[Natts_##table_name]; \
		Datum	values[Natts_##table_name]; \
	}
#define CatalogInsertValuesContext(table_name, var) \
	CatalogInsertValuesDecl(table_name, var) _##table_name##_##var = { \
		.idx	= NULL, \
		.nulls	= {false}, \
		.values	= {0} \
	}, *var = &_##table_name##_##var

#define CatalogUpdateValuesDecl(table_name, var) \
	struct _cat_##table_name##_##var##_upd_vals { \
		CatalogIndexState	idx; \
		bool		nulls[Natts_##table_name]; \
		Datum		values[Natts_##table_name]; \
		Bitmapset	*updated; \
	}
#define CatalogUpdateValuesContext(table_name, var) \
	CatalogUpdateValuesDecl(table_name, var) _##table_name##_##var = { \
		.idx		= NULL, \
		.nulls		= {false}, \
		.values		= {0}, \
		.updated	= NULL \
	}, *var = &_##table_name##_##var

#define CatalogFormContext(table_name, var, tuple) \
	Form_##table_name var = (Form_##table_name) GETSTRUCT(tuple)

#define CatalogInsertFormDecl(table_name, var) \
	struct _cat_##table_name##_##var##_ins_form { \
		CatalogIndexState	idx; \
		Form_##table_name	form; \
		bool				nulls[Natts_##table_name]; \
	}
#define CatalogInsertFormContext(table_name, var) \
	CatalogInsertFormDecl(table_name, var) _##table_name##_##var = { \
		.idx		= NULL, \
		.form		= NULL, \
		.nulls		= {false} \
	}, *var = &_##table_name##_##var

#define CatalogUpdateFormDecl(table_name, var) \
	struct _cat_##table_name##_##var##_upd_form { \
		CatalogIndexState	idx; \
		Form_##table_name	form; \
		bool				nulls[Natts_##table_name]; \
		Bitmapset			*updated; \
	}
#define CatalogUpdateFormContext(table_name, var) \
	CatalogUpdateFormDecl(table_name, var) _##table_name##_##var = { \
		.idx		= NULL, \
		.form		= NULL, \
		.nulls		= {false}, \
		.updated	= NULL \
	}, *var = &_##table_name##_##var

#define CatalogSetForm(table_name, var, tuple) \
	var->form = (Form_##table_name) GETSTRUCT(tuple)

#define CatalogGetFormField(var, field) var->form->field

#define CatalogTupleValue(var, table_name, field) \
	var->values[Anum_##table_name##_##field - 1]

#define FormCatalogTuple(relation, var) \
	heap_form_tuple(RelationGetDescr(relation), var->values, var->nulls)

#define DeformCatalogTuple(relation, tuple, var) \
	heap_deform_tuple(tuple, RelationGetDescr(relation), var->values, var->nulls)

#define ModifyCatalogTupleValues(relation, tuple, var) \
	do { \
		HeapTuple new; \
		\
		new = heap_update_tuple(tuple, RelationGetDescr(relation), \
								var->values, var->nulls, var->updated); \
		CatalogTupleUpdate(relation, &new->t_self, new, var->updated, var->idx); \
		heap_freetuple(new); \
	} while(0)

#define UpdateCatalogTupleField(relation, tuple, var) \
	CatalogTupleUpdate(relation, &tuple->t_self, tuple, var->updated, var->idx)

#define ModifyCatalogTupleField(relation, tuple, var) \
	do { \
		HeapTuple new; \
		\
		new = heap_update_tuple(tuple, RelationGetDescr(relation), \
								NULL, var->nulls, var->updated); \
		CatalogTupleUpdate(relation, &new->t_self, new, var->updated, var->idx); \
		heap_freetuple(new); \
	} while(0)

#define InsertCatalogTupleValues(relation, var) \
	do { \
		HeapTuple new = heap_form_tuple(RelationGetDescr(relation), \
								  var->values, var->nulls); \
		CatalogTupleInsert(relation, new, var->idx); \
		heap_freetuple(new); \
	} while(0)

#define CatalogTupleSetField(var, table_name, field, value) \
	var->field = (value)

#define CatalogTupleSetFieldNull(var, table_name, field) \
		var->nulls[Anum_##table_name##_##field - 1] = true

#define CatalogTupleSetValue(var, table_name, field, value) \
	do { \
		var->values[Anum_##table_name##_##field - 1] = (value); \
		var->nulls[Anum_##table_name##_##field - 1] = false; \
	} while(0)

#define CatalogTupleSetValueNull(var, table_name, field) \
	var->nulls[Anum_##table_name##_##field - 1] = true

#define CatalogTupleUpdateValue(var, table_name, field, value) \
	do { \
		var->values[Anum_##table_name##_##field - 1] = (value); \
		var->nulls[Anum_##table_name##_##field - 1] = false; \
		var->updated = bms_add_member(var->updated, \
								Anum_##table_name##_##field - \
								FirstLowInvalidHeapAttributeNumber); \
	} while(0)

#define CatalogTupleUpdateValueNull(var, table_name, field) \
	do { \
		var->nulls[Anum_##table_name##_##field - 1] = true; \
		var->updated = bms_add_member(var->updated, \
								Anum_##table_name##_##field - \
								FirstLowInvalidHeapAttributeNumber); \
	} while(0)

#define CatalogTupleUpdateField(var, table_name, field, value) \
	do { \
		var->form->field = (value); \
		var->nulls[Anum_##table_name##_##field - 1] = false; \
		var->updated = bms_add_member(var->updated, \
								Anum_##table_name##_##field - \
								FirstLowInvalidHeapAttributeNumber); \
	} while(0)

#define CatalogTupleCondUpdateField(var, table_name, field, cond_set_value, stmt) \
	if (!(cond_set_value)) \
	{ \
		var->nulls[Anum_##table_name##_##field - 1] = false; \
		var->updated = bms_add_member(var->updated, \
								Anum_##table_name##_##field - \
								FirstLowInvalidHeapAttributeNumber); \
	} else stmt

#define CatalogTupleUpdateStrField(var, table_name, field, value) \
	do { \
		namestrcpy(&(var->form->field), value); \
		var->nulls[Anum_##table_name##_##field - 1] = false; \
		var->updated = bms_add_member(var->updated, \
								Anum_##table_name##_##field - \
								FirstLowInvalidHeapAttributeNumber); \
	} while(0)

#define CatalogTupleUpdateFieldNull(var, table_name, field) \
	do { \
		var->nulls[Anum_##table_name##_##field - 1] = true; \
		var->updated = bms_add_member(var->updated, \
								Anum_##table_name##_##field - \
								FirstLowInvalidHeapAttributeNumber); \
	} while(0)

#define CatalogTupleUpdateMarkAllColumnsUpdated(var, table_name) \
	var->updated = bms_add_range(var->updated, 1 - FirstLowInvalidHeapAttributeNumber, \
						  Natts_##table_name - FirstLowInvalidHeapAttributeNumber)

#define CatalogTupleReuseUpdateContext(var) \
	do { \
		bms_free(var->updated); \
		var->updated = NULL; \
	} while(0)

#define CatalogTupleHasChanged(var) (!bms_is_empty(var->updated))



/* TODO BELOW */

#define HeapTupleValue(table_name, field, values) \
	(values)[Anum_##table_name##_##field - 1]

#define HeapTupleSetField(table_name, field, value, form_ptr) \
	(form_ptr)->field = (value)

#define HeapTupleSetFieldNull(table_name, field, nulls) \
	(nulls)[Anum_##table_name##_##field - 1] = true

/*
 * These are useful when forming tuples for CatalogTupleUpdate()
 *
 * Updated catalog tuples need to track which fields were changed when
 * calling  heap_update_tuple(), so we use a bitmap to keep track of that.
 */
#define HeapTupleMarkColumnUpdated(table_name, field, updated) \
	(updated) = bms_add_member((updated), \
		Anum_##table_name##_##field - FirstLowInvalidHeapAttributeNumber)

#define HeapTupleUpdateSetAllColumnsUpdated(table_name, updated) \
	(updated) = bms_add_range((updated), 1 - FirstLowInvalidHeapAttributeNumber, \
						  Natts_##table_name - FirstLowInvalidHeapAttributeNumber)

#define HeapTupleSetColumnNotUpdated(table_name, field, updated) \
	(updated) = bms_del_member((updated), \
		Anum_##table_name##_##field - FirstLowInvalidHeapAttributeNumber)

#define HeapTupleUpdateField(table_name, field, value, form_ptr, updated) \
	do { \
		(form_ptr)->field = (value); \
		HeapTupleMarkColumnUpdated(table_name, field, updated); \
	} while(0)

#define HeapTupleUpdateValue(table_name, field, value, values, nulls, updated) \
	do { \
		(values)[Anum_##table_name##_##field - 1] = (Datum) (value); \
		(nulls)[Anum_##table_name##_##field - 1] = false; \
		HeapTupleMarkColumnUpdated(table_name, field, updated); \
	} while(0)

#define HeapTupleUpdateValueNull(table_name, field, values, nulls, updated) \
	do { \
		(values)[Anum_##table_name##_##field - 1] = (Datum) 0; \
		(nulls)[Anum_##table_name##_##field - 1] = true; \
		HeapTupleMarkColumnUpdated(table_name, field, updated); \
	} while(0)

/* HeapTupleHeader functions implemented in utils/time/combocid.c */
extern CommandId HeapTupleHeaderGetCmin(const HeapTupleHeaderData *tup);
extern CommandId HeapTupleHeaderGetCmax(const HeapTupleHeaderData *tup);
extern void HeapTupleHeaderAdjustCmax(const HeapTupleHeaderData *tup,
									  CommandId *cmax, bool *iscombo);

/* Prototype for HeapTupleHeader accessors in heapam.c */
extern TransactionId HeapTupleGetUpdateXid(const HeapTupleHeaderData *tup);

#endif							/* HTUP_H */
