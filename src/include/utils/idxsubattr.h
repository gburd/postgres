/*-------------------------------------------------------------------------
 *
 * idxsubattr.h
 *    Data structures for indexed-subattr tracking on sub-attribute-aware
 *    types (JSONB, XML, etc.).  Used by the relcache, executor, and
 *    type-specific extract/compare functions.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/utils/idxsubattr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IDXSUBATTR_H
#define IDXSUBATTR_H

#include "access/attnum.h"
#include "fmgr.h"
#include "nodes/bitmapset.h"
#include "postgres_ext.h"

/* Forward declarations */
typedef struct RelationData *Relation;

/*
 * IdxSubattrDesc - one subattr descriptor extracted from one expression
 * index column.
 *
 * 'descriptor' is a type-specific opaque varlena Datum.  For JSONB it is
 * a text[] of path elements (e.g., {"a","b"} for data->'a'->'b').  For
 * XML it is a text containing an XPath string.
 *
 * Stored in CacheMemoryContext as part of RelSubattrInfo.
 */
typedef struct IdxSubattrDesc
{
	Datum		descriptor;		/* type-specific varlena, in
								 * CacheMemoryContext */
	Oid			indexoid;		/* source index OID (diagnostic only) */
	int			indexcol;		/* source index column, 0-based */
} IdxSubattrDesc;

/*
 * SubattrInfo - all indexed subattr descriptors for one base-table
 * attribute, plus the cached typidxcompare FmgrInfo for runtime use.
 */
typedef struct SubattrInfo
{
	AttrNumber	attnum;			/* base table attribute number */
	Oid			typoid;			/* pg_type OID of the attribute */
	int			ndescriptors;	/* length of descriptors[] */
	IdxSubattrDesc *descriptors;	/* array, in CacheMemoryContext */
	FmgrInfo	comparefn;		/* cached pg_type.typidxcompare */
	bool		has_comparefn;	/* false if typidxcompare is InvalidOid */
} SubattrInfo;

/*
 * RelSubattrInfo - per-relation cache of all indexed-subattr info.
 * Stored in RelationData.rd_idxsubattrs.  NULL when the relation has
 * no expression indexes on sub-attribute-aware types.
 *
 * subattr_attrs uses the FirstLowInvalidHeapAttributeNumber offset
 * convention, consistent with RelationGetIndexAttrBitmap().
 */
typedef struct RelSubattrInfo
{
	int			nattrs;			/* length of attrs[] */
	SubattrInfo *attrs;			/* array, NOT indexed by attnum */
	Bitmapset  *subattr_attrs;	/* quick membership test for attnums */

	/*
	 * Attnums referenced by at least one simple (non-expression) index
	 * column.  Used to exclude attributes from the subattr optimization: if
	 * an attribute has both expression and simple index references, any byte
	 * change triggers an index update for the simple index, so the subattr
	 * check cannot avoid the update.
	 *
	 * Same offset convention as subattr_attrs.
	 */
	Bitmapset  *simple_indexed_attrs;
} RelSubattrInfo;


/*
 * Ensure rd_idxsubattrs is populated (lazy build).  Returns the
 * cached pointer, which may be NULL if no subattr indexes exist.
 */
extern RelSubattrInfo *RelationGetIdxSubattrs(Relation rel);

/*
 * Does this attribute have any expression-index subattr descriptors?
 */
extern bool attr_has_subattr_indexes(Relation rel, AttrNumber attnum);

/*
 * Does this attribute have subattr descriptors AND is NOT referenced?
 * by any simple (whole-column) index.
 */
extern bool attr_subattr_only(Relation rel, AttrNumber attnum);

/*
 * Look up the SubattrInfo for a specific attribute.
 * Returns NULL if the attribute has no subattr indexes.
 */
extern SubattrInfo *RelationGetSubattrInfo(Relation rel,
										   AttrNumber attnum);

/*
 * Free rd_idxsubattrs (called during relcache invalidation).
 */
extern void FreeIdxSubattrs(RelSubattrInfo *info);

#endif							/* IDXSUBATTR_H */
