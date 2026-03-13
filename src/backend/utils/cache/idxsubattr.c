/*-------------------------------------------------------------------------
 *
 * idxsubattr.c
 *    Public API for the per-relation indexed-subattr cache
 *    (RelationData.rd_idxsubattrs).
 *
 *    The main build function, RelationBuildIdxSubattrs(), and its helpers
 *    live in relcache.c alongside RelationGetIndexAttrBitmap() so that
 *    they follow the same locking and restart patterns for safe
 *    interaction with CREATE INDEX CONCURRENTLY.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/cache/idxsubattr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "utils/idxsubattr.h"
#include "utils/rel.h"


/* Defined in relcache.c */
extern void RelationBuildIdxSubattrs(Relation rel);


/*
 * RelationGetIdxSubattrs
 *
 * Public entry point: ensure rd_idxsubattrs is populated and return it.
 * May return NULL if there are no subattr indexes.
 */
RelSubattrInfo *
RelationGetIdxSubattrs(Relation rel)
{
	if (!rel->rd_idxsubattrsvalid)
		RelationBuildIdxSubattrs(rel);
	return rel->rd_idxsubattrs;
}


/*
 * attr_has_subattr_indexes
 *
 * Returns true if the specified attribute has at least one expression-index
 * subattr descriptor.
 */
bool
attr_has_subattr_indexes(Relation rel, AttrNumber attnum)
{
	RelSubattrInfo *info = RelationGetIdxSubattrs(rel);

	if (info == NULL)
		return false;

	return bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber,
						 info->subattr_attrs);
}


/*
 * attr_subattr_only
 *
 * Returns true if the attribute has subattr descriptors AND is NOT
 * referenced by any simple (whole-column) index.
 */
bool
attr_subattr_only(Relation rel, AttrNumber attnum)
{
	RelSubattrInfo *info = RelationGetIdxSubattrs(rel);
	int			offset;

	if (info == NULL)
		return false;

	offset = attnum - FirstLowInvalidHeapAttributeNumber;

	return bms_is_member(offset, info->subattr_attrs) &&
		!bms_is_member(offset, info->simple_indexed_attrs);
}


/*
 * RelationGetSubattrInfo
 *
 * Look up the SubattrInfo for a specific attribute.
 * Returns NULL if the attribute has no subattr indexes.
 */
SubattrInfo *
RelationGetSubattrInfo(Relation rel, AttrNumber attnum)
{
	RelSubattrInfo *info = RelationGetIdxSubattrs(rel);
	int			i;

	if (info == NULL)
		return NULL;

	for (i = 0; i < info->nattrs; i++)
	{
		if (info->attrs[i].attnum == attnum)
			return &info->attrs[i];
	}

	return NULL;
}


/*
 * FreeIdxSubattrs
 *
 * Free a RelSubattrInfo structure (called during relcache invalidation).
 */
void
FreeIdxSubattrs(RelSubattrInfo *info)
{
	int			i,
				j;

	if (info == NULL)
		return;

	for (i = 0; i < info->nattrs; i++)
	{
		SubattrInfo *attr = &info->attrs[i];

		for (j = 0; j < attr->ndescriptors; j++)
		{
			pfree(DatumGetPointer(attr->descriptors[j].descriptor));
		}
		pfree(attr->descriptors);
	}

	bms_free(info->subattr_attrs);
	bms_free(info->simple_indexed_attrs);
	pfree(info->attrs);
	pfree(info);
}
