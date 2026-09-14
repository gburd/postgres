/*-------------------------------------------------------------------------
 *
 * tidbitmap.h
 *	  PostgreSQL tuple-id (TID) bitmap package
 *
 * This module provides bitmap data structures that are spiritually
 * similar to Bitmapsets, but are specially adapted to store sets of
 * tuple identifiers (TIDs), or ItemPointers.  In particular, the division
 * of an ItemPointer into BlockNumber and OffsetNumber is catered for.
 * Also, since we wish to be able to store very large tuple sets in
 * memory with this data structure, we support "lossy" storage, in which
 * we no longer remember individual tuple offsets on a page but only the
 * fact that a particular page needs to be visited.
 *
 *
 * Copyright (c) 2003-2026, PostgreSQL Global Development Group
 *
 * src/include/nodes/tidbitmap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TIDBITMAP_H
#define TIDBITMAP_H

#include "storage/itemptr.h"
#include "utils/dsa.h"

/*
 * The maximum number of tuples per page is not large (typically 256 with
 * 8K pages, or 1024 with 32K pages).  So there's not much point in making
 * the per-page bitmaps variable size.  We just legislate that the size
 * is this:
 *
 * This ceiling must cover every table AM's line-pointer array, not just
 * heap's.  FLUX pages mix full tuples with small overflow-continuation
 * records and do not clamp offsets to MaxHeapTuplesPerPage (FluxPageAddTuple
 * omits PAI_IS_HEAP), so a FLUX page can legitimately place tuples at offsets
 * above MaxHeapTuplesPerPage (~408 vs 291 at 8K).  Index bitmap scans feed the
 * raw heap TIDs from the index into tbm_add_tuples() with no knowledge of the
 * underlying AM, so if this ceiling were only heap-sized, a FLUX bitmap scan
 * of a page with high offsets errors out with "tuple offset out of range".
 * Size the ceiling for the widest AM (FLUX).  The FLUX bound is spelled out
 * from page geometry here to avoid pulling FLUX's include chain into this
 * widely-included header; a StaticAssert in flux_handler.c keeps it honest
 * against the real MaxFluxItemsPerPage.
 *
 * FLUX_TBM_HDR_OPAQUE == MAXALIGN(SizeOfPageHeaderData(24)) +
 *   MAXALIGN(sizeof(FluxPageOpaqueData)(8)) == 32
 * FLUX_TBM_TUPPITCH == MAXALIGN(sizeof(FluxOverflowRecordHeader)(16)) +
 *   sizeof(ItemIdData)(4) == 20
 */
#define FLUX_TBM_HDR_OPAQUE		32
#define FLUX_TBM_TUPPITCH		20
#define MaxFluxTuplesPerBitmapPage \
	((int) ((BLCKSZ - FLUX_TBM_HDR_OPAQUE) / FLUX_TBM_TUPPITCH))
#define TBM_MAX_TUPLES_PER_PAGE  \
	(MaxHeapTuplesPerPage > MaxFluxTuplesPerBitmapPage ? \
	 MaxHeapTuplesPerPage : MaxFluxTuplesPerBitmapPage)

/*
 * Actual bitmap representation is private to tidbitmap.c.  Callers can
 * do IsA(x, TIDBitmap) on it, but nothing else.
 */
typedef struct TIDBitmap TIDBitmap;

/* Likewise, TBMPrivateIterator is private */
typedef struct TBMPrivateIterator TBMPrivateIterator;
typedef struct TBMSharedIterator TBMSharedIterator;

/*
 * Callers with both private and shared implementations can use this unified
 * API.
 */
typedef struct TBMIterator
{
	bool		shared;
	union
	{
		TBMPrivateIterator *private_iterator;
		TBMSharedIterator *shared_iterator;
	}			i;
} TBMIterator;

/* Result structure for tbm_iterate */
typedef struct TBMIterateResult
{
	BlockNumber blockno;		/* block number containing tuples */

	bool		lossy;

	/*
	 * Whether or not the tuples should be rechecked. This is always true if
	 * the page is lossy but may also be true if the query requires recheck.
	 */
	bool		recheck;

	/*
	 * Pointer to the page containing the bitmap for this block. It is a void *
	 * to avoid exposing the details of the tidbitmap PagetableEntry to API
	 * users.
	 */
	void	   *internal_page;
} TBMIterateResult;

/* function prototypes in nodes/tidbitmap.c */

extern TIDBitmap *tbm_create(Size maxbytes, dsa_area *dsa);
extern void tbm_free(TIDBitmap *tbm);
extern void tbm_free_shared_area(dsa_area *dsa, dsa_pointer dp);

extern void tbm_add_tuples(TIDBitmap *tbm,
						   const ItemPointerData *tids, int ntids,
						   bool recheck);
extern void tbm_add_page(TIDBitmap *tbm, BlockNumber pageno);

extern void tbm_union(TIDBitmap *a, const TIDBitmap *b);
extern void tbm_intersect(TIDBitmap *a, const TIDBitmap *b);

extern int	tbm_extract_page_tuple(TBMIterateResult *iteritem,
								   OffsetNumber *offsets,
								   uint32 max_offsets);

extern bool tbm_is_empty(const TIDBitmap *tbm);

extern TBMPrivateIterator *tbm_begin_private_iterate(TIDBitmap *tbm);
extern dsa_pointer tbm_prepare_shared_iterate(TIDBitmap *tbm);
extern bool tbm_private_iterate(TBMPrivateIterator *iterator, TBMIterateResult *tbmres);
extern bool tbm_shared_iterate(TBMSharedIterator *iterator, TBMIterateResult *tbmres);
extern void tbm_end_private_iterate(TBMPrivateIterator *iterator);
extern void tbm_end_shared_iterate(TBMSharedIterator *iterator);
extern TBMSharedIterator *tbm_attach_shared_iterate(dsa_area *dsa,
													dsa_pointer dp);
extern int	tbm_calculate_entries(Size maxbytes);

extern TBMIterator tbm_begin_iterate(TIDBitmap *tbm,
									 dsa_area *dsa, dsa_pointer dsp);
extern void tbm_end_iterate(TBMIterator *iterator);

extern bool tbm_iterate(TBMIterator *iterator, TBMIterateResult *tbmres);

static inline bool
tbm_exhausted(TBMIterator *iterator)
{
	/*
	 * It doesn't matter if we check the private or shared iterator here. If
	 * tbm_end_iterate() was called, they will be NULL
	 */
	return !iterator->i.private_iterator;
}

#endif							/* TIDBITMAP_H */
