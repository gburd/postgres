/**
 * @file noxu_lsm.h
 * @brief LSM-tree level management for Noxu table access method.
 *
 * Implements a multi-level LSM-tree inspired by HanoiDB's fractal merge
 * strategy.  Writes land in a nursery (Level 0), cascade through levels
 * with A/B/X merge segments, and transition from row-oriented to columnar
 * format during merges.
 *
 * Level 1 uses row-oriented pages (line pointers → MinimalTuples).
 * Level 2+ data is in the existing B-tree attribute pages (columnar).
 * Compression happens at merge time when batches are large enough.
 *
 * Segments are page ranges within the existing relation file, tracked
 * by an LSM metadata page (pointed to from the metapage opaque area).
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/noxu_lsm.h
 */
#ifndef NOXU_LSM_H
#define NOXU_LSM_H

#include "access/noxu_internal.h"
#include "access/htup.h"

/*
 * Page type identifiers for LSM pages.
 *
 * These are stored in the opaque area's nx_page_id field, like all
 * other Noxu page types.
 */
#define NX_LSM_ROW_PAGE_ID		0xF089	/* Row-oriented segment page */
#define NX_LSM_META_PAGE_ID		0xF08A	/* LSM metadata page */

/*
 * Maximum number of LSM levels.  With default base_capacity=4096 and
 * doubling per level, 20 levels can hold ~4 billion rows.
 */
#define NX_LSM_MAX_LEVELS		20

/*
 * Segment identifiers.
 */
#define NX_LSM_SEG_NONE		'\0'
#define NX_LSM_SEG_A		'A'
#define NX_LSM_SEG_B		'B'
#define NX_LSM_SEG_X		'X'		/* merge-in-progress output */

/*
 * NXLSMRowPageOpaque - Opaque area for row-oriented segment pages.
 *
 * Stored in pd_special.  The page body uses standard line pointers
 * (ItemId array) pointing to MinimalTuple data, similar to a heap page.
 */
typedef struct NXLSMRowPageOpaque
{
	nxtid		first_tid;		/* First TID on this page (inclusive) */
	nxtid		last_tid;		/* Last TID on this page (inclusive) */
	BlockNumber next_page;		/* Next page in segment (InvalidBlockNumber if last) */
	uint16		level_num;		/* Which LSM level (1, 2, ...) */
	char		segment_id;		/* 'A', 'B', or 'X' */
	uint8		padding;
	uint16		nx_page_id;		/* Always NX_LSM_ROW_PAGE_ID */
} NXLSMRowPageOpaque;

#define NXLSMRowPageGetOpaque(page) \
	((NXLSMRowPageOpaque *) PageGetSpecialPointer(page))

/*
 * NXLSMSegmentDesc - Descriptor for a single segment within a level.
 *
 * A segment is a contiguous set of pages within the relation file,
 * tracking a batch of rows written by a nursery flush or merge.
 */
typedef struct NXLSMSegmentDesc
{
	BlockNumber first_block;	/* First page of segment */
	BlockNumber last_block;		/* Last page of segment */
	nxtid		first_tid;		/* First TID (inclusive) */
	nxtid		last_tid;		/* Last TID (inclusive) */
	int32		nrows;			/* Number of rows in segment */
	int32		npages;			/* Number of pages in segment */
	char		segment_id;		/* 'A', 'B', 'X', or '\0' (empty) */
	bool		is_columnar;	/* false=row-oriented, true=columnar (B-tree) */
	char		padding[2];
} NXLSMSegmentDesc;

/* Check if a segment descriptor is empty (no data) */
#define NXLSMSegmentIsEmpty(seg) ((seg)->segment_id == NX_LSM_SEG_NONE)

/*
 * NXLSMLevelDesc - Descriptor for a single LSM level.
 *
 * Each level holds up to two segments (A and B).  When both exist,
 * a merge produces X.  On completion, X either stays at this level
 * (as new A, clearing B) or promotes to the next level.
 */
typedef struct NXLSMLevelDesc
{
	int32		level_num;		/* 1-based level number */
	int32		capacity;		/* Max rows: base_capacity * 2^(level_num-1) */
	NXLSMSegmentDesc seg_a;
	NXLSMSegmentDesc seg_b;
	NXLSMSegmentDesc seg_x;	/* merge-in-progress output */
	bool		merge_active;	/* Is a merge currently in progress? */
	char		padding[3];
} NXLSMLevelDesc;

/*
 * NXLSMMetaPageData - On-disk LSM metadata stored in a dedicated page.
 *
 * Pointed to from NXMetaPageOpaque.nx_lsm_meta.  Contains the full
 * state of all LSM levels and their segments.
 */
typedef struct NXLSMMetaPageData
{
	int32		nlevels;		/* Number of active levels (0 = LSM not used) */
	int32		base_capacity;	/* Level 1 capacity (GUC: noxu.lsm_base_capacity) */
	NXLSMLevelDesc levels[NX_LSM_MAX_LEVELS];
} NXLSMMetaPageData;

/*
 * NXLSMMetaPageOpaque - Opaque area for the LSM metadata page.
 */
typedef struct NXLSMMetaPageOpaque
{
	uint16		nx_flags;
	uint16		nx_page_id;		/* Always NX_LSM_META_PAGE_ID */
} NXLSMMetaPageOpaque;

#define NXLSMMetaPageGetOpaque(page) \
	((NXLSMMetaPageOpaque *) PageGetSpecialPointer(page))

/*
 * NXLSMMetaCache - Backend-private cache of LSM metadata.
 *
 * Avoids reading the LSM metadata page on every operation.
 */
typedef struct NXLSMMetaCache
{
	BlockNumber meta_blk;		/* Block number of LSM metadata page */
	bool		valid;			/* Is this cache populated? */
	NXLSMMetaPageData data;		/* Cached copy of the metadata */
} NXLSMMetaCache;

/* GUC variables */
extern PGDLLIMPORT bool noxu_lsm_enabled;
extern PGDLLIMPORT int noxu_lsm_base_capacity;
extern PGDLLIMPORT int noxu_lsm_max_levels;

/* LSM metadata management */
extern BlockNumber nx_lsm_ensure_meta(Relation rel);
extern NXLSMMetaPageData *nx_lsm_get_meta(Relation rel);
extern void nx_lsm_meta_update(Relation rel, NXLSMMetaPageData *data);

/* Row-oriented segment page I/O */
extern void nx_lsm_write_row_pages(Relation rel,
									MinimalTuple *tuples,
									nxtid *tids,
									int nrows,
									int level_num,
									char segment_id,
									BlockNumber *first_blk_out,
									BlockNumber *last_blk_out,
									int *npages_out);
extern int nx_lsm_read_row_segment(Relation rel,
									NXLSMSegmentDesc *seg,
									nxtid *tids_out,
									Datum **col_datums_out,
									bool **col_isnulls_out,
									int start_offset,
									TupleDesc tupdesc);

/* Segment management */
extern void nx_lsm_free_segment_pages(Relation rel, NXLSMSegmentDesc *seg);
extern bool nx_lsm_assign_to_level(Relation rel, int level_num,
									NXLSMSegmentDesc *new_seg);
extern void nx_lsm_request_merge(Relation rel, int level_num);

/* Scan support */
extern bool nx_lsm_tid_in_segment(NXLSMSegmentDesc *seg, nxtid tid);

/*
 * NXCompressionPolicy - Per-level compression codec eligibility.
 *
 * Controls which compression codecs are tried at each LSM level.
 * When NULL is passed to nxbt_attr_create_item(), all codecs are
 * evaluated (current default behavior).  When non-NULL, only
 * allowed codecs are tried.
 *
 * Level 1 (row pages): No column compression (row-oriented format).
 * Level 2: Cheap codecs (FOR, Dict, bitpacking) for warm data.
 * Level 3+: All codecs for maximum compression on cold data.
 */
typedef struct NXCompressionPolicy
{
	bool		allow_chimp;		/* Float XOR compression */
	bool		allow_dod;			/* Delta-of-delta */
	bool		allow_for;			/* Frame of reference */
	bool		allow_dict;			/* Dictionary encoding */
	bool		allow_fsst;			/* FSST string compression */
	bool		allow_uuid_delta;	/* UUID v7 delta */
	bool		allow_shared_dict;	/* zstd shared dictionary */
	bool		allow_page_compress; /* zstd/LZ4/pglz page-level */
} NXCompressionPolicy;

/* Pre-defined policies for each level tier */
extern const NXCompressionPolicy nx_lsm_policy_level2;		/* cheap codecs */
extern const NXCompressionPolicy nx_lsm_policy_full;		/* all codecs */

/* Get the compression policy for a given level */
extern const NXCompressionPolicy *nx_lsm_get_level_policy(int level_num);

/*
 * NXSegmentZoneMap - Per-segment min/max statistics for predicate pushdown.
 *
 * Built during merge, stored alongside segment descriptors in the LSM
 * metadata.  Allows entire segments to be skipped during scan when the
 * query predicate doesn't overlap the segment's value range.
 */
#define NX_ZONEMAP_MAX_COLS		32	/* Max columns tracked per segment */

typedef struct NXSegmentZoneMap
{
	int			ncols;			/* Number of columns tracked */
	bool		valid;			/* Zone map has been populated */
	Datum		col_min[NX_ZONEMAP_MAX_COLS];
	Datum		col_max[NX_ZONEMAP_MAX_COLS];
	int32		col_null_count[NX_ZONEMAP_MAX_COLS];
} NXSegmentZoneMap;

/* Merge operations (noxu_merge_worker.c) */
extern void nx_lsm_merge_level(Relation rel, int level_num);
extern void nx_lsm_merge_all_pending(Relation rel);

/* Background merge worker (noxu_merge_worker.c) */
extern void NoxuMergeWorkerMain(Datum main_arg);
extern void NoxuMergeWorkerRegister(void);
extern void nx_lsm_merge_init_gucs(void);

/* GUC variables */
extern PGDLLIMPORT int noxu_lsm_merge_interval_ms;

/* Initialization */
extern void nx_lsm_init_gucs(void);

#endif							/* NOXU_LSM_H */
