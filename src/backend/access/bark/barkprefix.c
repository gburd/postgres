/*-------------------------------------------------------------------------
 *
 * barkprefix.c
 *	  Leaf key prefix compression for BARK indexes.
 *
 * A leaf of an index whose prefix_compression reloption is on may be flagged
 * BARK_PREFIX: its data items are stored "coded" against per-page group
 * anchors, dropping the leading bytes each shares with its anchor.  The
 * on-disk format is described at BARK_PREFIX in bark.h; the README covers how
 * the rest of BARK uses it.  The design is modeled on nbtree's nbtprefix.c
 * (the "group-directory" coder); the one thing BARK adds is its richer entry
 * shapes, whose bodies (a LIST locator array, a POSTING sbm blob) follow the
 * coded key just as they follow the plain key, with the entry's body offset in
 * t_tid adjusted to the coded position.
 *
 * Everything here works on pages and tuples alone, never the catalog, so WAL
 * redo can lay out a coded page exactly as the primary did.  A page gets its
 * directory when it is written as a whole (CREATE INDEX, either half of a
 * split); an insert codes the new tuple against an existing group, sharing
 * however much it happens to share, and deduplication re-codes against the
 * page's existing directory.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/barkprefix.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bark.h"
#include "access/itup.h"
#include "access/tupmacs.h"
#include "storage/bufpage.h"
#include "utils/rel.h"
#include "varatt.h"

/* In-memory anchor: the group's first member, as a plain tuple. */
typedef struct BarkPrefixAnchor
{
	IndexTuple	rep;			/* representative tuple owning the shared
								 * bytes */
} BarkPrefixAnchor;

/* In-memory directory of a page being laid out by bark_prefix_form_page. */
typedef struct BarkPrefixDir
{
	int			ngroups;
	BarkPrefixAnchor anchors[FLEXIBLE_ARRAY_MEMBER];
} BarkPrefixDir;

/*
 * Where two tuples first diverge inside one key column.  D (lead_shared) is
 * the data-area offset of that divergent column, so [0..D) is byte-identical
 * between the two.  sv (shared_len) is the divergent column's common value-byte
 * prefix, after its length header.  Decode and coding carry both through
 * BarkPrefixCode.
 */
typedef struct BarkPrefixSplit
{
	Size		lead_shared;	/* D: data-area bytes of fully-shared cols */
	Size		shared_len;		/* sv: divergent col's shared value bytes */
} BarkPrefixSplit;

/*
 * Validated splice offsets of a coded member, filled by
 * bark_prefix_check_member and consumed by bark_prefix_decode so the two
 * cannot disagree: D, sv, the member's and the anchor's divergent-column
 * prefix widths (mhdr, ahdr), and the member's stored residual length.  All
 * are bounded against the member and the anchor before use.
 */
typedef struct BarkPrefixSplice
{
	Size		lead_shared;	/* D */
	Size		shared_len;		/* sv */
	Size		mhdr;			/* member's divergent-column prefix */
	Size		ahdr;			/* anchor's divergent-column prefix */
	Size		residual;		/* member's stored residual bytes */
} BarkPrefixSplice;

/* BARK_PREFIX_ATTR_BYVAL and the on-disk structs live in bark.h. */

/*
 * A coded member is never larger than its plain form by more than
 * BARK_PREFIX_CODE_OVERHEAD, which admission reserves before coding.  The
 * worst case keeps a MAXALIGN'd code header and the body at its own MAXALIGN
 * boundary: MAXALIGN(code header + key) plus the body exceeds the plain
 * MAXALIGN(key) + body by at most the code header plus one body alignment.
 */
StaticAssertDecl(BARK_PREFIX_CODE_OVERHEAD >=
				 MAXALIGN(sizeof(BarkPrefixCode)) + MAXIMUM_ALIGNOF,
				 "BARK_PREFIX_CODE_OVERHEAD too small for the coded member header");

/*
 * bark_prefix_enabled
 *		Is prefix compression turned on for this index?
 *
 * Unlike BARK's former single-prefix coder, which required a varlena first
 * key column, the group-directory coder works for any type or collation and
 * for multi-column keys, so the only gate is the reloption.
 */
bool
bark_prefix_enabled(Relation index)
{
	return BarkGetPrefixCompression(index);
}

/*
 * Byte offset at which a leaf entry's body (a LIST locator array, a POSTING
 * sbm blob) begins, which is the end of its key part: the body offset of an
 * alt-TID entry, the whole size of a SINGLE.  Coding drops only key bytes, so
 * it carries the body unchanged past the coded key.
 */
static inline Size
bark_prefix_keyend(IndexTuple itup)
{
	if (!BarkEntryIsAltTID(itup))
		return IndexTupleSize(itup);
	return BarkEntryGetBodyOffset(itup);
}

/*
 * Byte offset of the anchor-offset array within a directory item holding
 * `natts' layout records.  The layout precedes it; the anchors follow it.
 */
static inline Size
bark_prefix_anchoroff_base(int natts)
{
	return offsetof(BarkPrefixDirHdr, attrs) + sizeof(BarkPrefixAttr) * natts;
}

/* The anchor-offset array of a directory header (natts layout records). */
static inline uint16 *
bark_prefix_anchoroff(BarkPrefixDirHdr *hdr)
{
	return (uint16 *) ((char *) hdr + bark_prefix_anchoroff_base(hdr->natts));
}

/*
 * Size of a directory item for `ngroups' groups and `natts' key-column layout
 * records, whose MAXALIGN'd anchor blobs total `anchorsz' bytes.
 */
static inline Size
bark_prefix_diritem_size(int natts, int ngroups, Size anchorsz)
{
	return MAXALIGN(bark_prefix_anchoroff_base(natts) +
					sizeof(uint16) * ngroups) + anchorsz;
}

/*
 * Width of the length header of a key column whose value begins at `data'.
 *
 * `attr' is the column's stored layout.  A varlena column's header is 1 or 4
 * bytes, read from `data'; any fixed-length or by-value column has none.  The
 * column walk uses this to step over each column's header.
 */
static inline Size
bark_prefix_header_width(const BarkPrefixAttr * attr, const char *data)
{
	if (attr->attlen != -1 || (attr->flags & BARK_PREFIX_ATTR_BYVAL) != 0)
		return 0;
	return VARATT_IS_1B(data) ? VARHDRSZ_SHORT : VARHDRSZ;
}

/*
 * Advance `off' (a data-area byte offset) past key column `i' of a tuple whose
 * data area starts at `data' and whose null bitmap is `bitmap' (NULL when the
 * tuple has no nulls).  Sets *valoff to the column's value offset (after any
 * length header) and *vallen to its value length; a null column occupies no
 * bytes and reports *vallen 0.  `attrs'/`natts' is the stored layout, `limit'
 * the data-area length (a bound so a malformed length cannot walk off the
 * tuple).  Returns false if column `i' does not fit within `limit'.
 */
static inline bool
bark_prefix_column(const BarkPrefixAttr * attrs, int natts, int i,
				   const char *data, const uint8 *bitmap, Size limit,
				   Size *off, Size *valoff, Size *vallen)
{
	const		BarkPrefixAttr *attr = &attrs[i];
	Size		start;
	Size		hdr;
	Size		len;

	if (bitmap != NULL && att_isnull(i, bitmap))
	{
		*valoff = *off;
		*vallen = 0;
		return true;			/* a null column occupies no data bytes */
	}

	/* align the column start, honoring the packed-varlena rule */
	if (attr->attlen == -1 && *off < limit && VARATT_NOT_PAD_BYTE(data + *off))
		start = *off;
	else
		start = att_nominal_alignby(*off, attr->attalignby);
	if (start > limit)
		return false;

	if (attr->attlen > 0)
	{
		hdr = 0;
		len = attr->attlen;
	}
	else if (attr->attlen == -1)
	{
		if (start >= limit)
			return false;		/* no room for a length header */
		hdr = bark_prefix_header_width(attr, data + start);
		if (start + hdr > limit)
			return false;
		len = VARSIZE_ANY(data + start);
	}
	else
	{
		Assert(attr->attlen == -2); /* cstring */
		hdr = 0;
		len = strnlen(data + start, limit - start) + 1;
	}

	if (start + len > limit)
		return false;
	*valoff = start + hdr;
	*vallen = len - hdr;
	*off = start + len;
	return true;
}

/*
 * Walk `rep' and `itup' column by column and set *split to where their key
 * values first diverge: the data-area offset D of the first column whose value
 * bytes differ, and sv the common value-byte prefix of that column.
 *
 * Every column before the divergent one has byte-identical value bytes in both
 * tuples; a column equal by value has equal length, hence an identical length
 * header and identical alignment padding, so [0..D) is byte-identical and can
 * be shared whole from the anchor.  The divergent column is the last shared
 * one, so re-inserting its (possibly different-width) header per member cannot
 * shift the alignment of any shared column.  Without a layout (`attrs' NULL),
 * it shares the whole data area: D = 0 and sv = the common byte prefix from
 * offset 0.
 */
static void
bark_prefix_walk(const BarkPrefixAttr * attrs, int natts,
				 IndexTuple rep, IndexTuple itup, BarkPrefixSplit *split)
{
	Size		rephoff = IndexInfoFindDataOffset(rep->t_info);
	Size		hoff = IndexInfoFindDataOffset(itup->t_info);
	const char *rd = (const char *) rep + rephoff;
	const char *md = (const char *) itup + hoff;
	const uint8 *rbitmap = IndexTupleHasNulls(rep) ?
		(const uint8 *) rep + sizeof(IndexTupleData) : NULL;
	const uint8 *mbitmap = IndexTupleHasNulls(itup) ?
		(const uint8 *) itup + sizeof(IndexTupleData) : NULL;
	Size		replim = bark_prefix_keyend(rep) - rephoff;
	Size		mlim = bark_prefix_keyend(itup) - hoff;
	Size		roff = 0;
	Size		moff = 0;

	/* No layout: share the whole data area, with no carved header. */
	if (attrs == NULL)
	{
		Size		limit = Min(replim, mlim);
		Size		n = 0;

		while (n < limit && rd[n] == md[n])
			n++;
		split->lead_shared = 0;
		split->shared_len = n;
		return;
	}

	for (int i = 0; i < natts; i++)
	{
		Size		rvaloff,
					rvallen,
					mvaloff,
					mvallen;
		Size		common;
		Size		cmplen;
		Size		divoff = Max(roff, moff);

		if (!bark_prefix_column(attrs, natts, i, rd, rbitmap, replim,
								&roff, &rvaloff, &rvallen) ||
			!bark_prefix_column(attrs, natts, i, md, mbitmap, mlim,
								&moff, &mvaloff, &mvallen))
		{
			split->lead_shared = divoff;
			split->shared_len = 0;
			return;
		}

		/*
		 * A column stays in the shared leading span only when its whole
		 * data-area footprint is byte-identical in both tuples: the same
		 * total length (roff == moff) and the same bytes.  That covers the
		 * length header and any alignment padding, and it separates a null
		 * column (no bytes) from an empty value (a bare varlena header),
		 * which share the same empty value but occupy different footprints.
		 * Compare from divoff, where both columns began in lockstep.
		 */
		if (roff == moff &&
			memcmp(rd + divoff, md + divoff, roff - divoff) == 0)
			continue;

		/*
		 * The divergent column: D is its data-area offset, sv its common
		 * value-byte prefix (0 when one side is null, since a null column has
		 * no value to share).
		 */
		cmplen = Min(rvallen, mvallen);
		common = 0;
		while (common < cmplen && rd[rvaloff + common] == md[mvaloff + common])
			common++;
		split->lead_shared = divoff;
		split->shared_len = common;
		return;
	}

	/* Every key column matched: share the whole data area, no divergent col. */
	Assert(roff == moff);
	split->lead_shared = roff;
	split->shared_len = 0;
}

/*
 * Divergent-column prefix (alignment padding plus length header) at data-area
 * offset D: the bytes decode must keep with each member before splicing in the
 * shared value.  Columns [0..D) are byte-identical across the group, so this
 * walks `data' (any group member's data area, bounded by `limit') with null
 * bitmap `bitmap' to the column that begins at D, then measures that column in
 * `divdata' (the bytes of the data area from D, bounded by `divlimit').  For
 * the anchor, `divdata' is `data + D'; for a coded member it is the member's
 * residual, whose [0..D) bytes equal the anchor's.  Returns 0 when D is at or
 * past the data area, the divergent column is null, or every column is shared;
 * sets *ok false (returns 0) on a malformed length that would overrun.
 */
static Size
bark_prefix_divprefix(const BarkPrefixAttr * attrs, int natts,
					  const char *data, const uint8 *bitmap, Size limit,
					  const char *divdata, Size divlimit, Size lead_shared,
					  bool *ok)
{
	Size		off = 0;

	*ok = true;
	if (attrs == NULL || lead_shared >= limit)
		return 0;

	for (int i = 0; i < natts; i++)
	{
		const		BarkPrefixAttr *attr = &attrs[i];
		Size		valoff,
					vallen;
		Size		pad;

		if (off > lead_shared)
			return 0;			/* D fell inside a column, not a boundary */
		if (bitmap != NULL && att_isnull(i, bitmap))
			continue;			/* null column: no bytes, no carved header */
		if (off < lead_shared)
		{
			if (!bark_prefix_column(attrs, natts, i, data, bitmap, limit,
									&off, &valoff, &vallen))
			{
				*ok = false;
				return 0;
			}
			continue;
		}

		/* column i is the first non-null column beginning at D */
		if (attr->attlen != -1 || (attr->flags & BARK_PREFIX_ATTR_BYVAL) != 0)
			return 0;			/* fixed or by-value: value starts at D */

		/* varlena: packed short stays unaligned, else align for a 4-byte hdr */
		if (divlimit > 0 && VARATT_NOT_PAD_BYTE(divdata))
			pad = 0;
		else
			pad = att_nominal_alignby(lead_shared, attr->attalignby) -
				lead_shared;
		if (pad + VARHDRSZ_SHORT > divlimit)
		{
			*ok = false;
			return 0;
		}
		if (VARATT_IS_1B(divdata + pad))
			return pad + VARHDRSZ_SHORT;
		if (pad + VARHDRSZ > divlimit)
		{
			*ok = false;
			return 0;
		}
		return pad + VARHDRSZ;
	}
	return 0;					/* every column shared: no divergent column */
}

/*
 * Divergent-column prefix of whole tuple `itup' at offset D (see
 * bark_prefix_divprefix).  For the anchor, which is a complete tuple; the
 * caller has bounded D against the key size.
 */
static inline Size
bark_prefix_divhdr(const BarkPrefixAttr * attrs, int natts,
				   IndexTuple itup, Size lead_shared)
{
	Size		hoff = IndexInfoFindDataOffset(itup->t_info);
	Size		limit = bark_prefix_keyend(itup) - hoff;
	const char *data = (const char *) itup + hoff;
	const uint8 *bitmap = IndexTupleHasNulls(itup) ?
		(const uint8 *) itup + sizeof(IndexTupleData) : NULL;
	bool		ok;

	return bark_prefix_divprefix(attrs, natts, data, bitmap, limit,
								 data + lead_shared, limit - lead_shared,
								 lead_shared, &ok);
}

/*
 * Total data-area bytes `itup' shares with `rep': the fully-shared leading
 * columns D plus the divergent column's shared value prefix sv.  This is what
 * coding removes from the member, so grouping and the size estimate rank by
 * it.
 */
static inline Size
bark_prefix_shared_bytes(const BarkPrefixSplit *split)
{
	return split->lead_shared + split->shared_len;
}

/*
 * Size of `itup' coded against representative `rep' (what
 * bark_prefix_encode_against would produce), without forming it.  `shared'
 * counts the data-area bytes dropped (leading columns plus the divergent
 * value prefix); the member keeps its own divergent-column header, which is
 * part of the residual, so the residual is the key data area minus `shared'.
 * Its body (LIST/POSTING) follows the residual at its own MAXALIGN boundary.
 */
static Size
bark_prefix_coded_size(IndexTuple itup, Size shared)
{
	Size		hoff = IndexInfoFindDataOffset(itup->t_info);
	Size		keyend = bark_prefix_keyend(itup);
	Size		bodysz = IndexTupleSize(itup) - keyend;
	Size		sz;

	sz = MAXALIGN(sizeof(IndexTupleData) + sizeof(BarkPrefixCode) +
				  (hoff - sizeof(IndexTupleData)) +
				  (keyend - hoff - shared));
	if (bodysz > 0)
		sz = MAXALIGN(sz + bodysz);
	return sz;
}

/*
 * bark_prefix_group_start
 *		Should `itup' start a new group?
 *
 * `rep' is the current group's representative and `prev' the preceding tuple
 * (both NULL for the page's first tuple).  Sets *shared to the data-area bytes
 * `itup' shares with the representative it ends up under (itself if it starts
 * a group).  This is the only grouping rule, shared by the CREATE INDEX
 * estimate (bark_prefix_est_add) and the layout (bark_prefix_choose_groups),
 * so the two cannot disagree.
 *
 * A group starts when `itup' shares nothing with `rep', or shares at least one
 * MAXALIGN quantum more with `prev' than with `rep'.  The rule looks only
 * backwards and the new group is led by `itup', so no earlier tuple is ever
 * regrouped and the estimate stays exact tuple by tuple.
 */
static bool
bark_prefix_group_start(const BarkPrefixAttr * attrs, int natts,
						IndexTuple rep, IndexTuple prev, IndexTuple itup,
						Size *shared)
{
	BarkPrefixSplit split;
	Size		s_rep;

	if (rep == NULL)
	{
		bark_prefix_walk(attrs, natts, itup, itup, &split);
		*shared = bark_prefix_shared_bytes(&split);
		return true;
	}

	bark_prefix_walk(attrs, natts, rep, itup, &split);
	s_rep = bark_prefix_shared_bytes(&split);
	if (s_rep == 0)
	{
		bark_prefix_walk(attrs, natts, itup, itup, &split);
		*shared = bark_prefix_shared_bytes(&split);
		return true;
	}

	if (prev != NULL && prev != rep)
	{
		BarkPrefixSplit psplit;
		Size		s_prev;

		bark_prefix_walk(attrs, natts, prev, itup, &psplit);
		s_prev = bark_prefix_shared_bytes(&psplit);
		if (s_prev >= s_rep + MAXIMUM_ALIGNOF)
		{
			bark_prefix_walk(attrs, natts, itup, itup, &split);
			*shared = bark_prefix_shared_bytes(&split);
			return true;
		}
	}

	*shared = s_rep;
	return false;
}

/*
 * Running size estimate of a BARK_PREFIX page laid out from a sequence of
 * plain tuples in key order, as bark_prefix_form_page would lay it out (same
 * grouping, same coded sizes).  Used by CREATE INDEX to fill coded pages.  The
 * tuples passed to bark_prefix_est_add must stay valid while the estimate is
 * in use.
 */
void
bark_prefix_est_add(BarkPrefixPageEst *est, const BarkPrefixAttr * attrs,
					int natts, IndexTuple itup)
{
	Size		plain = MAXALIGN(IndexTupleSize(itup)) + sizeof(ItemIdData);
	Size		shared;

	est->natts = natts;
	if (bark_prefix_group_start(attrs, natts, est->grouprep, est->prev, itup,
								&shared))
	{
		est->grouprep = itup;
		est->ngroups++;
		est->anchorsz += MAXALIGN(MAXALIGN(sizeof(BarkPrefixAnchorDisk)) +
								  bark_prefix_keyend(itup));
	}
	est->lastcoded = bark_prefix_coded_size(itup, shared) + sizeof(ItemIdData);
	est->maxcoded = Max(est->maxcoded, est->lastcoded - sizeof(ItemIdData));
	est->codedsz += est->lastcoded;
	est->plainsz += plain;
	est->maxplain = Max(est->maxplain, plain);
	est->prev = itup;
}

/* Bytes (items plus line pointers) the coded page would use */
Size
bark_prefix_est_coded(const BarkPrefixPageEst *est)
{
	return bark_prefix_diritem_size(est->natts, est->ngroups, est->anchorsz) +
		sizeof(ItemIdData) + est->codedsz;
}

/* The directory and each member must fit the IndexTuple size field. */
bool
bark_prefix_est_representable(const BarkPrefixPageEst *est)
{
	return bark_prefix_diritem_size(est->natts, est->ngroups, est->anchorsz) <=
		INDEX_SIZE_MASK &&
		est->maxcoded <= INDEX_SIZE_MASK;
}

/*
 * Is a coded page worth having, given its coded and plain sizes?  Demand a
 * real saving: decoding costs CPU on every access, so a page that would save
 * only a few bytes stays plain.
 */
bool
bark_prefix_worthwhile(Size codedsz, Size plainsz)
{
	return codedsz * 16 <= plainsz * 15;
}

/*
 * bark_prefix_build_layout
 *		Fill `attrs' (INDEX_MAX_KEYS entries) with the index's key-column
 *		layout and return the key count.
 *
 * The primary calls this from RelationGetDescr(index) at page-write time; the
 * layout it returns is stored in the directory so decode can parse tuples
 * page-only.
 */
int
bark_prefix_build_layout(Relation index, BarkPrefixAttr * attrs)
{
	TupleDesc	tupdesc = RelationGetDescr(index);
	int			natts = IndexRelationGetNumberOfKeyAttributes(index);

	Assert(natts >= 1 && natts <= INDEX_MAX_KEYS);
	for (int i = 0; i < natts; i++)
	{
		CompactAttribute *attr = TupleDescCompactAttr(tupdesc, i);

		attrs[i].attlen = attr->attlen;
		attrs[i].attalignby = attr->attalignby;
		attrs[i].flags = attr->attbyval ? BARK_PREFIX_ATTR_BYVAL : 0;
	}
	return natts;
}

/*
 * bark_prefix_choose_groups
 *		Group plain tuples items[0..n-1], in key order, by
 *		bark_prefix_group_start.
 *
 * Returns a palloc'd BarkPrefixDir, or NULL when no two tuples share a group.
 */
static BarkPrefixDir *
bark_prefix_choose_groups(const BarkPrefixAttr * attrs, int natts,
						  IndexTuple *items, int n)
{
	BarkPrefixDir *dir;
	int			ngroups = 0;

	StaticAssertStmt(MaxIndexTuplesPerPage <= BARK_PREFIX_MAXGROUPS,
					 "group number must fit in uint16");

	if (n <= 1)
		return NULL;

	dir = palloc(offsetof(BarkPrefixDir, anchors) + sizeof(BarkPrefixAnchor) * n);
	for (int i = 0; i < n; i++)
	{
		IndexTuple	rep = ngroups == 0 ? NULL : dir->anchors[ngroups - 1].rep;
		IndexTuple	prev = i == 0 ? NULL : items[i - 1];
		Size		shared;

		if (bark_prefix_group_start(attrs, natts, rep, prev, items[i], &shared))
			dir->anchors[ngroups++].rep = items[i];
	}

	if (ngroups >= n)
	{
		pfree(dir);
		return NULL;			/* every tuple its own group: nothing shared */
	}

	dir->ngroups = ngroups;
	return dir;
}

/*
 * Serialize `dir' as a palloc'd directory item, setting *itemsz to its size.
 * `attrs'/`natts' is the key-column layout stored in the header for page-only
 * decode.  The cached decoded sizes are left zero for bark_prefix_recount.
 */
static IndexTuple
bark_prefix_form_diritem(BarkPrefixDir *dir, const BarkPrefixAttr * attrs,
						 int natts, Size *itemsz)
{
	int			ng = dir->ngroups;
	Size		total = bark_prefix_diritem_size(natts, ng, 0);
	Size	   *anchoroff = palloc(sizeof(Size) * ng);
	char	   *buf;
	IndexTuple	item;
	BarkPrefixDirHdr *hdr;
	uint16	   *offarr;

	for (int g = 0; g < ng; g++)
	{
		Size		repsz = bark_prefix_keyend(dir->anchors[g].rep);

		anchoroff[g] = total;
		total += MAXALIGN(MAXALIGN(sizeof(BarkPrefixAnchorDisk)) + repsz);
	}

	if (total > INDEX_SIZE_MASK)
		elog(ERROR, "BARK prefix directory too large (%zu bytes)", total);

	buf = palloc0(total);
	item = (IndexTuple) buf;
	ItemPointerSetInvalid(&item->t_tid);
	item->t_info = (unsigned short) total;	/* size only; no null/varwidth */

	hdr = (BarkPrefixDirHdr *) buf;
	hdr->ngroups = (uint16) ng;
	hdr->decodedsz = 0;
	hdr->maxdecoded = 0;
	hdr->natts = (uint16) natts;
	for (int i = 0; i < natts; i++)
		hdr->attrs[i] = attrs[i];
	offarr = bark_prefix_anchoroff(hdr);
	for (int g = 0; g < ng; g++)
		offarr[g] = (uint16) anchoroff[g];

	for (int g = 0; g < ng; g++)
	{
		BarkPrefixAnchorDisk *ad = (BarkPrefixAnchorDisk *) (buf + anchoroff[g]);
		Size		repsz = bark_prefix_keyend(dir->anchors[g].rep);
		IndexTuple	rep;

		ad->repsz = (uint16) repsz;
		/* store the key part only, as a plain non-alt-TID tuple */
		rep = (IndexTuple) ((char *) ad + MAXALIGN(sizeof(BarkPrefixAnchorDisk)));
		memcpy(rep, dir->anchors[g].rep, repsz);
		rep->t_info = (rep->t_info & ~(INDEX_SIZE_MASK | INDEX_AM_RESERVED_BIT)) |
			repsz;
	}

	pfree(anchoroff);
	*itemsz = total;
	return item;
}

/* The directory item of a BARK_PREFIX page, unchecked. */
static inline char *
bark_prefix_diritem(Page page)
{
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	OffsetNumber off = BarkPagePrefixOff(opaque);

	Assert(BarkPageHasPrefix(opaque));
	return (char *) PageGetItem(page, PageGetItemId(page, off));
}

/*
 * Key-column layout stored in a coded page's directory, for re-coding against
 * that page page-only (insert, recode).  The directory holds exactly the
 * layout the page was coded with, so a member re-coded with it decodes the
 * same way.
 */
static inline const BarkPrefixAttr *
bark_prefix_page_layout(char *diritem, int *natts)
{
	BarkPrefixDirHdr *hdr = (BarkPrefixDirHdr *) diritem;

	*natts = hdr->natts;
	return hdr->attrs;
}

/* Pointer to anchor g's on-disk blob within the directory item. */
static inline BarkPrefixAnchorDisk *
bark_prefix_anchor(char *diritem, int g)
{
	BarkPrefixDirHdr *hdr = (BarkPrefixDirHdr *) diritem;

	return (BarkPrefixAnchorDisk *) (diritem + bark_prefix_anchoroff(hdr)[g]);
}

/* The representative IndexTuple inside an on-disk anchor blob. */
static inline IndexTuple
bark_prefix_anchor_rep(BarkPrefixAnchorDisk *ad)
{
	return (IndexTuple) ((char *) ad + MAXALIGN(sizeof(BarkPrefixAnchorDisk)));
}

/* Report a malformed physical representation without interpreting its data. */
pg_noreturn static void
bark_prefix_corrupt(OffsetNumber offnum, const char *field)
{
	ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			 errmsg_internal("invalid BARK prefix %s at offset %u", field, offnum)));
}

/* Establish page bounds before using either line pointers or the opaque area. */
static void
bark_prefix_check_header(Page page)
{
	PageHeader	phdr = (PageHeader) page;
	BarkPageOpaque opaque;

	if (phdr->pd_special != BLCKSZ - MAXALIGN(sizeof(BarkPageOpaqueData)) ||
		phdr->pd_lower < SizeOfPageHeaderData ||
		phdr->pd_lower > phdr->pd_upper ||
		phdr->pd_upper > phdr->pd_special ||
		(phdr->pd_lower - SizeOfPageHeaderData) % sizeof(ItemIdData) != 0)
		bark_prefix_corrupt(InvalidOffsetNumber, "page header");

	opaque = BarkPageGetOpaque(page);
	if (!BarkPageHasPrefix(opaque) || !BarkPageIsLeaf(opaque) ||
		opaque->bark_level != 0 ||
		PageGetMaxOffsetNumber(page) < BarkPageFirstDataKey(opaque))
		bark_prefix_corrupt(InvalidOffsetNumber, "page layout");
}

/* The caller has checked the page header, but not the line pointer. */
static IndexTuple
bark_prefix_checked_item(Page page, OffsetNumber offnum, Size minsize)
{
	ItemId		itemid;
	Size		offset;
	Size		len;
	IndexTuple	itup;

	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
		bark_prefix_corrupt(offnum, "item offset");
	itemid = PageGetItemId(page, offnum);
	offset = ItemIdGetOffset(itemid);
	len = ItemIdGetLength(itemid);
	if ((!ItemIdIsNormal(itemid) && !ItemIdIsDead(itemid)) ||
		offset != MAXALIGN(offset) ||
		offset < ((PageHeader) page)->pd_upper ||
		offset > ((PageHeader) page)->pd_special ||
		len > ((PageHeader) page)->pd_special - offset || len < minsize)
		bark_prefix_corrupt(offnum, "line pointer");
	itup = (IndexTuple) PageGetItem(page, itemid);
	if (IndexTupleSize(itup) != len || len != MAXALIGN(len))
		bark_prefix_corrupt(offnum, "tuple size");
	return itup;
}

/* Check the directory header and offset array, without visiting any anchor. */
static BarkPrefixDirHdr *
bark_prefix_checked_directory(Page page)
{
	OffsetNumber offnum = BarkPagePrefixOff(BarkPageGetOpaque(page));
	BarkPrefixDirHdr *hdr = (BarkPrefixDirHdr *)
		bark_prefix_checked_item(page, offnum, offsetof(BarkPrefixDirHdr, attrs));
	Size		len = IndexTupleSize(&hdr->header);

	if (hdr->header.t_info != len ||
		ItemPointerGetBlockNumberNoCheck(&hdr->header.t_tid) != InvalidBlockNumber ||
		ItemPointerGetOffsetNumberNoCheck(&hdr->header.t_tid) != InvalidOffsetNumber ||
		ItemIdIsDead(PageGetItemId(page, offnum)) ||
		hdr->ngroups == 0 || hdr->ngroups > BARK_PREFIX_MAXGROUPS ||
		hdr->natts > INDEX_MAX_KEYS ||
		bark_prefix_diritem_size(hdr->natts, hdr->ngroups, 0) > len)
		bark_prefix_corrupt(offnum, "directory header");
	return hdr;
}

/* Validate just the selected anchor; never follow an unchecked byte offset. */
static IndexTuple
bark_prefix_checked_anchor(BarkPrefixDirHdr *hdr, int group, OffsetNumber offnum)
{
	Size		len = IndexTupleSize(&hdr->header);
	Size		offset;
	BarkPrefixAnchorDisk *ad;
	IndexTuple	rep;

	if (group >= hdr->ngroups)
		bark_prefix_corrupt(offnum, "group number");
	offset = bark_prefix_anchoroff(hdr)[group];
	if (offset != MAXALIGN(offset) ||
		offset < bark_prefix_diritem_size(hdr->natts, hdr->ngroups, 0) ||
		offset > len ||
		MAXALIGN(sizeof(BarkPrefixAnchorDisk)) + sizeof(IndexTupleData) > len - offset)
		bark_prefix_corrupt(offnum, "anchor offset");
	ad = (BarkPrefixAnchorDisk *) ((char *) hdr + offset);
	if (ad->repsz < sizeof(IndexTupleData) ||
		ad->repsz > len - offset - MAXALIGN(sizeof(BarkPrefixAnchorDisk)))
		bark_prefix_corrupt(offnum, "anchor size");
	rep = bark_prefix_anchor_rep(ad);
	if (IndexTupleSize(rep) != ad->repsz ||
		ad->repsz != MAXALIGN(ad->repsz) ||
		(rep->t_info & INDEX_AM_RESERVED_BIT) != 0 ||
		ad->repsz < IndexInfoFindDataOffset(rep->t_info))
		bark_prefix_corrupt(offnum, "anchor tuple header");
	return rep;
}

/* Check every source and destination extent used by the decoder. */
static Size
bark_prefix_check_member(BarkPrefixDirHdr *hdr, IndexTuple coded,
						 OffsetNumber offnum, IndexTuple *repp,
						 BarkPrefixSplice *splice)
{
	BarkPrefixCode *code = (BarkPrefixCode *) ((char *) coded + sizeof(IndexTupleData));
	IndexTuple	rep = bark_prefix_checked_anchor(hdr, code->group, offnum);
	Size		keysz = code->orig_info & INDEX_SIZE_MASK;
	Size		hoff = IndexInfoFindDataOffset(code->orig_info);
	Size		rephoff = IndexInfoFindDataOffset(rep->t_info);
	Size		repdatalen = IndexTupleSize(rep) - rephoff;
	Size		lead_shared = code->lead_shared;
	Size		shared_len = code->shared_len;
	Size		bodysz;
	Size		codedbody;
	Size		plainsz;
	Size		codesz = IndexTupleSize(coded);
	Size		mhdr;
	Size		ahdr;
	bool		ok_m,
				ok_a;
	const char *codebody = (const char *) code + sizeof(BarkPrefixCode);
	Size		bitmaplen = hoff - sizeof(IndexTupleData);
	const uint8 *mbitmap = (code->orig_info & INDEX_NULL_MASK) != 0 ?
		(const uint8 *) codebody : NULL;
	const char *repdata = (const char *) rep + rephoff;
	const char *residp;
	Size		residual;

	/*
	 * The code header stores the original (plain) entry's t_info flags with
	 * its key-part end in the t_info size field and its total plain size in
	 * orig_total.  The coded entry's own alt-TID bit (and body offset) match
	 * the original's shape; the body bytes equal orig_total - key end.
	 */
	if (keysz < hoff || keysz > code->orig_total ||
		code->orig_total > INDEX_SIZE_MASK ||
		BarkEntryGetShape(coded) == BARK_SHAPE_OVERSIZED ||
		BarkEntryGetShape(coded) == BARK_SHAPE_PIVOT)
		bark_prefix_corrupt(offnum, "member header");
	codedbody = code->orig_total - keysz;

	/*
	 * The shared span is the fully-shared leading columns (lead_shared, from
	 * the anchor's [0..D)) plus the divergent column's shared value bytes
	 * (shared_len).  Both must lie within the member's key data area and the
	 * anchor must carry the shared leading bytes.
	 */
	if (lead_shared > keysz - hoff ||
		shared_len > keysz - hoff - lead_shared ||
		lead_shared > repdatalen)
		bark_prefix_corrupt(offnum, "shared length");
	residual = keysz - hoff - lead_shared - shared_len;
	bodysz = MAXALIGN(sizeof(IndexTupleData) + sizeof(BarkPrefixCode) +
					  bitmaplen + residual);
	if (bodysz > codesz || MAXALIGN(bodysz + codedbody) != codesz)
		bark_prefix_corrupt(offnum, "residual extent");
	residp = codebody + bitmaplen;

	/*
	 * The divergent column's header stays private: the member's own copy
	 * leads its residual, the anchor's is skipped so the shared value bytes
	 * splice after it.  Both prefixes (padding plus length header) are
	 * measured from the layout: the member's from its residual, the anchor's
	 * from its stored tuple, each walking the anchor's byte-identical [0..D)
	 * to reach the divergent column.  Bound every splice extent against the
	 * member residual and the anchor's data area before decode copies.
	 */
	mhdr = bark_prefix_divprefix(hdr->attrs, hdr->natts, repdata, mbitmap,
								 repdatalen, residp, residual, lead_shared,
								 &ok_m);
	ahdr = bark_prefix_divprefix(hdr->attrs, hdr->natts, repdata,
								 IndexTupleHasNulls(rep) ?
								 (const uint8 *) rep + sizeof(IndexTupleData) :
								 NULL, repdatalen, repdata + lead_shared,
								 repdatalen - lead_shared, lead_shared, &ok_a);
	if (!ok_m || !ok_a || mhdr > residual ||
		lead_shared + ahdr + shared_len > repdatalen)
		bark_prefix_corrupt(offnum, "shared length");

	plainsz = code->orig_total;
	if (plainsz > INDEX_SIZE_MASK || plainsz > BLCKSZ)
		bark_prefix_corrupt(offnum, "decoded tuple size");
	if (repp != NULL)
		*repp = rep;
	if (splice != NULL)
	{
		splice->lead_shared = lead_shared;
		splice->shared_len = shared_len;
		splice->mhdr = mhdr;
		splice->ahdr = ahdr;
		splice->residual = residual;
	}
	return plainsz;
}

/*
 * bark_prefix_check
 *		Validate a whole BARK_PREFIX page; ERROR if it is malformed.
 *
 * For amcheck and pageinspect, before they read the page's tuples.  Unlike
 * decode, this also visits unused anchors and checks the cached decoded sizes
 * against the split-fit invariant (see bark_prefix_split_needed).
 */
void
bark_prefix_check(Page page)
{
	BarkPageOpaque opaque;
	BarkPrefixDirHdr *hdr;
	OffsetNumber diroff;
	Size		end;
	Size		total = 0;
	Size		maxitem = 0;
	Size		hikeysz = 0;
	Size		usable = BLCKSZ - SizeOfPageHeaderData - MAXALIGN(sizeof(BarkPageOpaqueData));

	bark_prefix_check_header(page);
	opaque = BarkPageGetOpaque(page);
	diroff = BarkPagePrefixOff(opaque);
	hdr = bark_prefix_checked_directory(page);
	end = bark_prefix_diritem_size(hdr->natts, hdr->ngroups, 0);
	for (int g = 0; g < hdr->ngroups; g++)
	{
		IndexTuple	rep = bark_prefix_checked_anchor(hdr, g, diroff);

		if (bark_prefix_anchoroff(hdr)[g] != end)
			bark_prefix_corrupt(diroff, "anchor layout");
		end += MAXALIGN(sizeof(BarkPrefixAnchorDisk)) + IndexTupleSize(rep);
	}
	if (end != IndexTupleSize(&hdr->header))
		bark_prefix_corrupt(diroff, "directory size");
	for (OffsetNumber off = BarkPageFirstDataKey(opaque);
		 off <= PageGetMaxOffsetNumber(page); off++)
	{
		IndexTuple	coded = bark_prefix_checked_item(page, off,
													 sizeof(IndexTupleData) + sizeof(BarkPrefixCode));
		Size		sz = MAXALIGN(bark_prefix_check_member(hdr, coded, off, NULL, NULL)) +
			sizeof(ItemIdData);

		total += sz;
		maxitem = Max(maxitem, sz);
	}
	if (total != hdr->decodedsz || maxitem != hdr->maxdecoded)
		bark_prefix_corrupt(diroff, "directory decoded size");
	if (!BarkPageRightmost(opaque))
	{
		IndexTuple	hikey = bark_prefix_checked_item(page, BARK_P_HIKEY, sizeof(IndexTupleData));

		hikeysz = MAXALIGN(IndexTupleSize(hikey)) + sizeof(ItemIdData);
	}
	if (total + hikeysz + 2 * maxitem + MAXALIGN(sizeof(ItemPointerData)) > 2 * usable)
		bark_prefix_corrupt(diroff, "decoded split budget");
}

/*
 * Code plain tuple `itup' against group `group', whose representative is
 * `rep'.  `attrs'/`natts' is the key-column layout used to decide where the
 * two diverge (see bark_prefix_walk).  Returns a palloc'd coded tuple, or NULL
 * if its size does not fit INDEX_SIZE_MASK.
 *
 * The member drops the fully-shared leading columns (D bytes) and the
 * divergent column's shared value prefix (sv bytes), keeping its own
 * divergent-column header.  Its stored residual is the key data area from D
 * with the shared value bytes cut out of the middle: [divergent header][value
 * bytes after the shared ones][later columns].  A LIST/POSTING body follows
 * the residual at its own MAXALIGN boundary, as on a plain entry, with the
 * body offset in t_tid set to the coded position.
 */
static IndexTuple
bark_prefix_encode_against(const BarkPrefixAttr * attrs, int natts,
						   IndexTuple rep, IndexTuple itup, int group)
{
	Size		hoff = IndexInfoFindDataOffset(itup->t_info);
	Size		keyend = bark_prefix_keyend(itup);
	Size		bodysz = IndexTupleSize(itup) - keyend;
	BarkPrefixSplit split;
	Size		lead_shared;	/* D: fully-shared leading columns */
	Size		shared_len;		/* sv: divergent column's shared value bytes */
	Size		mhdr;			/* divergent column's prefix in itup */
	Size		residual;
	Size		bodysoff;
	Size		codesz;
	char	   *buf;
	char	   *residp;
	IndexTuple	coded;
	BarkPrefixCode *code;

	bark_prefix_walk(attrs, natts, rep, itup, &split);
	lead_shared = split.lead_shared;
	shared_len = split.shared_len;
	mhdr = bark_prefix_divhdr(attrs, natts, itup, lead_shared);

	residual = keyend - hoff - lead_shared - shared_len;
	bodysoff = MAXALIGN(sizeof(IndexTupleData) + sizeof(BarkPrefixCode) +
						(hoff - sizeof(IndexTupleData)) + residual);
	codesz = bodysoff;
	if (bodysz > 0)
		codesz = MAXALIGN(bodysoff + bodysz);
	if (codesz > INDEX_SIZE_MASK)
		return NULL;

	buf = palloc0(codesz);
	coded = (IndexTuple) buf;
	coded->t_tid = itup->t_tid;
	/*
	 * The coded tuple keeps the original's alt-TID bit (so its shape and body
	 * offset read back from t_tid) but not its null or varwidth bits: its own
	 * bytes after the header are the code header, not a parseable data area.
	 * The original's flags, null bitmap and sizes live in the code header for
	 * decode.
	 */
	coded->t_info = (itup->t_info & INDEX_AM_RESERVED_BIT) | (unsigned short) codesz;

	code = (BarkPrefixCode *) (buf + sizeof(IndexTupleData));
	code->group = (uint16) group;
	code->lead_shared = (uint16) lead_shared;
	code->shared_len = (uint16) shared_len;
	code->orig_total = (uint16) IndexTupleSize(itup);
	/* flags and key-part end of the original; coded's own size is codesz */
	code->orig_info = (itup->t_info & ~INDEX_SIZE_MASK) | (unsigned short) keyend;

	/* copy the original's null bitmap (bytes between header and data area) */
	memcpy(buf + sizeof(IndexTupleData) + sizeof(BarkPrefixCode),
		   (char *) itup + sizeof(IndexTupleData),
		   hoff - sizeof(IndexTupleData));

	/*
	 * Copy the residual.  The fully-shared leading D bytes come from the
	 * anchor, so they are not stored.  The member's own divergent header
	 * (mhdr bytes at D) stays, the next shared_len value bytes are dropped,
	 * and the rest of the key data area follows.
	 */
	residp = buf + sizeof(IndexTupleData) + sizeof(BarkPrefixCode) +
		(hoff - sizeof(IndexTupleData));
	memcpy(residp, (char *) itup + hoff + lead_shared, mhdr);
	memcpy(residp + mhdr,
		   (char *) itup + hoff + lead_shared + mhdr + shared_len,
		   residual - mhdr);

	/*
	 * A LIST/POSTING body stays as it is, in the standard layout, at the
	 * coded entry's own body offset.  Code that reads only the body (VACUUM,
	 * posting-list updates) works on the coded tuple once the body offset is
	 * fixed.
	 */
	if (bodysz > 0)
	{
		memcpy(buf + bodysoff, (char *) itup + keyend, bodysz);
		Assert(BarkEntryIsAltTID(coded));
		BarkEntrySetBodyOffset(coded, (uint16) bodysoff);
	}

	return coded;
}

/*
 * bark_prefix_decode
 *		Decode the item `itup' of the BARK_PREFIX page `page' into `buf', and
 *		return it; an item stored plain is returned as it is.
 *
 * Returns a pointer into `buf'.  Every extent the copy reads or writes is
 * validated first, so a corrupt page raises ERROR rather than reading past the
 * page or overrunning `buf'.  The inverse of bark_prefix_encode, byte for byte.
 */
IndexTuple
bark_prefix_decode(Page page, IndexTuple itup, BarkItemBuf *buf)
{
	BarkPrefixDirHdr *hdr;
	BarkPrefixCode *code;
	IndexTuple	rep;
	BarkPrefixSplice splice;
	Size		rephoff;
	Size		hoff;
	Size		bitmaplen;
	Size		keyend;
	Size		bodysz;
	Size		bodysoff;
	char	   *codebody;
	char	   *residp;
	char	   *out = buf->data;
	IndexTuple	plain = (IndexTuple) out;

	/*
	 * itup is a data item of the page, resolved by BarkPageGetItem from a
	 * valid line pointer, so its own IndexTupleSize bounds it.  Validate the
	 * directory and this member before any copy; InvalidOffsetNumber is used
	 * only in a corruption message, since the item is already in hand.
	 */
	bark_prefix_check_header(page);
	hdr = bark_prefix_checked_directory(page);
	if (IndexTupleSize(itup) < sizeof(IndexTupleData) + sizeof(BarkPrefixCode) ||
		IndexTupleSize(itup) != MAXALIGN(IndexTupleSize(itup)))
		bark_prefix_corrupt(InvalidOffsetNumber, "member size");
	bark_prefix_check_member(hdr, itup, InvalidOffsetNumber, &rep, &splice);
	code = (BarkPrefixCode *) ((char *) itup + sizeof(IndexTupleData));
	rephoff = IndexInfoFindDataOffset(rep->t_info);
	hoff = IndexInfoFindDataOffset(code->orig_info);
	bitmaplen = hoff - sizeof(IndexTupleData);
	codebody = (char *) code + sizeof(BarkPrefixCode);
	residp = codebody + bitmaplen;
	keyend = code->orig_info & INDEX_SIZE_MASK;
	bodysz = code->orig_total - keyend;
	bodysoff = ((char *) code - (char *) itup) + MAXALIGN(sizeof(BarkPrefixCode) +
														  bitmaplen + splice.residual);

	if (code->orig_total > sizeof(BarkItemBuf))
		bark_prefix_corrupt(InvalidOffsetNumber, "decoded tuple size");

	/*
	 * Reconstruct the key data area from the offsets bark_prefix_check_member
	 * validated.  [0..D) is the fully-shared leading columns, spliced whole
	 * from the anchor.  The divergent column's header stays private: the
	 * member's own prefix (mhdr) leads its residual, the anchor's (ahdr) is
	 * skipped so the shared value bytes come after it, then the member's
	 * remaining residual.
	 */
	plain->t_tid = itup->t_tid;
	plain->t_info = code->orig_info;
	memcpy(out + sizeof(IndexTupleData), codebody, bitmaplen);
	/* fully-shared leading columns from the anchor */
	memcpy(out + hoff, (char *) rep + rephoff, splice.lead_shared);
	/* member's own divergent-column prefix (alignment and length header) */
	memcpy(out + hoff + splice.lead_shared, residp, splice.mhdr);
	/* shared value bytes from the anchor, after its divergent-column prefix */
	memcpy(out + hoff + splice.lead_shared + splice.mhdr,
		   (char *) rep + rephoff + splice.lead_shared + splice.ahdr,
		   splice.shared_len);
	/* the member's remaining residual value bytes */
	memcpy(out + hoff + splice.lead_shared + splice.mhdr + splice.shared_len,
		   residp + splice.mhdr, splice.residual - splice.mhdr);
	/* zero any padding between the key end and the body */
	if (bodysz > 0)
		memset(out + keyend, 0, MAXALIGN(keyend) - keyend);

	/* the body (LIST locator array, POSTING sbm blob) at its own boundary */
	if (bodysz > 0)
	{
		memcpy(out + keyend, (char *) itup + bodysoff, bodysz);
		plain->t_info = (code->orig_info & ~INDEX_SIZE_MASK) |
			(unsigned short) code->orig_total;
		BarkEntrySetBodyOffset(plain, (uint16) keyend);
	}

	return plain;
}

/*
 * bark_coded_size
 *		The MAXALIGNed size `itup' takes on `page', coded if the page codes it.
 */
Size
bark_coded_size(Page page, IndexTuple itup)
{
	char	   *diritem;
	int			natts;
	const		BarkPrefixAttr *attrs;
	OffsetNumber maxoff;
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	IndexTuple	rep;
	BarkPrefixSplit split;

	if (!BarkPageHasPrefix(opaque) || !bark_prefix_codable(itup))
		return MAXALIGN(IndexTupleSize(itup));

	diritem = bark_prefix_diritem(page);
	attrs = bark_prefix_page_layout(diritem, &natts);
	maxoff = PageGetMaxOffsetNumber(page);

	/* Size against the last group's anchor; an insert joins a neighbor. */
	if (maxoff >= BarkPageFirstDataKey(opaque))
	{
		IndexTuple	ncoded = (IndexTuple) PageGetItem(page, PageGetItemId(page, maxoff));
		BarkPrefixCode *ncode = (BarkPrefixCode *) ((char *) ncoded +
													sizeof(IndexTupleData));

		rep = bark_prefix_anchor_rep(bark_prefix_anchor(diritem, ncode->group));
	}
	else
		rep = bark_prefix_anchor_rep(bark_prefix_anchor(diritem, 0));

	bark_prefix_walk(attrs, natts, rep, itup, &split);
	return bark_prefix_coded_size(itup, bark_prefix_shared_bytes(&split));
}

/*
 * bark_prefix_encode
 *		The coded form of the leaf entry `itup' for `page', palloc'd; `itup'
 *		itself when the page has no prefix or the entry is stored plain.
 *
 * The tuple joins the group of the item at the last data offset (where an
 * append lands); callers that insert at a specific offset use
 * bark_prefix_encode_insert.  This keeps the hot in-place writers (add TID,
 * swap, overwrite, build-append) simple.
 */
IndexTuple
bark_prefix_encode(Page page, IndexTuple itup)
{
	char	   *diritem;
	int			natts;
	const		BarkPrefixAttr *attrs;
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	OffsetNumber maxoff;
	IndexTuple	rep;
	int			group;
	IndexTuple	coded;

	if (!BarkPageHasPrefix(opaque) || !bark_prefix_codable(itup))
		return itup;

	diritem = bark_prefix_diritem(page);
	attrs = bark_prefix_page_layout(diritem, &natts);
	maxoff = PageGetMaxOffsetNumber(page);

	if (maxoff >= BarkPageFirstDataKey(opaque))
	{
		IndexTuple	ncoded = (IndexTuple) PageGetItem(page, PageGetItemId(page, maxoff));
		BarkPrefixCode *ncode = (BarkPrefixCode *) ((char *) ncoded +
													sizeof(IndexTupleData));

		group = ncode->group;
	}
	else
		group = 0;
	rep = bark_prefix_anchor_rep(bark_prefix_anchor(diritem, group));

	coded = bark_prefix_encode_against(attrs, natts, rep, itup, group);
	if (coded == NULL)
		elog(ERROR, "coded BARK leaf entry too large");

#ifdef USE_ASSERT_CHECKING
	{
		BarkItemBuf check;
		IndexTuple	decoded;

		/*
		 * Decode must reproduce the plain entry byte for byte.  The coded
		 * tuple is not yet on the page, so bark_prefix_decode_mem decodes it
		 * off-page against its group's anchor in the page directory.
		 */
		decoded = bark_prefix_decode_mem(page, coded, group, &check);
		Assert(IndexTupleSize(decoded) == IndexTupleSize(itup) &&
			   memcmp(decoded, itup, IndexTupleSize(itup)) == 0);
	}
#endif

	return coded;
}

/*
 * bark_prefix_codable
 *		Is `itup' stored coded on a BARK_PREFIX page?  Only a leaf data entry
 *		of a codable shape; an OVERSIZED entry keeps its key out of line and is
 *		stored as it is, as pivots are.
 */
bool
bark_prefix_codable(IndexTuple itup)
{
	BarkEntryShape shape = BarkEntryGetShape(itup);

	return shape == BARK_SHAPE_SINGLE || shape == BARK_SHAPE_LIST ||
		shape == BARK_SHAPE_POSTING;
}

/*
 * Decode a coded tuple `coded' that is not on `page', against group `group' of
 * `page's directory, into `buf'.  Only the cassert round-trip check in
 * bark_prefix_encode and bark_prefix_recode use this; the hot path decodes
 * on-page items through bark_prefix_decode.
 */
IndexTuple
bark_prefix_decode_mem(Page page, IndexTuple coded, int group, BarkItemBuf *buf)
{
	char	   *diritem = bark_prefix_diritem(page);
	BarkPrefixDirHdr *hdr = (BarkPrefixDirHdr *) diritem;
	BarkPrefixCode *code = (BarkPrefixCode *) ((char *) coded + sizeof(IndexTupleData));
	IndexTuple	rep;
	BarkPrefixSplice splice;
	Size		rephoff;
	Size		hoff;
	Size		bitmaplen;
	Size		keyend;
	Size		bodysz;
	Size		bodysoff;
	char	   *codebody;
	char	   *residp;
	char	   *out = buf->data;
	IndexTuple	plain = (IndexTuple) out;

	bark_prefix_check_member(hdr, coded, BarkPagePrefixOff(BarkPageGetOpaque(page)),
							 &rep, &splice);
	Assert(code->group == group);
	rephoff = IndexInfoFindDataOffset(rep->t_info);
	hoff = IndexInfoFindDataOffset(code->orig_info);
	bitmaplen = hoff - sizeof(IndexTupleData);
	codebody = (char *) code + sizeof(BarkPrefixCode);
	residp = codebody + bitmaplen;
	keyend = code->orig_info & INDEX_SIZE_MASK;
	bodysz = code->orig_total - keyend;
	bodysoff = sizeof(IndexTupleData) + MAXALIGN(sizeof(BarkPrefixCode) +
												 bitmaplen + splice.residual);

	plain->t_tid = coded->t_tid;
	plain->t_info = code->orig_info;
	memcpy(out + sizeof(IndexTupleData), codebody, bitmaplen);
	memcpy(out + hoff, (char *) rep + rephoff, splice.lead_shared);
	memcpy(out + hoff + splice.lead_shared, residp, splice.mhdr);
	memcpy(out + hoff + splice.lead_shared + splice.mhdr,
		   (char *) rep + rephoff + splice.lead_shared + splice.ahdr,
		   splice.shared_len);
	memcpy(out + hoff + splice.lead_shared + splice.mhdr + splice.shared_len,
		   residp + splice.mhdr, splice.residual - splice.mhdr);
	if (bodysz > 0)
	{
		memset(out + keyend, 0, MAXALIGN(keyend) - keyend);
		memcpy(out + keyend, (char *) coded + bodysoff, bodysz);
		plain->t_info = (code->orig_info & ~INDEX_SIZE_MASK) |
			(unsigned short) code->orig_total;
		BarkEntrySetBodyOffset(plain, (uint16) keyend);
	}
	return plain;
}

/*
 * bark_prefix_form_page
 *		Lay out a BARK_PREFIX leaf from `hikey' and items[0..n-1].
 *
 * `newpage' must be PageInit'd with its opaque area set; `hikey' is NULL on a
 * rightmost page, and the items are plain and in key order.  Returns false,
 * leaving `newpage' unspecified, if coding does not pay off (unless `force'),
 * the directory or a coded member exceeds INDEX_SIZE_MASK, or the coded items
 * do not fit one page.  The caller sets LSN and prune XID.
 */
bool
bark_prefix_form_page(Page newpage, IndexTuple hikey, IndexTuple *items, int n,
					  const BarkPrefixAttr * attrs, int natts, bool force)
{
	BarkPrefixPageEst est;
	BarkPrefixDir *dir;
	IndexTuple	diritem;
	Size		diritemsz;
	OffsetNumber newoff;
	bool		ok = true;

	if (n <= 0)
		return false;

	/* Is it worth it?  The estimate is exact: same grouping, same sizes. */
	memset(&est, 0, sizeof(est));
	for (int i = 0; i < n; i++)
		bark_prefix_est_add(&est, attrs, natts, items[i]);
	if (!bark_prefix_est_representable(&est) ||
		(!force && !bark_prefix_worthwhile(bark_prefix_est_coded(&est),
										   est.plainsz)))
		return false;

	dir = bark_prefix_choose_groups(attrs, natts, items, n);
	if (dir == NULL)
	{
		Assert(!force || n == 1);
		return false;			/* nothing shared */
	}

	if (hikey &&
		PageAddItem(newpage, hikey, IndexTupleSize(hikey), BARK_P_HIKEY,
					false, false) == InvalidOffsetNumber)
		elog(ERROR, "could not add high key during BARK prefix compression");

	diritem = bark_prefix_form_diritem(dir, attrs, natts, &diritemsz);
	if (PageAddItem(newpage, diritem, diritemsz,
					BarkPagePrefixOff(BarkPageGetOpaque(newpage)),
					false, false) == InvalidOffsetNumber)
		ok = false;
	BarkPageGetOpaque(newpage)->bark_flags |= BARK_PREFIX;
	newoff = OffsetNumberNext(BarkPagePrefixOff(BarkPageGetOpaque(newpage)));

	for (int i = 0, g = 0; ok && i < n; i++)
	{
		IndexTuple	coded;

		while (g + 1 < dir->ngroups && items[i] == dir->anchors[g + 1].rep)
			g++;

		coded = bark_prefix_encode_against(attrs, natts, dir->anchors[g].rep,
										   items[i], g);
		Assert(coded != NULL);
		if (PageAddItem(newpage, coded, IndexTupleSize(coded), newoff,
						false, false) == InvalidOffsetNumber)
			ok = false;			/* the coded items do not fit one page */
		newoff = OffsetNumberNext(newoff);
		pfree(coded);
	}

	pfree(diritem);
	pfree(dir);
	if (!ok)
		return false;

	bark_prefix_recount(newpage);

#ifdef USE_ASSERT_CHECKING
	/* decode must reproduce every plain tuple byte for byte */
	for (int i = 0; i < n; i++)
	{
		BarkItemBuf chk;
		OffsetNumber off = BarkPageFirstDataKey(BarkPageGetOpaque(newpage)) + i;
		IndexTuple	on = (IndexTuple) PageGetItem(newpage, PageGetItemId(newpage, off));
		IndexTuple	d = bark_prefix_decode(newpage, on, &chk);

		Assert(IndexTupleSize(d) == IndexTupleSize(items[i]) &&
			   memcmp(d, items[i], IndexTupleSize(d)) == 0);
	}
#endif

	return true;
}

/*
 * MAXALIGN'd size of coded member `coded' once decoded, from its header alone.
 * The unit bark_findsplitloc and the CREATE INDEX estimate use.
 */
static inline Size
bark_prefix_plain_size(IndexTuple coded)
{
	BarkPrefixCode *code = (BarkPrefixCode *) ((char *) coded +
											   sizeof(IndexTupleData));

	return MAXALIGN(code->orig_total);
}

/*
 * bark_prefix_split_needed
 *		Must inserting a plain tuple of size `newitemsz' into BARK_PREFIX page
 *		`page' split it first?
 *
 * The split-fit guarantee: a coded page may hold more decoded content than a
 * plain page, but its decoded items must always split into two plain pages.
 * Each coded page keeps T + hikey + 2 * m + heap TID <= 2 * usable, where T is
 * the decoded size of its data items (with line pointers) and m the largest.
 */
bool
bark_prefix_split_needed(Page page, Size newitemsz)
{
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	BarkPrefixDirHdr *hdr = (BarkPrefixDirHdr *) bark_prefix_diritem(page);
	Size		usable = BLCKSZ - SizeOfPageHeaderData -
		MAXALIGN(sizeof(BarkPageOpaqueData));
	Size		newsz = MAXALIGN(newitemsz) + sizeof(ItemIdData);
	Size		total = hdr->decodedsz + newsz;
	Size		maxitem = Max((Size) hdr->maxdecoded, newsz);
	Size		hikeysz = 0;

	if (!BarkPageRightmost(opaque))
		hikeysz = MAXALIGN(ItemIdGetLength(PageGetItemId(page, BARK_P_HIKEY))) +
			sizeof(ItemIdData);

	return total + hikeysz + 2 * maxitem + MAXALIGN(sizeof(ItemPointerData)) >
		2 * usable;
}

/*
 * bark_prefix_recount
 *		Recompute the directory's cached decoded sizes after a change to the
 *		page's data items (deletion, body update, a rebuilt page), or drop the
 *		directory if no data items remain.  Page-only, so WAL redo calls it
 *		after replaying the same change and reproduces the page.
 */
void
bark_prefix_recount(Page page)
{
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	OffsetNumber minoff,
				maxoff;
	BarkPrefixDirHdr *hdr;
	Size		total = 0,
				maxitem = 0;

	if (!BarkPageHasPrefix(opaque))
		return;

	minoff = BarkPageFirstDataKey(opaque);
	maxoff = PageGetMaxOffsetNumber(page);
	if (maxoff < minoff)
	{
		/*
		 * An emptied leaf holds only its high key: drop the directory and
		 * become a plain page.
		 */
		PageIndexTupleDelete(page, BarkPagePrefixOff(opaque));
		opaque->bark_flags &= ~BARK_PREFIX;
		return;
	}

	for (OffsetNumber off = minoff; off <= maxoff; off = OffsetNumberNext(off))
	{
		IndexTuple	coded = (IndexTuple) PageGetItem(page, PageGetItemId(page, off));
		Size		sz = bark_prefix_plain_size(coded) + sizeof(ItemIdData);

		total += sz;
		maxitem = Max(maxitem, sz);
	}
	Assert(total <= PG_UINT16_MAX);
	hdr = (BarkPrefixDirHdr *) bark_prefix_diritem(page);
	hdr->decodedsz = (uint16) total;
	hdr->maxdecoded = (uint16) maxitem;
}

/*
 * bark_prefix_note_insert
 *		Account for coded item `coded' just added to BARK_PREFIX page `page'.
 *		The O(1) counterpart of bark_prefix_recount for a single insert.
 *		Page-only; insert redo calls it too.
 */
void
bark_prefix_note_insert(Page page, IndexTuple coded)
{
	BarkPrefixDirHdr *hdr = (BarkPrefixDirHdr *) bark_prefix_diritem(page);
	Size		sz = bark_prefix_plain_size(coded) + sizeof(ItemIdData);

	Assert(hdr->decodedsz + sz <= PG_UINT16_MAX);
	hdr->decodedsz += (uint16) sz;
	hdr->maxdecoded = (uint16) Max((Size) hdr->maxdecoded, sz);
}
