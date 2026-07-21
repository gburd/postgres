/*-------------------------------------------------------------------------
 *
 * rowid.h
 *	  Table-AM-governed row identifier ("RowID") contract for index access.
 *
 * An index entry must be able to (Role 1) locate the table row it points at
 * and (Role 2) provide a total order that makes the (index-key, row-id) pair
 * unique so the index AM can dedup, order, and tiebreak entries with equal
 * keys.  Historically both roles were the fixed 6-byte heap ItemPointer
 * ("TID"), hardcoded throughout the index and executor layers.  That bakes
 * heap's model into the index AM: a table AM that reuses a TID across row
 * versions (an in-place-MVCC AM such as RECNO) cannot make its equal-key
 * entries distinct, because two versions of one row share one 6-byte TID.
 *
 * The RowIDType descriptor decouples the row identity from the index AM.  The
 * table AM registers a descriptor declaring the identity's width and how to
 * compare it.  The index AM stores a RowID of the declared width per entry and
 * orders entries by the descriptor's comparator, without interpreting the
 * bytes and without any "is this a heap TID" test.  The index AM never turns a
 * RowID into a locator: to fetch or delete a row it hands the opaque RowID
 * back to the table AM (index_fetch_tuple / table_index_delete_tuples), which
 * resolves it however it likes.  An index-organized table (identity = primary
 * key) is therefore expressible: it never has to produce a TID.
 *
 * Heap registers a descriptor of width sizeof(ItemPointerData) whose bytes are
 * exactly the heap TID and whose comparator is ItemPointerCompare, so heap
 * index tuples are byte-for-byte identical to before -- heap is one descriptor
 * instance with a fixed-6-byte path, no privileged branch.  RECNO registers a
 * wider descriptor whose RowID is (stable TID, generation): the table AM (not
 * the index AM) resolves the TID part for fetch; the generation makes A->B->A
 * recurrences on one physical TID distinct in the index so RECNO can keep the
 * tuple in place across every UPDATE.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/rowid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ROWID_H
#define ROWID_H

#include "storage/itemptr.h"

/*
 * Maximum width, in bytes, of any table AM's RowID.  Bounds the inline buffer
 * carried by index scans and tuple slots so no per-row allocation is needed.
 * Heap uses 6 (a bare ItemPointerData); RECNO uses 10 (ItemPointerData + a
 * uint32 generation).  Raise only with care: it sizes on-stack/embedded
 * buffers on hot paths.
 */
#define MAX_ROWID_SIZE		16

/*
 * A RowID value as handled at the index<->table boundary: an opaque byte
 * string of length `len` (<= MAX_ROWID_SIZE).  Compared, stored, and hashed
 * by the owning table AM's RowIDType; never interpreted by the index AM.
 */
typedef struct RowID
{
	uint8		len;
	uint8		data[MAX_ROWID_SIZE];
} RowID;

/*
 * RowIDCmpFn -- total order over two stored RowID byte strings of the widths
 * the descriptor declares.  Returns <0, 0, or >0.  Used by the index AM as the
 * tiebreaker over equal keys.  For heap this is ItemPointerCompare over the
 * two 6-byte values.
 */
typedef int32 (*RowIDCmpFn) (const uint8 *a, const uint8 *b);

/*
 * RowIDPostIter -- opaque-ish iterator state for walking a codec-encoded
 * posting payload in strict ascending RowID (cmp) order.  The codec owns the
 * layout; only the codec's iter_next reads these fields.  Sized to hold a
 * sparsemap cursor + a decode scratch RowID without a per-iteration alloc.
 */
typedef struct RowIDPostIter
{
	const uint8 *payload;		/* the encoded posting bytes */
	Size		len;			/* payload length */
	uint32		pos;			/* codec-private cursor (e.g. next index) */
	void	   *cur;			/* codec-private state (e.g. sparsemap_t *) */
	uint64		last;			/* codec-private: last key yielded */
	void	   *cur2;			/* codec-private: iteration cursor (sm_cursor_t *) */
} RowIDPostIter;

/*
 * RowIDPostingOps -- a table AM's OPTIONAL index deduplication posting codec.
 *
 * When a RowIDType supplies this, the index AM (nbtree) may collapse many
 * equal-key entries into ONE posting tuple whose row-identity payload the
 * codec owns, instead of storing one leaf tuple per equal-key row.  This is
 * how a wide-RowID AM (RECNO, width > sizeof(ItemPointerData)) regains
 * deduplication: nbtree disables its native 6-byte-TID posting for wide
 * RowIDs, and defers the encoding to this codec.  Heap leaves this NULL and
 * keeps nbtree's native TID-array posting, byte-for-byte unchanged.
 *
 * CONTRACT: encode/iter MUST preserve strict ascending cmp order; the codec
 * is bulk-build + ordered-iterate ONLY (never a random point-membership probe
 * on a hot path).  encode returns 0 ("does not fit / not worth it") to make
 * the index AM fall back to the plain multi-tuple representation.
 */
typedef struct RowIDPostingOps
{
	/* Encode `n` ascending-by-cmp RowIDs into `out` (cap `outcap`); return
	 * bytes written, or 0 to decline (caller keeps plain tuples).  Must be
	 * deterministic (amcheck re-encodes and compares). */
	Size		(*encode) (const RowID *rowids, int n, uint8 *out, Size outcap);

	/* Number of RowIDs in an encoded payload (cheap header read). */
	int			(*count) (const uint8 *payload, Size len);

	/* Decode the i'th RowID (0-based, ascending) into *out. */
	void		(*decode_n) (const uint8 *payload, Size len, int i, RowID *out);

	/* Begin an ascending iteration; iter_next yields each RowID in order,
	 * returning false when exhausted.  Cursor-backed: O(total), not O(n*chunks). */
	void		(*iter_begin) (const uint8 *payload, Size len, RowIDPostIter *it);
	bool		(*iter_next) (RowIDPostIter *it, RowID *out);

	/* Remove `ndead` RowIDs from a payload (VACUUM / index delete) into `out`;
	 * return new length, or 0 meaning "< 2 remain -> caller falls back to a
	 * plain tuple".  `dead` is ascending by cmp. */
	Size		(*remove) (const uint8 *payload, Size len,
						  const RowID *dead, int ndead, uint8 *out, Size outcap);
} RowIDPostingOps;

/*
 * RowIDType -- a table AM's description of its index row identity.  Treated as
 * opaque, immutable data by the index AM, which only reads `width` and calls
 * `cmp`.  Locating the row from a RowID is the table AM's job, done via
 * index_fetch_tuple / table_index_delete_tuples on the opaque RowID -- there
 * is deliberately no "give me the TID" callback here, because that would
 * re-assert that every identity contains a heap TID the index may interpret.
 */
typedef struct RowIDType
{
	/* Fixed width, in bytes, of this AM's stored RowID (<= MAX_ROWID_SIZE). */
	uint8		width;

	/*
	 * Total order over two stored RowID values (Role 2 tiebreaker).  Both
	 * arguments point at `width` bytes.  Must be a strict total order so the
	 * index AM's ordering invariants hold.
	 */
	RowIDCmpFn	cmp;

	/*
	 * OPTIONAL index deduplication posting codec (Role 2 space optimization).
	 * NULL means "no codec": heap uses nbtree's native 6-byte-TID posting; a
	 * wide-RowID AM with a NULL codec simply gets no deduplication (one leaf
	 * tuple per equal-key row).  A non-NULL codec lets nbtree collapse
	 * equal-key rows into a codec-encoded posting tuple.  Read by nbtree only
	 * to decide dedup eligibility and to encode/decode/iterate/remove the
	 * posting payload; never interpreted otherwise.
	 */
	const RowIDPostingOps *posting;
} RowIDType;

/* The in-core heap descriptor: width 6, cmp == ItemPointerCompare. */
extern const RowIDType HeapRowIDType;

/* Comparator matching RowIDCmpFn for a 6-byte heap-TID identity. */
extern int32 rowid_tid_compare(const uint8 *a, const uint8 *b);

#endif							/* ROWID_H */
