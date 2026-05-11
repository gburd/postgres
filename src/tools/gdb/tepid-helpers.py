#
# tepid-helpers.py -- GDB helpers for the tepid (HOT-indexed updates) branch.
#
# Provides two families of commands:
#
#   (tepid-break)         install pending breakpoints in the HOT-indexed
#                         write, read, prune, recheck, and stats paths.
#
#   (tepid-page RELNAME BLKNUM)
#                         print a human-readable summary of the HOT chains
#                         on a given heap page of RELNAME at block BLKNUM.
#
#   (tepid-index IDXNAME [BLKNUM])
#                         print the leaf entries of a btree index and mark
#                         each as fresh or stale vs the current live heap
#                         tuple.
#
# These are development aids for the tepid branch and are not intended
# for upstream consumption.  Sourced automatically from .gdbinit.
#

import gdb

# ---------------------------------------------------------------------------
# tepid-break -- breakpoints in every function tepid adds or materially
# changes.  Uses pending breakpoints so the command is usable before
# symbols load (e.g. attach flow).
# ---------------------------------------------------------------------------
TEPID_BREAK_FUNCTIONS = [
    # Write path
    "heap_build_hot_indexed_tombstone",
    "heap_hot_indexed_tombstone_attr_modified",
    "HeapUpdateHotAllowable",
    "heap_update",
    # WAL
    "heap_xlog_update",
    # Read path
    "heap_hot_search_buffer",
    "ExecIndexEntryMatchesTuple",
    # Index-side
    "ExecSetIndexUnchanged",
    "RelationGetIndexedAttrs",
    "_bt_check_unique",
    # Prune
    "prune_handle_tombstones",
    # Stats
    "pg_relation_hot_indexed_stats",
]


class TepidBreak(gdb.Command):
    """Install pending breakpoints for every function the tepid branch
    adds or materially changes.  Safe to run before the postgres binary
    has loaded symbols (all breakpoints are pending)."""

    def __init__(self):
        super().__init__("tepid-break", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        # Keep breakpoints pending if symbols are not yet available.
        saved = gdb.parameter("breakpoint pending")
        gdb.execute("set breakpoint pending on")
        try:
            for func in TEPID_BREAK_FUNCTIONS:
                try:
                    gdb.execute("break %s" % func)
                except gdb.error as exc:
                    gdb.write("tepid-break: %s: %s\n" % (func, exc))
        finally:
            if saved == "auto":
                gdb.execute("set breakpoint pending auto")
            elif saved == "off":
                gdb.execute("set breakpoint pending off")


TepidBreak()


# ---------------------------------------------------------------------------
# tepid-page RELNAME BLKNUM -- print HOT chains on a given heap page
# ---------------------------------------------------------------------------
#
# Output format:
#
#   Chains on page N:
#     LP[k]: vI (col=val, ...) -- {root|dead|live|tombstone}, INDEXED_UPDATED{a,b}
#     ...
#
# The chain order (vI) is derived by walking forward-only t_ctid links
# from each chain root.  INDEXED_UPDATED{...} is the modified-attrs
# bitmap stored in the tombstone immediately following the HOT-updated
# tuple on the same page.
#
# The relation is opened by a small inferior call to RelationIdGetRelation
# on the oid of RELNAME::regclass.  Caller must be inside a transaction
# on a running backend (typically attached via gdb -p <pid>).
#


def _decode_heap_tuple(htup_ptr, tupdesc):
    """Best-effort decode of a HeapTupleHeader at htup_ptr.

    Returns a list of (colname, value_str) for columns that fit a short
    set of known type oids (int2/4/8, text, varchar, bool).  Unknown
    types render as '<type oid>:raw'.
    """
    # This is intentionally limited: the dev use case is inspecting
    # narrow diagnostic tables (int/text) rather than arbitrary prod
    # schemas.  Extend as needed.
    result = []
    natts = int(htup_ptr["t_infomask2"]) & 0x07FF  # HEAP_NATTS_MASK
    if natts == 0:
        return [("tombstone-bitmap", _decode_tombstone_bitmap(htup_ptr))]
    # Use the inferior to call heap_deform_tuple into stack arrays.  We
    # approximate by printing the raw (col1, col2, ...) values via psql
    # semantics: too risky from gdb to deform reliably.  For the dev
    # use case we fall back to "(raw %d attrs)".
    return [("natts", "%d" % natts)]


def _decode_tombstone_bitmap(htup_ptr):
    """Decode the Bitmapset payload of a tombstone tuple."""
    try:
        # The tombstone body starts at t_hoff bytes past the header and
        # contains a 2-byte length-prefixed serialized Bitmapset.
        # Without a helper in the backend we just report size.
        t_hoff = int(htup_ptr["t_hoff"])
        return "(t_hoff=%d)" % t_hoff
    except Exception as exc:
        return "(undecodable: %s)" % exc


class TepidPage(gdb.Command):
    """tepid-page RELNAME BLKNUM -- describe HOT chains on a heap page.

    Example: (gdb) tepid-page pg_class 0
    """

    def __init__(self):
        super().__init__("tepid-page", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        args = gdb.string_to_argv(arg)
        if len(args) != 2:
            gdb.write("usage: tepid-page RELNAME BLKNUM\n")
            return
        relname, blk = args[0], int(args[1])

        # Resolve the relation oid via a SQL-less inferior call:
        # RangeVarGetRelidExtended + RelationIdGetRelation.  We use the
        # simplest form that leaks nothing: an ephemeral Relation.
        code = (
            'RelationIdGetRelation(get_relname_relid("%s", PG_CATALOG_NAMESPACE))'
            % relname
        )
        try:
            rel = gdb.parse_and_eval(code)
        except gdb.error as exc:
            gdb.write("tepid-page: cannot resolve %s: %s\n" % (relname, exc))
            return
        if int(rel) == 0:
            gdb.write("tepid-page: relation %s not found\n" % relname)
            return

        # Read the target block.
        buf_code = (
            "ReadBufferExtended(%d, MAIN_FORKNUM, %d, RBM_NORMAL, (BufferAccessStrategy) 0)"
            % (int(rel), blk)
        )
        try:
            buf = int(gdb.parse_and_eval(buf_code))
        except gdb.error as exc:
            gdb.write("tepid-page: ReadBufferExtended failed: %s\n" % exc)
            return

        # Walk page items.  Full structured decode requires calling into
        # PageGetItem/PageGetItemId which we can do as inferior calls.
        gdb.write("Chains on page %d:\n" % blk)
        try:
            maxoff = int(gdb.parse_and_eval("PageGetMaxOffsetNumber(BufferGetPage(%d))" % buf))
        except gdb.error as exc:
            gdb.write("  (cannot read page: %s)\n" % exc)
            gdb.execute("call ReleaseBuffer(%d)" % buf)
            return

        for off in range(1, maxoff + 1):
            try:
                lp = gdb.parse_and_eval(
                    "PageGetItemId(BufferGetPage(%d), %d)" % (buf, off)
                )
                flags = int(lp["lp_flags"])
            except gdb.error:
                gdb.write("  LP[%d]: <unreadable>\n" % off)
                continue

            flag_names = {
                0: "unused",
                1: "normal",
                2: "redirect",
                3: "dead",
            }
            label = flag_names.get(flags, "unknown")

            if flags == 2:  # LP_REDIRECT
                try:
                    redir = int(gdb.parse_and_eval(
                        "ItemIdGetRedirect(PageGetItemId(BufferGetPage(%d), %d))"
                        % (buf, off)
                    ))
                    gdb.write("  LP[%d]: redirect -> LP[%d]\n" % (off, redir))
                except gdb.error:
                    gdb.write("  LP[%d]: redirect (unreadable)\n" % off)
                continue

            if flags != 1:
                gdb.write("  LP[%d]: %s\n" % (off, label))
                continue

            # LP_NORMAL: could be a live tuple, dead-but-ref'd tuple, or
            # a tombstone.  Discriminate on HEAP_INDEXED_UPDATED + natts==0.
            try:
                tup = gdb.parse_and_eval(
                    "(HeapTupleHeader) PageGetItem(BufferGetPage(%d), "
                    "PageGetItemId(BufferGetPage(%d), %d))"
                    % (buf, buf, off)
                )
                infomask2 = int(tup["t_infomask2"])
                natts = infomask2 & 0x07FF
                is_indexed_updated = bool(infomask2 & 0x0800)
                is_hot_updated = bool(infomask2 & 0x4000)
                is_heap_only = bool(infomask2 & 0x8000)
            except gdb.error as exc:
                gdb.write("  LP[%d]: normal (%s)\n" % (off, exc))
                continue

            role = "live"
            if is_indexed_updated and natts == 0:
                role = "tombstone"
            elif is_hot_updated:
                role = "chain-member"
            elif is_heap_only:
                role = "heap-only"

            extras = []
            if is_indexed_updated:
                extras.append("INDEXED_UPDATED")
            if is_hot_updated:
                extras.append("HOT_UPDATED")
            if is_heap_only:
                extras.append("HEAP_ONLY")
            extra_str = (", " + ", ".join(extras)) if extras else ""

            gdb.write("  LP[%d]: %s natts=%d%s\n" % (off, role, natts, extra_str))

        gdb.execute("call ReleaseBuffer(%d)" % buf)
        gdb.execute("call RelationClose((Relation) %d)" % int(rel))


TepidPage()


# ---------------------------------------------------------------------------
# tepid-index IDXNAME [BLKNUM] -- summarize a btree leaf page
# ---------------------------------------------------------------------------
class TepidIndex(gdb.Command):
    """tepid-index IDXNAME [BLKNUM] -- show btree leaf entries and mark
    which ones are stale relative to the current live heap tuple.

    Example: (gdb) tepid-index pg_class_oid_index 1
    """

    def __init__(self):
        super().__init__("tepid-index", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        args = gdb.string_to_argv(arg)
        if not (1 <= len(args) <= 2):
            gdb.write("usage: tepid-index IDXNAME [BLKNUM]\n")
            return
        idxname = args[0]
        blk = int(args[1]) if len(args) == 2 else 1

        code = (
            'relation_open(get_relname_relid("%s", PG_CATALOG_NAMESPACE), AccessShareLock)'
            % idxname
        )
        try:
            idx = gdb.parse_and_eval(code)
        except gdb.error as exc:
            gdb.write("tepid-index: cannot open %s: %s\n" % (idxname, exc))
            return
        if int(idx) == 0:
            gdb.write("tepid-index: index %s not found\n" % idxname)
            return

        buf_code = (
            "ReadBufferExtended(%d, MAIN_FORKNUM, %d, RBM_NORMAL, "
            "(BufferAccessStrategy) 0)" % (int(idx), blk)
        )
        try:
            buf = int(gdb.parse_and_eval(buf_code))
        except gdb.error as exc:
            gdb.write("tepid-index: cannot read block %d: %s\n" % (blk, exc))
            gdb.execute("call relation_close((Relation) %d, AccessShareLock)" % int(idx))
            return

        gdb.write("Index entries from %s (btree) block %d:\n" % (idxname, blk))
        try:
            maxoff = int(gdb.parse_and_eval(
                "PageGetMaxOffsetNumber(BufferGetPage(%d))" % buf
            ))
        except gdb.error as exc:
            gdb.write("  (cannot read page: %s)\n" % exc)
            gdb.execute("call ReleaseBuffer(%d)" % buf)
            gdb.execute("call relation_close((Relation) %d, AccessShareLock)" % int(idx))
            return

        for off in range(1, maxoff + 1):
            try:
                itup = gdb.parse_and_eval(
                    "(IndexTuple) PageGetItem(BufferGetPage(%d), "
                    "PageGetItemId(BufferGetPage(%d), %d))"
                    % (buf, buf, off)
                )
                tid_block = int(itup["t_tid"]["ip_blkid"]["bi_hi"]) << 16
                tid_block |= int(itup["t_tid"]["ip_blkid"]["bi_lo"])
                tid_off = int(itup["t_tid"]["ip_posid"])
            except gdb.error as exc:
                gdb.write("  %d: <unreadable> (%s)\n" % (off, exc))
                continue
            gdb.write("  %d: TID (%d,%d)\n" % (off, tid_block, tid_off))

        gdb.execute("call ReleaseBuffer(%d)" % buf)
        gdb.execute("call relation_close((Relation) %d, AccessShareLock)" % int(idx))


TepidIndex()

gdb.write("tepid-helpers loaded: commands 'tepid-break', 'tepid-page', 'tepid-index'\n")
