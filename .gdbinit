# tepid dev additions: .gdbinit for debugging the HOT-indexed updates branch
#
# Usage:
#   gdb -x .gdbinit <postgres-binary>
#   source .gdbinit            (from inside gdb)
#
# This file is tracked in the repo for developer convenience on the tepid
# branch.  It is not intended for upstream consumption and is ignored when
# generating patches for the mailing list.
#
# What this file does:
#   - Sources src/tools/gdb/tepid-helpers.py, which registers three
#     commands: tepid-break, tepid-page, tepid-index.
#   - Calls tepid-break immediately to install pending breakpoints in
#     every function the branch adds or materially changes.  Breakpoints
#     are pending so the command is safe to run before symbols load (e.g.
#     before attach).
#
# Breakpoints fall into four functional groups:
#   Write path:    heap_build_hot_indexed_tombstone,
#                  heap_hot_indexed_tombstone_attr_modified,
#                  HeapUpdateHotAllowable, heap_update
#   WAL:           heap_xlog_update
#   Read path:     heap_hot_search_buffer, ExecIndexEntryMatchesTuple,
#                  ExecSetIndexUnchanged, RelationGetIndexedAttrs,
#                  _bt_check_unique
#   Prune:         prune_handle_tombstones
#   Stats:         pg_relation_hot_indexed_stats
#
# To disable a specific breakpoint group temporarily use gdb's own
# "disable" / "enable" commands with the breakpoint numbers shown by
# "info breakpoints" after tepid-break runs.

# Keep a local repo-rooted path in sync with the worktree.
source src/tools/gdb/tepid-helpers.py

# Install the breakpoints.  Pending mode keeps them queued until the
# postgres binary has loaded symbols.
set breakpoint pending on
tepid-break

# Convenience: print (col=val, ...) tuples, one per line.
set print pretty on
set print array on
set print union on

# Useful aliases that don't have command-class entries in tepid-helpers.
define tbreak
    tepid-break
end
document tbreak
    Alias for tepid-break.  Installs pending breakpoints for every
    function the tepid branch adds or materially changes.
end

define tpage
    tepid-page $arg0 $arg1
end
document tpage
    tpage RELNAME BLKNUM -- show HOT chains on a heap page.  Wraps
    tepid-page; identical argument syntax.
end

define tindex
    tepid-index $arg0 $arg1
end
document tindex
    tindex IDXNAME BLKNUM -- show btree leaf entries.  Wraps
    tepid-index; identical argument syntax.
end
