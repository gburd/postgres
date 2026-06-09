-- read_indexscan: read-only btree index-scan workload used to isolate the
-- cost of requesting the index tuple on every btree scan (xs_want_itup),
-- which the HOT-indexed read path does unconditionally.  Run against a
-- freshly reset siu_table (no stale HOT-indexed entries), so the only
-- master-vs-tepid difference on this cell is that per-scan itup handling --
-- not any leaf-key recheck work.  The predicate is an equality on the
-- indexed column b and the target list includes the non-indexed column e,
-- forcing a plain (heap-fetching) index scan rather than an index-only scan.
\set id random(1, :rows)
SELECT a, b, c, d, e FROM siu_table WHERE b = :id;
