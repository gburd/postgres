/* src/test/modules/test_idxundo/test_idxundo--1.0.sql */
\echo Use "CREATE EXTENSION test_idxundo" to load this file. \quit

CREATE FUNCTION idxundo_apply_nbtree_leaf(index regclass, blkno int, "offset" int)
RETURNS text AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE FUNCTION idxundo_apply_hash(index regclass, blkno int, "offset" int)
RETURNS text AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE FUNCTION idxundo_count_dead(index regclass, blkno int)
RETURNS int AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE FUNCTION idxundo_capture(index regclass, blkno int, "offset" int)
RETURNS bytea AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE FUNCTION idxundo_apply_nbtree_leaf_with(index regclass, blkno int, "offset" int, itup bytea)
RETURNS text AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE FUNCTION idxundo_apply_hash_with(index regclass, blkno int, "offset" int, itup bytea)
RETURNS text AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Total LP_DEAD line pointers across every block of an index.  Used by the
-- end-to-end tests, where the aborted entries' block is not known in advance.
CREATE FUNCTION idxundo_count_dead_all(index regclass)
RETURNS int AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Does the current transaction hold published index UNDO?  Reports
-- XactUndo.has_undo together with whether a permanent chain-head batch LSN was
-- recorded -- the two pieces AtAbort_XactUndo() requires before it will walk a
-- chain.  The write path used to satisfy neither, which made rollback a silent
-- no-op; this is the regression guard for that.
CREATE FUNCTION idxundo_undo_chain_published()
RETURNS bool AS 'MODULE_PATHNAME' LANGUAGE C STRICT;
