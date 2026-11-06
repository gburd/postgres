# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/commit_ts/t/001_base.pl.

Single-node commit-timestamp test: with track_commit_timestamp=on, a row's
commit timestamp (pg_xact_commit_timestamp of its xmin) is set close to now()
and survives an immediate-shutdown crash recovery unchanged. Generated from the
Perl original via .agent/gen_golden.py.
"""


def test_001_base(create_pg):
    """Commit timestamp is set and persists across crash recovery."""
    node = create_pg("foxtrot", start=False)
    node.append_conf("track_commit_timestamp=on")
    node.start()
    node.safe_psql("create table t as select now from (select now(), pg_sleep(1)) f")
    true = node.safe_psql(
        "select t.now - ts.* < '1s' from t, pg_class c, pg_xact_commit_timestamp(c.xmin) ts where relname = 't'"
    )
    assert true == "t", "commit TS is set"
    ts = node.safe_psql(
        "select ts.* from pg_class, pg_xact_commit_timestamp(xmin) ts where relname = 't'"
    )
    node.stop("immediate")
    node.start()
    recovered_ts = node.safe_psql(
        "select ts.* from pg_class, pg_xact_commit_timestamp(xmin) ts where relname = 't'"
    )
    assert recovered_ts == ts, "commit TS remains after crash recovery"
