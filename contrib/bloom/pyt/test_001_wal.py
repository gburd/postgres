# Copyright (c) 2018-2026, PostgreSQL Global Development Group

"""Port of contrib/bloom/t/001_wal.pl.

Bloom-index WAL replay: a bloom index built and repeatedly modified
(delete/vacuum/insert cycles) on a streaming primary must replay identically on
a hot standby, so index scans return the same rows on both nodes at every step.
"""

_QUERIES = """SET enable_seqscan=off;
SET enable_bitmapscan=on;
SET enable_indexscan=on;
SELECT * FROM tst WHERE i = 0;
SELECT * FROM tst WHERE i = 3;
SELECT * FROM tst WHERE t = 'b';
SELECT * FROM tst WHERE t = 'f';
SELECT * FROM tst WHERE i = 3 AND t = 'c';
SELECT * FROM tst WHERE i = 7 AND t = 'e';
"""


def _test_index_replay(primary, standby, test_name):
    """Wait for catch-up, then assert index-scan results match on both nodes."""
    primary.wait_for_catchup(standby)
    primary_result = primary.safe_psql(_QUERIES)
    standby_result = standby.safe_psql(_QUERIES)
    assert primary_result == standby_result, "{}: query result matches".format(
        test_name
    )


def test_001_wal(create_pg):
    """Bloom index changes replay identically on a hot standby."""
    node_primary = create_pg("primary", allows_streaming=True)
    backup_name = "my_backup"
    node_primary.backup(backup_name)
    node_standby = create_pg(
        "standby", from_backup=(node_primary, backup_name), has_streaming=True
    )
    node_primary.safe_psql("CREATE EXTENSION bloom;")
    node_primary.safe_psql("CREATE TABLE tst (i int4, t text);")
    node_primary.safe_psql(
        "INSERT INTO tst SELECT i%10, substr(encode(sha256(i::text::bytea), "
        "'hex'), 1, 1) FROM generate_series(1,10000) i;"
    )
    node_primary.safe_psql(
        "CREATE INDEX bloomidx ON tst USING bloom (i, t) WITH (col1 = 3);"
    )
    _test_index_replay(node_primary, node_standby, "initial")
    for i in range(1, 11):
        node_primary.safe_psql("DELETE FROM tst WHERE i = {};".format(i))
        _test_index_replay(node_primary, node_standby, "delete {}".format(i))
        node_primary.safe_psql("VACUUM tst;")
        _test_index_replay(node_primary, node_standby, "vacuum {}".format(i))
        start = 100001 + (i - 1) * 10000
        end = 100000 + i * 10000
        node_primary.safe_psql(
            "INSERT INTO tst SELECT i%10, substr(encode(sha256(i::text::bytea), "
            "'hex'), 1, 1) FROM generate_series({},{}) i;".format(start, end)
        )
        _test_index_replay(node_primary, node_standby, "insert {}".format(i))
