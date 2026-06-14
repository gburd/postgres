# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of contrib/amcheck/t/002_cic.pl.

amcheck verification (bt_index_check / gin_index_check / bt_index_parent_check)
holds up under concurrent INSERTs and CREATE/DROP INDEX CONCURRENTLY: a pgbench
workload runs transactions and CIC in parallel, and a parent check after a row
removed by a still-in-progress transaction reports no corruption.
"""

import pypg


def test_002_cic(create_pg):
    """amcheck under concurrent INSERTs and CREATE INDEX CONCURRENTLY."""
    node = create_pg("CIC_test", start=False)
    node.append_conf("lock_timeout = {}".format(1000 * pypg.test_timeout_default()))
    node.start()
    node.safe_psql("CREATE EXTENSION amcheck")
    node.safe_psql("CREATE TABLE tbl(i int, j jsonb)")
    node.safe_psql("CREATE INDEX idx ON tbl(i)")
    node.safe_psql("CREATE INDEX ginidx ON tbl USING gin(j)")
    node.pgbench(
        "--no-vacuum --client=5 --transactions=100",
        0,
        [r"actually processed"],
        [r"^$"],
        "concurrent INSERTs and CIC",
        {
            "002_pgbench_concurrent_transaction": (
                "BEGIN;\n"
                "INSERT INTO tbl VALUES(0,"
                ' \'{"a":[["b",{"x":1}],["b",{"x":2}]],"c":3}\');\n'
                "COMMIT;\n"
            ),
            "002_pgbench_concurrent_transaction_savepoints": (
                "BEGIN;\nSAVEPOINT s1;\nINSERT INTO tbl VALUES(0, '[[14,2,3]]');\n"
                "COMMIT;\n"
            ),
            "002_pgbench_concurrent_cic": (
                "SELECT pg_try_advisory_lock(42)::integer AS gotlock \\gset\n"
                "\\if :gotlock\n"
                "    DROP INDEX CONCURRENTLY idx;\n"
                "    CREATE INDEX CONCURRENTLY idx ON tbl(i);\n"
                "    DROP INDEX CONCURRENTLY ginidx;\n"
                "    CREATE INDEX CONCURRENTLY ginidx ON tbl USING gin(j);\n"
                "    SELECT bt_index_check('idx',true);\n"
                "    SELECT gin_index_check('ginidx');\n"
                "    SELECT pg_advisory_unlock(42);\n"
                "\\endif\n"
            ),
        },
    )
    node.safe_psql("CREATE TABLE quebec(i int primary key)")
    node.safe_psql("INSERT INTO quebec SELECT i FROM generate_series(1, 2) s(i);")
    in_progress_h = node.background_psql("postgres")
    in_progress_h.query("BEGIN; SELECT pg_current_xact_id();")
    node.safe_psql("DELETE FROM quebec WHERE i = 1;")
    node.safe_psql("CREATE INDEX CONCURRENTLY oscar ON quebec(i);")
    result = node.psql_capture(
        "SELECT bt_index_parent_check('oscar', heapallindexed => true)"
    )
    assert result.rc == 0, "bt_index_parent_check for CIC after removed row"
    in_progress_h.quit()
    node.stop()
