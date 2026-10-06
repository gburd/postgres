# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of contrib/amcheck/t/003_cic_2pc.pl.

CREATE INDEX CONCURRENTLY (btree and gin) interleaved with prepared (two-phase)
transactions, then across a server restart, must produce structurally valid
indexes: bt_index_check/gin_index_check pass. Finally a concurrent pgbench
workload mixes 2PC inserts with CIC/REINDEX CONCURRENTLY under an advisory lock,
checking the indexes throughout.
"""

import pypg


def _build_indexes_with_2pc(node):
    main_h = node.background_psql("postgres")
    main_h.query_safe("BEGIN;\nINSERT INTO tbl VALUES(0, '[[14,2,3]]');\n")
    cic_h = node.background_psql("postgres")
    cic_h.query_until(
        r"start",
        "\\echo start\nCREATE INDEX CONCURRENTLY idx ON tbl(i);\n"
        "CREATE INDEX CONCURRENTLY ginidx ON tbl USING gin(j);\n",
    )
    main_h.query_safe("PREPARE TRANSACTION 'a';\n")
    main_h.query_safe("BEGIN;\nINSERT INTO tbl VALUES(0, '[[14,2,3]]');\n")
    node.safe_psql("COMMIT PREPARED 'a';")
    main_h.query_safe(
        "PREPARE TRANSACTION 'b';\nBEGIN;\n"
        "INSERT INTO tbl VALUES(0, '\"mary had a little lamb\"');\n"
    )
    node.safe_psql("COMMIT PREPARED 'b';")
    main_h.query_safe("PREPARE TRANSACTION 'c';\nCOMMIT PREPARED 'c';\n")
    main_h.quit()
    cic_h.quit()


def test_003_cic_2pc(create_pg):
    """CIC interleaved with 2PC and across restart yields valid indexes."""
    node = create_pg("CIC_2PC_test", start=False)
    node.append_conf("max_prepared_transactions = 10")
    node.append_conf("lock_timeout = {}".format(1000 * pypg.test_timeout_default()))
    node.start()
    node.safe_psql("CREATE EXTENSION amcheck")
    node.safe_psql("CREATE TABLE tbl(i int, j jsonb)")
    _build_indexes_with_2pc(node)
    assert (
        node.psql_capture("SELECT bt_index_check('idx',true)").exit_code == 0
    ), "bt_index_check after overlapping 2PC"
    assert (
        node.psql_capture("SELECT gin_index_check('ginidx')").exit_code == 0
    ), "gin_index_check after overlapping 2PC"
    node.safe_psql(
        "BEGIN;\nINSERT INTO tbl VALUES(0, "
        '\'{"a":[["b",{"x":1}],["b",{"x":2}]],"c":3}\');\n'
        "PREPARE TRANSACTION 'spans_restart';\nBEGIN;\nCREATE TABLE unused ();\n"
        "PREPARE TRANSACTION 'persists_forever';\n"
    )
    node.restart()
    reindex_h = node.background_psql("postgres")
    reindex_h.query_until(
        r"start",
        "\\echo start\nDROP INDEX CONCURRENTLY idx;\n"
        "CREATE INDEX CONCURRENTLY idx ON tbl(i);\n"
        "DROP INDEX CONCURRENTLY ginidx;\n"
        "CREATE INDEX CONCURRENTLY ginidx ON tbl USING gin(j);\n",
    )
    node.safe_psql("COMMIT PREPARED 'spans_restart'")
    reindex_h.quit()
    assert (
        node.psql_capture("SELECT bt_index_check('idx',true)").exit_code == 0
    ), "bt_index_check after 2PC and restart"
    assert (
        node.psql_capture("SELECT gin_index_check('ginidx')").exit_code == 0
    ), "gin_index_check after 2PC and restart"
    node.safe_psql("REINDEX TABLE tbl;")
    node.pgbench(
        "--no-vacuum --client=5 --transactions=100",
        0,
        [r"actually processed"],
        [r"^$"],
        "concurrent INSERTs w/ 2PC and CIC",
        _PGBENCH_FILES,
    )
    node.stop()


_PGBENCH_FILES = {
    "003_pgbench_concurrent_2pc": (
        "BEGIN;\nINSERT INTO tbl VALUES(0,'null');\n"
        "PREPARE TRANSACTION 'c:client_id';\nCOMMIT PREPARED 'c:client_id';\n"
    ),
    "003_pgbench_concurrent_2pc_savepoint": (
        "BEGIN;\nSAVEPOINT s1;\n"
        'INSERT INTO tbl VALUES(0,\'[false, "jnvaba", -76, 7, {"_": [1]}, 9]\');\n'
        "PREPARE TRANSACTION 'c:client_id';\nCOMMIT PREPARED 'c:client_id';\n"
    ),
    "003_pgbench_concurrent_cic": (
        "SELECT pg_try_advisory_lock(42)::integer AS gotlock \\gset\n"
        "\\if :gotlock\n\tDROP INDEX CONCURRENTLY idx;\n"
        "\tCREATE INDEX CONCURRENTLY idx ON tbl(i);\n"
        "\tSELECT bt_index_check('idx',true);\n"
        "\tSELECT pg_advisory_unlock(42);\n\\endif\n"
    ),
    "004_pgbench_concurrent_ric": (
        "SELECT pg_try_advisory_lock(42)::integer AS gotlock \\gset\n"
        "\\if :gotlock\n\tREINDEX INDEX CONCURRENTLY idx;\n"
        "\tSELECT bt_index_check('idx',true);\n"
        "\tSELECT pg_advisory_unlock(42);\n\\endif\n"
    ),
    "005_pgbench_concurrent_cic": (
        "SELECT pg_try_advisory_lock(42)::integer AS gotginlock \\gset\n"
        "\\if :gotginlock\n\tDROP INDEX CONCURRENTLY ginidx;\n"
        "\tCREATE INDEX CONCURRENTLY ginidx ON tbl USING gin(j);\n"
        "\tSELECT gin_index_check('ginidx');\n"
        "\tSELECT pg_advisory_unlock(42);\n\\endif\n"
    ),
    "006_pgbench_concurrent_ric": (
        "SELECT pg_try_advisory_lock(42)::integer AS gotginlock \\gset\n"
        "\\if :gotginlock\n\tREINDEX INDEX CONCURRENTLY ginidx;\n"
        "\tSELECT gin_index_check('ginidx');\n"
        "\tSELECT pg_advisory_unlock(42);\n\\endif\n"
    ),
}
