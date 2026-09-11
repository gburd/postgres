# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/022_twophase_cascade.pl.

Cascading logical replication of 2PC (node_A -> node_B -> node_C), both
non-streaming and streaming.
"""

_APP_B = "tap_sub_B"
_APP_C = "tap_sub_C"
_TWOPHASE = (
    "SELECT count(1) = 0 FROM pg_subscription WHERE subtwophasestate NOT IN ('e');"
)


def _prepared(node):
    return node.safe_psql("SELECT count(*) FROM pg_prepared_xacts;")


def _cascade_catchup(node_a, node_b):
    node_a.wait_for_catchup(_APP_B)
    node_b.wait_for_catchup(_APP_C)


def _both_prepared(node_b, node_c, expected, msg):
    assert _prepared(node_b) == expected, msg + " B"
    assert _prepared(node_c) == expected, msg + " C"


def _setup(node_a, node_b, node_c):
    for node in (node_a, node_b, node_c):
        node.append_conf(
            "max_prepared_transactions = 10\nlogical_decoding_work_mem = 64kB"
        )
        node.start()

    node_a.safe_psql("CREATE TABLE tab_full (a int PRIMARY KEY)")
    node_a.safe_psql("INSERT INTO tab_full SELECT generate_series(1,10);")
    node_a.safe_psql("CREATE TABLE test_tab (a int primary key, b bytea)")
    node_a.safe_psql("INSERT INTO test_tab VALUES (1, 'foo'), (2, 'bar')")
    for node in (node_b, node_c):
        node.safe_psql("CREATE TABLE tab_full (a int PRIMARY KEY)")
        node.safe_psql(
            "CREATE TABLE test_tab (a int primary key, b bytea, "
            "c timestamptz DEFAULT now(), d bigint DEFAULT 999)"
        )

    a_connstr = node_a.connstr() + " dbname=postgres"
    node_a.safe_psql("CREATE PUBLICATION tap_pub_A FOR TABLE tab_full, test_tab")
    node_b.safe_psql(
        "CREATE SUBSCRIPTION tap_sub_B CONNECTION "
        "'{} application_name={}' PUBLICATION tap_pub_A "
        "WITH (two_phase = on, streaming = off)".format(a_connstr, _APP_B)
    )
    b_connstr = node_b.connstr() + " dbname=postgres"
    node_b.safe_psql("CREATE PUBLICATION tap_pub_B FOR TABLE tab_full, test_tab")
    node_c.safe_psql(
        "CREATE SUBSCRIPTION tap_sub_C CONNECTION "
        "'{} application_name={}' PUBLICATION tap_pub_B "
        "WITH (two_phase = on, streaming = off)".format(b_connstr, _APP_C)
    )

    node_a.wait_for_catchup(_APP_B)
    node_b.wait_for_catchup(_APP_C)
    assert node_b.poll_query_until(_TWOPHASE), "twophase enabled on B"
    assert node_c.poll_query_until(_TWOPHASE), "twophase enabled on C"


def _non_streaming(node_a, node_b, node_c):
    node_a.safe_psql(
        "BEGIN;\nINSERT INTO tab_full VALUES (11);\n"
        "PREPARE TRANSACTION 'test_prepared_tab_full';"
    )
    _cascade_catchup(node_a, node_b)
    _both_prepared(node_b, node_c, "1", "transaction is prepared on subscriber")
    node_a.safe_psql("COMMIT PREPARED 'test_prepared_tab_full';")
    _cascade_catchup(node_a, node_b)
    for node, who in ((node_b, "B"), (node_c, "C")):
        assert node.safe_psql("SELECT count(*) FROM tab_full where a = 11;") == "1", (
            "Row inserted via 2PC has committed on subscriber " + who
        )
    _both_prepared(node_b, node_c, "0", "transaction is committed on subscriber")

    node_a.safe_psql(
        "BEGIN;\nINSERT INTO tab_full VALUES (12);\n"
        "PREPARE TRANSACTION 'test_prepared_tab_full';"
    )
    _cascade_catchup(node_a, node_b)
    _both_prepared(node_b, node_c, "1", "transaction is prepared on subscriber")
    node_a.safe_psql("ROLLBACK PREPARED 'test_prepared_tab_full';")
    _cascade_catchup(node_a, node_b)
    for node, who in ((node_b, "B"), (node_c, "C")):
        assert node.safe_psql("SELECT count(*) FROM tab_full where a = 12;") == "0", (
            "Row inserted via 2PC is not present on subscriber " + who
        )
    _both_prepared(node_b, node_c, "0", "transaction is ended on subscriber")

    # Nested transaction with savepoint rollback.
    node_a.safe_psql(
        "BEGIN;\nINSERT INTO tab_full VALUES (21);\nSAVEPOINT sp_inner;\n"
        "INSERT INTO tab_full VALUES (22);\nROLLBACK TO SAVEPOINT sp_inner;\n"
        "PREPARE TRANSACTION 'outer';"
    )
    _cascade_catchup(node_a, node_b)
    _both_prepared(node_b, node_c, "1", "transaction is prepared on subscriber")
    node_a.safe_psql("COMMIT PREPARED 'outer';")
    _cascade_catchup(node_a, node_b)
    _both_prepared(node_b, node_c, "0", "transaction is ended on subscriber")
    for node, who in ((node_b, "B"), (node_c, "C")):
        assert node.safe_psql("SELECT a FROM tab_full where a IN (21,22);") == "21", (
            "Rows committed are present on subscriber " + who
        )


def _enable_streaming(node_a, node_b, node_c):
    oldpid_b = node_a.safe_psql(
        "SELECT pid FROM pg_stat_replication WHERE application_name = '{}' "
        "AND state = 'streaming';".format(_APP_B)
    )
    oldpid_c = node_b.safe_psql(
        "SELECT pid FROM pg_stat_replication WHERE application_name = '{}' "
        "AND state = 'streaming';".format(_APP_C)
    )
    node_b.safe_psql("ALTER SUBSCRIPTION tap_sub_B SET (streaming = on);")
    node_c.safe_psql("ALTER SUBSCRIPTION tap_sub_C SET (streaming = on)")
    assert node_a.poll_query_until(
        "SELECT pid != {} FROM pg_stat_replication WHERE application_name = '{}' "
        "AND state = 'streaming';".format(oldpid_b, _APP_B)
    ), "apply restarted (B)"
    assert node_b.poll_query_until(
        "SELECT pid != {} FROM pg_stat_replication WHERE application_name = '{}' "
        "AND state = 'streaming';".format(oldpid_c, _APP_C)
    ), "apply restarted (C)"


def _streaming_2pc(node_a, node_b, node_c):
    node_a.safe_psql(
        "BEGIN;\n"
        "INSERT INTO test_tab SELECT i, sha256(i::text::bytea) "
        "FROM generate_series(3, 5000) s(i);\n"
        "UPDATE test_tab SET b = sha256(b) WHERE mod(a,2) = 0;\n"
        "DELETE FROM test_tab WHERE mod(a,3) = 0;\n"
        "PREPARE TRANSACTION 'test_prepared_tab';"
    )
    _cascade_catchup(node_a, node_b)
    _both_prepared(node_b, node_c, "1", "transaction is prepared on subscriber")
    node_a.safe_psql("COMMIT PREPARED 'test_prepared_tab';")
    _cascade_catchup(node_a, node_b)
    for node, who in ((node_b, "B"), (node_c, "C")):
        assert (
            node.safe_psql("SELECT count(*), count(c), count(d = 999) FROM test_tab")
            == "3334|3334|3334"
        ), ("Rows inserted by 2PC committed on subscriber " + who)
    _both_prepared(node_b, node_c, "0", "transaction is committed on subscriber")

    # Streamed 2PC with a nested ROLLBACK TO SAVEPOINT.
    node_a.safe_psql("DELETE FROM test_tab WHERE a > 2;")
    node_a.safe_psql(
        "BEGIN;\nINSERT INTO test_tab VALUES (9999, 'foobar');\nSAVEPOINT sp_inner;\n"
        "INSERT INTO test_tab SELECT i, sha256(i::text::bytea) "
        "FROM generate_series(3, 5000) s(i);\n"
        "UPDATE test_tab SET b = sha256(b) WHERE mod(a,2) = 0;\n"
        "DELETE FROM test_tab WHERE mod(a,3) = 0;\n"
        "ROLLBACK TO SAVEPOINT sp_inner;\n"
        "PREPARE TRANSACTION 'outer';"
    )
    _cascade_catchup(node_a, node_b)
    _both_prepared(node_b, node_c, "1", "transaction is prepared on subscriber")
    node_a.safe_psql("COMMIT PREPARED 'outer';")
    _cascade_catchup(node_a, node_b)
    _both_prepared(node_b, node_c, "0", "transaction is ended on subscriber")
    for node, who in ((node_b, "B"), (node_c, "C")):
        assert (
            node.safe_psql("SELECT count(*) FROM test_tab where b = 'foobar';") == "1"
        ), ("Rows committed are present on subscriber " + who)
        assert node.safe_psql("SELECT count(*) FROM test_tab;") == "3", (
            "Rows committed are present on subscriber " + who
        )


def _cleanup(node_a, node_b, node_c):
    node_c.safe_psql("DROP SUBSCRIPTION tap_sub_C")
    assert node_c.safe_psql("SELECT count(*) FROM pg_subscription") == "0", "C sub"
    assert node_c.safe_psql("SELECT count(*) FROM pg_subscription_rel") == "0", "C rel"
    assert node_c.safe_psql("SELECT count(*) FROM pg_replication_origin") == "0", "C ro"
    assert (
        node_b.safe_psql("SELECT count(*) FROM pg_replication_slots") == "0"
    ), "B slot"

    node_b.safe_psql("DROP SUBSCRIPTION tap_sub_B")
    assert node_b.safe_psql("SELECT count(*) FROM pg_subscription") == "0", "B sub"
    assert node_b.safe_psql("SELECT count(*) FROM pg_subscription_rel") == "0", "B rel"
    assert node_b.safe_psql("SELECT count(*) FROM pg_replication_origin") == "0", "B ro"
    assert (
        node_a.safe_psql("SELECT count(*) FROM pg_replication_slots") == "0"
    ), "A slot"


def test_twophase_cascade(create_pg):
    """Cascading 2PC logical replication, non-streaming and streaming."""
    node_a = create_pg("node_A", allows_streaming="logical", start=False)
    node_b = create_pg("node_B", allows_streaming="logical", start=False)
    node_c = create_pg("node_C", start=False)

    _setup(node_a, node_b, node_c)
    _non_streaming(node_a, node_b, node_c)
    _enable_streaming(node_a, node_b, node_c)
    _streaming_2pc(node_a, node_b, node_c)
    _cleanup(node_a, node_b, node_c)

    node_c.stop("fast")
    node_b.stop("fast")
    node_a.stop("fast")
