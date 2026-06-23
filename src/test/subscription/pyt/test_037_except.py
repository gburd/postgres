# Copyright (c) 2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/037_except.pl.

Logical replication of publications with an EXCEPT clause.
"""

_BINARY_CHANGES = (
    "SELECT count(*) = 0 FROM pg_logical_slot_get_binary_changes("
    "'test_slot', NULL, NULL, 'proto_version', '1', "
    "'publication_names', '{}')"
)


def _count(node, table):
    return node.safe_psql("SELECT count(*) FROM {}".format(table))


def _test_except_root_partition(publisher, subscriber, connstr, pubviaroot):
    # A root partitioned table in EXCEPT excludes all its partitions,
    # regardless of publish_via_partition_root.
    publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_part FOR ALL TABLES EXCEPT (TABLE root1) "
        "WITH (publish_via_partition_root = {});\n"
        "INSERT INTO root1 VALUES (1), (101);".format(pubviaroot)
    )
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub_part CONNECTION '{}' "
        "PUBLICATION tap_pub_part".format(connstr)
    )
    subscriber.wait_for_subscription_sync(publisher, "tap_sub_part")

    publisher.safe_psql(
        "SELECT slot_name FROM pg_replication_slot_advance("
        "'test_slot', pg_current_wal_lsn())"
    )
    publisher.safe_psql("INSERT INTO root1 VALUES (2), (102)")
    publisher.safe_psql(_BINARY_CHANGES.format("tap_pub_part"))
    publisher.wait_for_catchup("tap_sub_part")

    for table in ("root1", "part1", "part2", "part2_1"):
        assert _count(subscriber, table) == "0", "no rows replicated for " + table

    subscriber.safe_psql("DROP SUBSCRIPTION tap_sub_part")
    publisher.safe_psql("DROP PUBLICATION tap_pub_part")


def _test_multi_publication(publisher, subscriber, connstr, pub2_sql):
    """A table excluded by pub1's EXCEPT is replicated when pub2 includes it."""
    publisher.safe_psql(pub2_sql + "\nINSERT INTO tab1 VALUES(1);")
    subscriber.psql_capture(
        "CREATE SUBSCRIPTION tap_sub CONNECTION '{}' "
        "PUBLICATION tap_pub1, tap_pub2".format(connstr)
    )
    subscriber.wait_for_subscription_sync(publisher, "tap_sub")

    publisher.safe_psql("INSERT INTO tab1 VALUES(2)")
    publisher.wait_for_catchup("tap_sub")
    assert (
        publisher.safe_psql("SELECT * FROM tab1 ORDER BY a") == "1\n2"
    ), "table in one publication's EXCEPT but included by another is replicated"


def test_except(create_pg):
    """Publication EXCEPT clause across plain, inherited, and partitioned tables."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")
    connstr = publisher.connstr() + " dbname=postgres"

    publisher.safe_psql(
        "CREATE TABLE tab1 AS SELECT generate_series(1,10) AS a;\n"
        "CREATE TABLE parent (a int);\n"
        "CREATE TABLE child (b int) INHERITS (parent);\n"
        "CREATE TABLE parent1 (a int);\n"
        "CREATE TABLE child1 (b int) INHERITS (parent1);"
    )
    subscriber.safe_psql(
        "CREATE TABLE tab1 (a int);\n"
        "CREATE TABLE parent (a int);\n"
        "CREATE TABLE child (b int) INHERITS (parent);\n"
        "CREATE TABLE parent1 (a int);\n"
        "CREATE TABLE child1 (b int) INHERITS (parent1);"
    )

    publisher.safe_psql(
        "CREATE PUBLICATION tap_pub FOR ALL TABLES EXCEPT "
        "(TABLE tab1, parent, only parent1)"
    )
    publisher.safe_psql(
        "SELECT pg_create_logical_replication_slot('test_slot', 'pgoutput')"
    )
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION '{}' PUBLICATION tap_pub".format(
            connstr
        )
    )
    subscriber.wait_for_subscription_sync(publisher, "tap_sub")
    assert _count(subscriber, "tab1") == "0", "no initial copy for EXCEPT tables"

    publisher.safe_psql(
        "INSERT INTO tab1 VALUES(generate_series(11,20));\n"
        "INSERT INTO child VALUES(generate_series(11,20), generate_series(11,20));"
    )
    assert (
        publisher.safe_psql(_BINARY_CHANGES.format("tap_pub")) == "t"
    ), "no changes for EXCEPT tables in the replication slot"

    # ONLY parent1 in EXCEPT excludes only the parent, not its child.
    publisher.safe_psql(
        "INSERT INTO child1 VALUES(generate_series(11,20), generate_series(11,20))"
    )
    publisher.wait_for_catchup("tap_sub")
    assert _count(subscriber, "tab1") == "0", "tab1 excluded"
    assert _count(subscriber, "child") == "0", "child excluded via parent"
    assert _count(subscriber, "child1") == "10", "child1 replicated (ONLY parent1)"

    publisher.safe_psql("CREATE TABLE tab2 AS SELECT generate_series(1,10) AS a")
    subscriber.safe_psql("CREATE TABLE tab2 (a int)")
    publisher.safe_psql("ALTER PUBLICATION tap_pub SET ALL TABLES EXCEPT (TABLE tab2)")
    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub REFRESH PUBLICATION")
    subscriber.wait_for_subscription_sync(publisher, "tap_sub")
    assert _count(subscriber, "tab2") == "0", "no initial copy for EXCEPT tab2"
    assert _count(subscriber, "tab1") == "20", "tab1 copied once removed from EXCEPT"

    subscriber.safe_psql(
        "DROP SUBSCRIPTION tap_sub;\nTRUNCATE TABLE tab1;\n"
        "DROP TABLE parent, parent1, child, child1, tab2;"
    )
    publisher.safe_psql(
        "DROP PUBLICATION tap_pub;\nTRUNCATE TABLE tab1;\n"
        "DROP TABLE parent, parent1, child, child1, tab2;"
    )

    # Partitioned tables (publisher) mapping to plain tables (subscriber).
    publisher.safe_psql(
        "CREATE TABLE root1(a int) PARTITION BY RANGE(a);\n"
        "CREATE TABLE part1 PARTITION OF root1 FOR VALUES FROM (0) TO (100);\n"
        "CREATE TABLE part2 PARTITION OF root1 FOR VALUES FROM (100) TO (200) "
        "PARTITION BY RANGE(a);\n"
        "CREATE TABLE part2_1 PARTITION OF part2 FOR VALUES FROM (100) TO (150);"
    )
    subscriber.safe_psql(
        "CREATE TABLE root1(a int);\nCREATE TABLE part1(a int);\n"
        "CREATE TABLE part2(a int);\nCREATE TABLE part2_1(a int);"
    )
    _test_except_root_partition(publisher, subscriber, connstr, "false")
    _test_except_root_partition(publisher, subscriber, connstr, "true")

    # Subscribing to multiple publications.
    publisher.safe_psql(
        "CREATE PUBLICATION tap_pub1 FOR ALL TABLES EXCEPT (TABLE tab1);"
    )
    _test_multi_publication(
        publisher, subscriber, connstr, "CREATE PUBLICATION tap_pub2 FOR TABLE tab1;"
    )
    publisher.safe_psql("DROP PUBLICATION tap_pub2;\nTRUNCATE tab1;")
    subscriber.safe_psql("TRUNCATE tab1")

    _test_multi_publication(
        publisher, subscriber, connstr, "CREATE PUBLICATION tap_pub2 FOR ALL TABLES;"
    )

    subscriber.safe_psql("DROP SUBSCRIPTION tap_sub")
    publisher.safe_psql("DROP PUBLICATION tap_pub1")
    publisher.safe_psql("DROP PUBLICATION tap_pub2")
    publisher.stop("fast")
