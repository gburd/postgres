# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/010_truncate.pl."""


def _count(node, table, col="a"):
    return node.safe_psql(
        "SELECT count(*), min({0}), max({0}) FROM {1}".format(col, table)
    )


def test_truncate(create_pg):
    """TRUNCATE replication across publications, FKs, sync rep and multi-sub."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber", start=False)
    subscriber.append_conf("max_logical_replication_workers = 6")
    subscriber.start()

    connstr = publisher.connstr() + " dbname=postgres"

    for tab in ("tab1", "tab2", "tab3"):
        publisher.safe_psql("CREATE TABLE {} (a int PRIMARY KEY)".format(tab))
        subscriber.safe_psql("CREATE TABLE {} (a int PRIMARY KEY)".format(tab))
    publisher.safe_psql("CREATE TABLE tab4 (x int PRIMARY KEY, y int REFERENCES tab3)")
    subscriber.safe_psql("CREATE TABLE tab4 (x int PRIMARY KEY, y int REFERENCES tab3)")

    subscriber.safe_psql("CREATE SEQUENCE seq1 OWNED BY tab1.a")
    subscriber.safe_psql("ALTER SEQUENCE seq1 START 101")

    publisher.safe_psql("CREATE PUBLICATION pub1 FOR TABLE tab1")
    publisher.safe_psql(
        "CREATE PUBLICATION pub2 FOR TABLE tab2 WITH (publish = insert)"
    )
    publisher.safe_psql("CREATE PUBLICATION pub3 FOR TABLE tab3, tab4")
    for sub, pub in (("sub1", "pub1"), ("sub2", "pub2"), ("sub3", "pub3")):
        subscriber.safe_psql(
            "CREATE SUBSCRIPTION {} CONNECTION '{}' PUBLICATION {}".format(
                sub, connstr, pub
            )
        )

    subscriber.wait_for_subscription_sync()

    subscriber.safe_psql("INSERT INTO tab1 VALUES (1), (2), (3)")
    publisher.wait_for_catchup("sub1")

    publisher.safe_psql("TRUNCATE tab1")
    publisher.wait_for_catchup("sub1")
    assert _count(subscriber, "tab1") == "0||", "truncate replicated"
    assert (
        subscriber.safe_psql("SELECT nextval('seq1')") == "1"
    ), "sequence not restarted"

    publisher.safe_psql("TRUNCATE tab1 RESTART IDENTITY")
    publisher.wait_for_catchup("sub1")
    assert (
        subscriber.safe_psql("SELECT nextval('seq1')") == "101"
    ), "truncate restarted identities"

    # Publication that does not replicate truncate.
    subscriber.safe_psql("INSERT INTO tab2 VALUES (1), (2), (3)")
    publisher.safe_psql("TRUNCATE tab2")
    publisher.wait_for_catchup("sub2")
    assert _count(subscriber, "tab2") == "3|1|3", "truncate not replicated"

    publisher.safe_psql("ALTER PUBLICATION pub2 SET (publish = 'insert, truncate')")
    publisher.safe_psql("TRUNCATE tab2")
    publisher.wait_for_catchup("sub2")
    assert _count(subscriber, "tab2") == "0||", "truncate replicated after pub change"

    # Multiple tables connected by foreign keys.
    subscriber.safe_psql("INSERT INTO tab3 VALUES (1), (2), (3)")
    subscriber.safe_psql("INSERT INTO tab4 VALUES (11, 1), (111, 1), (22, 2)")
    publisher.safe_psql("TRUNCATE tab3, tab4")
    publisher.wait_for_catchup("sub3")
    assert _count(subscriber, "tab3") == "0||", "truncate of multiple tables replicated"
    assert _count(subscriber, "tab4", "x") == "0||", "truncate of multiple tables"

    # Truncate of multiple tables, some not published.
    subscriber.safe_psql("DROP SUBSCRIPTION sub2")
    publisher.safe_psql("DROP PUBLICATION pub2")
    subscriber.safe_psql("INSERT INTO tab1 VALUES (1), (2), (3)")
    subscriber.safe_psql("INSERT INTO tab2 VALUES (1), (2), (3)")
    publisher.safe_psql("TRUNCATE tab1, tab2")
    publisher.wait_for_catchup("sub1")
    assert _count(subscriber, "tab1") == "0||", "truncate, some not published"
    assert _count(subscriber, "tab2") == "3|1|3", "truncate, some not published"

    # Synchronous logical replication.
    _test_sync_rep(publisher, subscriber)

    # Multiple subscriptions for a single table.
    _test_multi_sub(publisher, subscriber, connstr)

    assert (
        subscriber.safe_psql(
            "SELECT deadlocks FROM pg_stat_database WHERE datname='postgres'"
        )
        == "0"
    ), "no deadlocks detected"


def _test_sync_rep(publisher, subscriber):
    publisher.safe_psql("ALTER SYSTEM SET synchronous_standby_names TO 'sub1'")
    publisher.safe_psql("SELECT pg_reload_conf()")
    publisher.safe_psql("INSERT INTO tab1 VALUES (1), (2), (3)")
    publisher.wait_for_catchup("sub1")
    assert (
        _count(subscriber, "tab1") == "3|1|3"
    ), "check synchronous logical replication"
    publisher.safe_psql("TRUNCATE tab1")
    publisher.wait_for_catchup("sub1")
    assert _count(subscriber, "tab1") == "0||", "truncate in synchronous logical rep"
    publisher.safe_psql("ALTER SYSTEM RESET synchronous_standby_names")
    publisher.safe_psql("SELECT pg_reload_conf()")


def _test_multi_sub(publisher, subscriber, connstr):
    publisher.safe_psql("CREATE TABLE tab5 (a int)")
    subscriber.safe_psql("CREATE TABLE tab5 (a int)")
    publisher.safe_psql("CREATE PUBLICATION pub5 FOR TABLE tab5")
    for sub in ("sub5_1", "sub5_2"):
        subscriber.safe_psql(
            "CREATE SUBSCRIPTION {} CONNECTION '{}' PUBLICATION pub5".format(
                sub, connstr
            )
        )
    subscriber.wait_for_subscription_sync()

    publisher.safe_psql("INSERT INTO tab5 VALUES (1), (2), (3)")
    publisher.wait_for_catchup("sub5_1")
    publisher.wait_for_catchup("sub5_2")
    assert _count(subscriber, "tab5") == "6|1|3", "insert replicated for multiple subs"

    publisher.safe_psql("TRUNCATE tab5")
    publisher.wait_for_catchup("sub5_1")
    publisher.wait_for_catchup("sub5_2")
    assert _count(subscriber, "tab5") == "0||", "truncate replicated for multiple subs"
