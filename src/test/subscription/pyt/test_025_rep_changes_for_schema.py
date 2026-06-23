# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/025_rep_changes_for_schema.pl.

Logical replication with FOR TABLES IN SCHEMA publications.
"""


def _rel_count(subscriber):
    return subscriber.safe_psql(
        "SELECT count(*) FROM pg_subscription_rel WHERE srsubid IN "
        "(SELECT oid FROM pg_subscription WHERE subname = 'tap_sub_schema')"
    )


def test_rep_changes_for_schema(create_pg):
    """FOR TABLES IN SCHEMA: initial sync, refresh, schema moves, drops."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")
    connstr = publisher.connstr() + " dbname=postgres"

    publisher.safe_psql("CREATE SCHEMA sch1")
    publisher.safe_psql("CREATE TABLE sch1.tab1 AS SELECT generate_series(1,10) AS a")
    publisher.safe_psql("CREATE TABLE sch1.tab2 AS SELECT generate_series(1,10) AS a")
    publisher.safe_psql(
        "CREATE TABLE sch1.tab1_parent (a int PRIMARY KEY, b text) "
        "PARTITION BY LIST (a)"
    )
    publisher.safe_psql(
        "CREATE TABLE public.tab1_child1 PARTITION OF sch1.tab1_parent "
        "FOR VALUES IN (1, 2, 3)"
    )
    publisher.safe_psql(
        "CREATE TABLE public.tab1_child2 PARTITION OF sch1.tab1_parent "
        "FOR VALUES IN (4, 5, 6)"
    )
    publisher.safe_psql("INSERT INTO sch1.tab1_parent values (1),(4)")

    subscriber.safe_psql("CREATE SCHEMA sch1")
    subscriber.safe_psql("CREATE TABLE sch1.tab1 (a int)")
    subscriber.safe_psql("CREATE TABLE sch1.tab2 (a int)")
    subscriber.safe_psql(
        "CREATE TABLE sch1.tab1_parent (a int PRIMARY KEY, b text) "
        "PARTITION BY LIST (a)"
    )
    subscriber.safe_psql(
        "CREATE TABLE public.tab1_child1 PARTITION OF sch1.tab1_parent "
        "FOR VALUES IN (1, 2, 3)"
    )
    subscriber.safe_psql(
        "CREATE TABLE public.tab1_child2 PARTITION OF sch1.tab1_parent "
        "FOR VALUES IN (4, 5, 6)"
    )

    publisher.safe_psql("CREATE PUBLICATION tap_pub_schema FOR TABLES IN SCHEMA sch1")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub_schema CONNECTION '{}' "
        "PUBLICATION tap_pub_schema".format(connstr)
    )
    subscriber.wait_for_subscription_sync(publisher, "tap_sub_schema")

    assert (
        subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM sch1.tab1")
        == "10|1|10"
    ), "check rows on subscriber catchup"
    assert (
        subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM sch1.tab2")
        == "10|1|10"
    ), "check rows on subscriber catchup"
    assert (
        subscriber.safe_psql("SELECT * FROM sch1.tab1_parent order by 1") == "1|\n4|"
    ), "check rows on subscriber catchup"

    publisher.safe_psql("INSERT INTO sch1.tab1 VALUES(generate_series(11,20))")
    publisher.safe_psql("INSERT INTO sch1.tab1_parent values (2),(5)")
    publisher.wait_for_catchup("tap_sub_schema")
    assert (
        subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM sch1.tab1")
        == "20|1|20"
    ), "check replicated inserts on subscriber"
    assert (
        subscriber.safe_psql("SELECT * FROM sch1.tab1_parent order by 1")
        == "1|\n2|\n4|\n5|"
    ), "check replicated inserts on subscriber"

    # A new table in the schema is not synced until the publication is refreshed.
    publisher.safe_psql("CREATE TABLE sch1.tab3 AS SELECT generate_series(1,10) AS a")
    subscriber.safe_psql("CREATE TABLE sch1.tab3(a int)")
    publisher.wait_for_catchup("tap_sub_schema")
    assert subscriber.safe_psql("SELECT count(*) FROM sch1.tab3") == "0"

    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub_schema REFRESH PUBLICATION")
    subscriber.wait_for_subscription_sync()
    publisher.safe_psql("INSERT INTO sch1.tab3 VALUES(11)")
    publisher.wait_for_catchup("tap_sub_schema")
    assert (
        subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM sch1.tab3")
        == "11|1|11"
    ), "check rows on subscriber catchup"

    # Moving a table out of the schema stops its replication.
    publisher.safe_psql("ALTER TABLE sch1.tab3 SET SCHEMA public")
    publisher.safe_psql("INSERT INTO public.tab3 VALUES(12)")
    publisher.wait_for_catchup("tap_sub_schema")
    assert (
        subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM sch1.tab3")
        == "11|1|11"
    ), "check replicated inserts on subscriber"

    assert _rel_count(subscriber) == "5", "relation status not yet dropped"
    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub_schema REFRESH PUBLICATION")
    subscriber.wait_for_subscription_sync()
    assert _rel_count(subscriber) == "4", "relation status was dropped"

    # Dropping a table removes it from pg_subscription_rel after refresh.
    publisher.safe_psql("DROP TABLE sch1.tab2")
    publisher.wait_for_catchup("tap_sub_schema")
    assert _rel_count(subscriber) == "4", "relation status not yet dropped"
    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub_schema REFRESH PUBLICATION")
    subscriber.wait_for_subscription_sync()
    assert _rel_count(subscriber) == "3", "relation status was dropped"

    # Dropping the schema from the publication stops publishing (2nd insert).
    publisher.safe_psql(
        "INSERT INTO sch1.tab1 VALUES(21);\n"
        "ALTER PUBLICATION tap_pub_schema DROP TABLES IN SCHEMA sch1;\n"
        "INSERT INTO sch1.tab1 values(22);"
    )
    publisher.wait_for_catchup("tap_sub_schema")
    assert (
        subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM sch1.tab1")
        == "21|1|21"
    ), "check replicated inserts on subscriber"

    subscriber.stop("fast")
    publisher.stop("fast")
