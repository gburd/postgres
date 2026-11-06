# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/006_rewrite.pl.

Logical replication behavior with heap rewrites.
"""


def test_rewrite(create_pg):
    """Replication survives DDL that rewrites the heap (ALTER TABLE ADD ...)."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")

    ddl = "CREATE TABLE test1 (a int, b text);"
    publisher.safe_psql(ddl)
    subscriber.safe_psql(ddl)

    publisher_connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION mypub FOR ALL TABLES;")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION mysub CONNECTION '{}' PUBLICATION mypub;".format(
            publisher_connstr
        )
    )

    subscriber.wait_for_subscription_sync(publisher, "mysub")

    publisher.safe_psql("INSERT INTO test1 (a, b) VALUES (1, 'one'), (2, 'two');")
    publisher.wait_for_catchup("mysub")

    assert (
        subscriber.safe_psql("SELECT a, b FROM test1") == "1|one\n2|two"
    ), "initial data replicated to subscriber"

    # DDL that causes a heap rewrite.
    ddl2 = "ALTER TABLE test1 ADD c int NOT NULL DEFAULT 0;"
    subscriber.safe_psql(ddl2)
    publisher.safe_psql(ddl2)
    publisher.wait_for_catchup("mysub")

    publisher.safe_psql("INSERT INTO test1 (a, b, c) VALUES (3, 'three', 33);")
    publisher.wait_for_catchup("mysub")

    assert (
        subscriber.safe_psql("SELECT a, b, c FROM test1")
        == "1|one|0\n2|two|0\n3|three|33"
    ), "data replicated to subscriber"

    subscriber.stop()
    publisher.stop()
