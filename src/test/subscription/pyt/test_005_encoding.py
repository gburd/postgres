# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/005_encoding.pl.

Replication between databases with different encodings.
"""


def test_encoding(create_pg):
    """A UTF8 publisher replicates to a LATIN1 subscriber with re-encoding."""
    publisher = create_pg(
        "publisher", allows_streaming="logical", extra=["--locale=C", "--encoding=UTF8"]
    )
    subscriber = create_pg("subscriber", extra=["--locale=C", "--encoding=LATIN1"])

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

    # Hand-rolled UTF-8 for "Motörhead".
    publisher.safe_psql(r"INSERT INTO test1 VALUES (1, E'Mot\xc3\xb6rhead')")
    publisher.wait_for_catchup("mysub")

    # LATIN1 ö is 0xf6.
    assert (
        subscriber.safe_psql(r"SELECT a FROM test1 WHERE b = E'Mot\xf6rhead'") == "1"
    ), "data replicated to subscriber"

    subscriber.stop()
    publisher.stop()
