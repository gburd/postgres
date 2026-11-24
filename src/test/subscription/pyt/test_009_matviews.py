# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/009_matviews.pl.

Materialized views are not supported by logical replication, but logical
decoding does produce change information for them; make sure they are ignored.
"""


def test_matviews(create_pg):
    """A materialized view on the publisher is not replicated and doesn't hang."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")

    publisher.safe_psql("CREATE TABLE test1 (a int PRIMARY KEY, b text)")
    subscriber.safe_psql("CREATE TABLE test1 (a int PRIMARY KEY, b text);")

    publisher_connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION mypub FOR ALL TABLES;")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION mysub CONNECTION '{}' PUBLICATION mypub;".format(
            publisher_connstr
        )
    )

    publisher.safe_psql("INSERT INTO test1 (a, b) VALUES (1, 'one'), (2, 'two');")
    publisher.wait_for_catchup("mysub")

    # Create an MV with some data; its data must not be replicated.
    publisher.safe_psql("CREATE MATERIALIZED VIEW testmv1 AS SELECT * FROM test1;")
    publisher.wait_for_catchup("mysub")

    # There is no equivalent relation on the subscriber, but MV data is not
    # replicated, so this does not hang. (bug #15044)

    subscriber.stop()
    publisher.stop()
