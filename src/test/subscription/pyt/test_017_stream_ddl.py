# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/017_stream_ddl.pl.

Streaming of large transactions with DDL and subtransactions.
"""


def test_stream_ddl(create_pg):
    """Streamed and non-streamed txns with interleaved DDL replicate correctly."""
    publisher = create_pg("publisher", allows_streaming="logical", start=False)
    publisher.append_conf("logical_decoding_work_mem = 64kB")
    publisher.start()
    subscriber = create_pg("subscriber")

    publisher.safe_psql("CREATE TABLE test_tab (a int primary key, b varchar)")
    publisher.safe_psql("INSERT INTO test_tab VALUES (1, 'foo'), (2, 'bar')")
    subscriber.safe_psql(
        "CREATE TABLE test_tab "
        "(a int primary key, b bytea, c INT, d INT, e INT, f INT)"
    )

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION tap_pub FOR TABLE test_tab")
    appname = "tap_sub"
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION "
        "'{} application_name={}' PUBLICATION tap_pub "
        "WITH (streaming = on)".format(connstr, appname)
    )

    subscriber.wait_for_subscription_sync(publisher, appname)
    assert (
        subscriber.safe_psql("SELECT count(*), count(c), count(d = 999) FROM test_tab")
        == "2|0|0"
    ), "check initial data was copied to subscriber"

    # Small (non-streamed) txn with DDL and DML.
    publisher.safe_psql(
        "BEGIN;\n"
        "INSERT INTO test_tab VALUES (3, sha256(3::text::bytea));\n"
        "ALTER TABLE test_tab ADD COLUMN c INT;\n"
        "SAVEPOINT s1;\n"
        "INSERT INTO test_tab VALUES (4, sha256(4::text::bytea), -4);\n"
        "COMMIT;\n"
    )
    # Large (streamed) txn with DDL and DML.
    publisher.safe_psql(
        "BEGIN;\n"
        "INSERT INTO test_tab SELECT i, sha256(i::text::bytea), -i "
        "FROM generate_series(5, 1000) s(i);\n"
        "ALTER TABLE test_tab ADD COLUMN d INT;\n"
        "SAVEPOINT s1;\n"
        "INSERT INTO test_tab SELECT i, sha256(i::text::bytea), -i, 2*i "
        "FROM generate_series(1001, 2000) s(i);\n"
        "COMMIT;\n"
    )
    # Small (non-streamed) txn with DDL and DML.
    publisher.safe_psql(
        "BEGIN;\n"
        "INSERT INTO test_tab VALUES (2001, sha256(2001::text::bytea), -2001, "
        "2*2001);\n"
        "ALTER TABLE test_tab ADD COLUMN e INT;\n"
        "SAVEPOINT s1;\n"
        "INSERT INTO test_tab VALUES (2002, sha256(2002::text::bytea), -2002, "
        "2*2002, -3*2002);\n"
        "COMMIT;\n"
    )

    publisher.wait_for_catchup(appname)
    assert (
        subscriber.safe_psql(
            "SELECT count(*), count(c), count(d), count(e) FROM test_tab"
        )
        == "2002|1999|1002|1"
    ), "streaming mode copied; extra columns get local defaults"

    # Large (streamed) txn with a DDL after DML, invalidating the sent schema.
    publisher.safe_psql(
        "BEGIN;\n"
        "INSERT INTO test_tab SELECT i, sha256(i::text::bytea), -i, 2*i, -3*i "
        "FROM generate_series(2003,5000) s(i);\n"
        "ALTER TABLE test_tab ADD COLUMN f INT;\n"
        "COMMIT;\n"
    )
    # Small txn to force the schema to be sent again with the new column.
    publisher.safe_psql(
        "BEGIN;\n"
        "INSERT INTO test_tab SELECT i, sha256(i::text::bytea), -i, 2*i, -3*i, 4*i "
        "FROM generate_series(5001,5005) s(i);\n"
        "COMMIT;\n"
    )

    publisher.wait_for_catchup(appname)
    assert (
        subscriber.safe_psql(
            "SELECT count(*), count(c), count(d), count(e), count(f) FROM test_tab"
        )
        == "5005|5002|4005|3004|5"
    ), "data copied for both streaming and non-streaming transactions"
