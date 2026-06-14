# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/019_stream_subxact_ddl_abort.pl.

Streaming of a transaction with subtransactions, DDLs, DMLs and rollbacks.
"""


def test_stream_subxact_ddl_abort(create_pg):
    """ROLLBACK TO savepoint inside a streamed txn is reflected on subscriber."""
    publisher = create_pg("publisher", allows_streaming="logical", start=False)
    publisher.append_conf("debug_logical_replication_streaming = immediate")
    publisher.start()
    subscriber = create_pg("subscriber")

    publisher.safe_psql("CREATE TABLE test_tab (a int primary key, b bytea)")
    publisher.safe_psql("INSERT INTO test_tab VALUES (1, 'foo'), (2, 'bar')")
    subscriber.safe_psql(
        "CREATE TABLE test_tab (a int primary key, b bytea, c INT, d INT, e INT)"
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
        subscriber.safe_psql("SELECT count(*), count(c) FROM test_tab") == "2|0"
    ), "check initial data was copied to subscriber"

    # Streamed transaction with DDL, DML and ROLLBACKs.
    publisher.safe_psql(
        "BEGIN;\n"
        "INSERT INTO test_tab VALUES (3, sha256(3::text::bytea));\n"
        "ALTER TABLE test_tab ADD COLUMN c INT;\n"
        "SAVEPOINT s1;\n"
        "INSERT INTO test_tab VALUES (4, sha256(4::text::bytea), -4);\n"
        "ALTER TABLE test_tab ADD COLUMN d INT;\n"
        "SAVEPOINT s2;\n"
        "INSERT INTO test_tab VALUES (5, sha256(5::text::bytea), -5, 5*2);\n"
        "ALTER TABLE test_tab ADD COLUMN e INT;\n"
        "SAVEPOINT s3;\n"
        "INSERT INTO test_tab VALUES (6, sha256(6::text::bytea), -6, 6*2, -6*3);\n"
        "ALTER TABLE test_tab DROP COLUMN c;\n"
        "ROLLBACK TO s1;\n"
        "INSERT INTO test_tab VALUES (4, sha256(4::text::bytea), 4);\n"
        "COMMIT;\n"
    )

    publisher.wait_for_catchup(appname)
    assert (
        subscriber.safe_psql("SELECT count(*), count(c) FROM test_tab") == "4|1"
    ), "rollback to savepoint reflected; extra columns get local defaults"
