# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/018_stream_subxact_abort.pl.

Streaming of transactions containing multiple subtransactions and rollbacks,
in both streaming=on and streaming=parallel modes, plus serialize-to-file.
"""

_SERIALIZE_LOG = (
    r"LOG: ( [A-Z0-9]+:)? logical replication apply worker will serialize the "
    r"remaining changes of remote transaction \d+ to a file"
)


def _count(node):
    return node.safe_psql("SELECT count(*), count(c) FROM test_tab")


def _wait_parallel(subscriber, offset, is_parallel, type_):
    if is_parallel:
        subscriber.wait_for_log(
            r"DEBUG: ( [A-Z0-9]+:)? finished processing the STREAM {} "
            r"command".format(type_),
            offset,
        )


def _test_streaming(publisher, subscriber, appname, is_parallel):
    offset = subscriber.current_log_position()
    publisher.safe_psql(
        "BEGIN;\n"
        "INSERT INTO test_tab VALUES (3, sha256(3::text::bytea));\n"
        "SAVEPOINT s1;\n"
        "INSERT INTO test_tab VALUES (4, sha256(4::text::bytea));\n"
        "SAVEPOINT s2;\n"
        "INSERT INTO test_tab VALUES (5, sha256(5::text::bytea));\n"
        "SAVEPOINT s3;\n"
        "INSERT INTO test_tab VALUES (6, sha256(6::text::bytea));\n"
        "ROLLBACK TO s2;\n"
        "INSERT INTO test_tab VALUES (7, sha256(7::text::bytea));\n"
        "ROLLBACK TO s1;\n"
        "INSERT INTO test_tab VALUES (8, sha256(8::text::bytea));\n"
        "SAVEPOINT s4;\n"
        "INSERT INTO test_tab VALUES (9, sha256(9::text::bytea));\n"
        "SAVEPOINT s5;\n"
        "INSERT INTO test_tab VALUES (10, sha256(10::text::bytea));\n"
        "COMMIT;\n"
    )
    publisher.wait_for_catchup(appname)
    _wait_parallel(subscriber, offset, is_parallel, "COMMIT")
    assert (
        _count(subscriber) == "6|0"
    ), "rollback to savepoint reflected; local defaults"

    offset = subscriber.current_log_position()
    publisher.safe_psql(
        "BEGIN;\n"
        "INSERT INTO test_tab VALUES (11, sha256(11::text::bytea));\n"
        "SAVEPOINT s1;\n"
        "INSERT INTO test_tab VALUES (12, sha256(12::text::bytea));\n"
        "SAVEPOINT s2;\n"
        "INSERT INTO test_tab VALUES (13, sha256(13::text::bytea));\n"
        "SAVEPOINT s3;\n"
        "INSERT INTO test_tab VALUES (14, sha256(14::text::bytea));\n"
        "RELEASE s2;\n"
        "INSERT INTO test_tab VALUES (15, sha256(15::text::bytea));\n"
        "ROLLBACK TO s1;\n"
        "COMMIT;\n"
    )
    publisher.wait_for_catchup(appname)
    _wait_parallel(subscriber, offset, is_parallel, "COMMIT")
    assert _count(subscriber) == "7|0", "rollback to savepoint reflected"

    offset = subscriber.current_log_position()
    publisher.safe_psql(
        "BEGIN;\n"
        "INSERT INTO test_tab VALUES (16, sha256(16::text::bytea));\n"
        "SAVEPOINT s1;\n"
        "INSERT INTO test_tab VALUES (17, sha256(17::text::bytea));\n"
        "SAVEPOINT s2;\n"
        "INSERT INTO test_tab VALUES (18, sha256(18::text::bytea));\n"
        "ROLLBACK;\n"
    )
    publisher.wait_for_catchup(appname)
    _wait_parallel(subscriber, offset, is_parallel, "ABORT")
    assert _count(subscriber) == "7|0", "rollback was reflected on subscriber"

    publisher.safe_psql("DELETE FROM test_tab WHERE (a > 2)")
    publisher.wait_for_catchup(appname)


def _test_serialize(publisher, subscriber, appname):
    """Serialize changes to a file and apply them at end-of-transaction."""
    subscriber.append_conf("debug_logical_replication_streaming = immediate")
    subscriber.append_conf("log_min_messages = warning")
    subscriber.reload()
    subscriber.safe_psql("SELECT 1")

    offset = subscriber.current_log_position()
    publisher.safe_psql("BEGIN;\nINSERT INTO test_tab_2 values(1);\nROLLBACK;\n")
    subscriber.wait_for_log(_SERIALIZE_LOG, offset)
    publisher.wait_for_catchup(appname)
    assert (
        subscriber.safe_psql("SELECT count(*) FROM test_tab_2") == "0"
    ), "rollback was reflected on subscriber"

    offset = subscriber.current_log_position()
    publisher.safe_psql(
        "BEGIN;\n"
        "INSERT INTO test_tab_2 values(1);\n"
        "SAVEPOINT sp;\n"
        "INSERT INTO test_tab_2 values(1);\n"
        "ROLLBACK TO sp;\n"
        "COMMIT;\n"
    )
    subscriber.wait_for_log(_SERIALIZE_LOG, offset)
    publisher.wait_for_catchup(appname)
    assert (
        subscriber.safe_psql("SELECT count(*) FROM test_tab_2") == "1"
    ), "rollback to savepoint was reflected on subscriber"


def test_stream_subxact_abort(create_pg):
    """Subxact rollbacks stream correctly in on/parallel modes and via files."""
    publisher = create_pg("publisher", allows_streaming="logical", start=False)
    publisher.append_conf("debug_logical_replication_streaming = immediate")
    publisher.start()
    subscriber = create_pg("subscriber")

    publisher.safe_psql("CREATE TABLE test_tab (a int primary key, b bytea)")
    publisher.safe_psql("INSERT INTO test_tab VALUES (1, 'foo'), (2, 'bar')")
    publisher.safe_psql("CREATE TABLE test_tab_2 (a int)")
    subscriber.safe_psql(
        "CREATE TABLE test_tab (a int primary key, b text, c INT, d INT, e INT)"
    )
    subscriber.safe_psql("CREATE TABLE test_tab_2 (a int)")

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION tap_pub FOR TABLE test_tab, test_tab_2")
    appname = "tap_sub"

    # Streaming mode 'on'.
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION "
        "'{} application_name={}' PUBLICATION tap_pub "
        "WITH (streaming = on)".format(connstr, appname)
    )
    subscriber.wait_for_subscription_sync(publisher, appname)
    assert _count(subscriber) == "2|0", "check initial data was copied to subscriber"

    _test_streaming(publisher, subscriber, appname, False)

    # Streaming mode 'parallel'.
    oldpid = publisher.safe_psql(
        "SELECT pid FROM pg_stat_replication "
        "WHERE application_name = '{}' AND state = 'streaming';".format(appname)
    )
    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub SET(streaming = parallel)")
    assert publisher.poll_query_until(
        "SELECT pid != {} FROM pg_stat_replication "
        "WHERE application_name = '{}' AND state = 'streaming';".format(oldpid, appname)
    ), "apply restarted after changing SUBSCRIPTION"

    subscriber.append_conf("log_min_messages = debug1")
    subscriber.reload()
    subscriber.safe_psql("SELECT 1")

    _test_streaming(publisher, subscriber, appname, True)
    _test_serialize(publisher, subscriber, appname)
