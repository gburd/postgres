# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/015_stream.pl.

Streaming of large (>64kB) logical-replication transactions in both
streaming=on and streaming=parallel modes, including binary mode, retention of
locally-changed extra columns, deadlock detection among the leader and parallel
apply workers, and serialization of streamed changes to a file.
"""

_PARALLEL_FINISHED = r"DEBUG: ( [A-Z0-9]+:)? finished processing the STREAM {} command"
_APPLIED_CHUNK = r"DEBUG: ( [A-Z0-9]+:)? applied [0-9]+ changes in the streaming chunk"
_DEADLOCK = r"ERROR: ( [A-Z0-9]+:)? deadlock detected"
_SERIALIZE = (
    r"LOG: ( [A-Z0-9]+:)? logical replication apply worker will serialize the "
    r"remaining changes of remote transaction \d+ to a file"
)


def _check_parallel_log(subscriber, offset, is_parallel, kind):
    """Wait for the parallel apply worker to finish a STREAM <kind> command."""
    if is_parallel:
        subscriber.wait_for_log(_PARALLEL_FINISHED.format(kind), offset)


def _test_streaming(publisher, subscriber, appname, is_parallel):
    """Common streaming checks for both streaming=on and streaming=parallel."""
    handle = publisher.background_psql("postgres", on_error_stop=False)
    offset = subscriber.current_log_position()
    handle.query_safe(
        "BEGIN;\n"
        "INSERT INTO test_tab SELECT i, sha256(i::text::bytea) "
        "FROM generate_series(3, 5000) s(i);\n"
        "UPDATE test_tab SET b = sha256(b) WHERE mod(a,2) = 0;\n"
        "DELETE FROM test_tab WHERE mod(a,3) = 0;"
    )
    publisher.safe_psql(
        "BEGIN;\n"
        "INSERT INTO test_tab SELECT i, sha256(i::text::bytea) "
        "FROM generate_series(5001, 9999) s(i);\n"
        "DELETE FROM test_tab WHERE a > 5000;\n"
        "COMMIT;"
    )
    handle.query_safe("COMMIT")
    handle.quit()

    publisher.wait_for_catchup(appname)
    _check_parallel_log(subscriber, offset, is_parallel, "COMMIT")
    assert (
        subscriber.safe_psql("SELECT count(*), count(c), count(d = 999) FROM test_tab")
        == "3334|3334|3334"
    ), "check extra columns contain local defaults"

    # Test streaming in binary mode.
    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub SET (binary = on)")
    offset = subscriber.current_log_position()
    publisher.safe_psql(
        "BEGIN;\n"
        "INSERT INTO test_tab SELECT i, sha256(i::text::bytea) "
        "FROM generate_series(5001, 10000) s(i);\n"
        "UPDATE test_tab SET b = sha256(b) WHERE mod(a,2) = 0;\n"
        "DELETE FROM test_tab WHERE mod(a,3) = 0;\n"
        "COMMIT;"
    )
    publisher.wait_for_catchup(appname)
    _check_parallel_log(subscriber, offset, is_parallel, "COMMIT")
    assert (
        subscriber.safe_psql("SELECT count(*), count(c), count(d = 999) FROM test_tab")
        == "6667|6667|6667"
    ), "check extra columns contain local defaults"

    # Locally changed extra columns must be retained after a streaming txn.
    subscriber.safe_psql(
        "UPDATE test_tab SET c = 'epoch'::timestamptz + 987654321 * interval '1s'"
    )
    offset = subscriber.current_log_position()
    publisher.safe_psql("UPDATE test_tab SET b = sha256(a::text::bytea)")
    publisher.wait_for_catchup(appname)
    _check_parallel_log(subscriber, offset, is_parallel, "COMMIT")
    assert (
        subscriber.safe_psql(
            "SELECT count(*), count(extract(epoch from c) = 987654321), "
            "count(d = 999) FROM test_tab"
        )
        == "6667|6667|6667"
    ), "check extra columns contain locally changed data"

    publisher.safe_psql("DELETE FROM test_tab WHERE (a > 2)")
    publisher.wait_for_catchup(appname)


def _setup(create_pg):
    """Create publisher/subscriber, tables, publication and subscription."""
    publisher = create_pg("publisher", allows_streaming="logical", start=False)
    publisher.append_conf("logical_decoding_work_mem = 64kB")
    publisher.start()
    subscriber = create_pg("subscriber")

    publisher.safe_psql("CREATE TABLE test_tab (a int primary key, b bytea)")
    publisher.safe_psql("INSERT INTO test_tab VALUES (1, 'foo'), (2, 'bar')")
    publisher.safe_psql("CREATE TABLE test_tab_2 (a int)")

    subscriber.safe_psql(
        "CREATE TABLE test_tab (a int primary key, b bytea, "
        "c timestamptz DEFAULT now(), d bigint DEFAULT 999)"
    )
    subscriber.safe_psql("CREATE TABLE test_tab_2 (a int)")
    subscriber.safe_psql("CREATE UNIQUE INDEX idx_tab on test_tab_2(a)")

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION tap_pub FOR TABLE test_tab, test_tab_2")
    appname = "tap_sub"
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION "
        "'{} application_name={}' PUBLICATION tap_pub "
        "WITH (streaming = on)".format(connstr, appname)
    )
    subscriber.wait_for_subscription_sync(publisher, appname)
    return publisher, subscriber, appname


def _switch_to_parallel(publisher, subscriber, appname):
    """Switch the subscription to streaming=parallel and bump log verbosity."""
    oldpid = publisher.safe_psql(
        "SELECT pid FROM pg_stat_replication "
        "WHERE application_name = '{}' AND state = 'streaming';".format(appname)
    )
    subscriber.safe_psql(
        "ALTER SUBSCRIPTION tap_sub SET(streaming = parallel, binary = off)"
    )
    assert publisher.poll_query_until(
        "SELECT pid != {} FROM pg_stat_replication "
        "WHERE application_name = '{}' AND state = 'streaming';".format(oldpid, appname)
    ), "Timed out while waiting for apply to restart after changing SUBSCRIPTION"
    subscriber.append_conf("log_min_messages = debug1")
    subscriber.reload()
    subscriber.safe_psql("SELECT 1")


def _deadlock_round(publisher, subscriber, appname, conflicting_insert, expected):
    """Drive one deadlock-detection round and verify the post-recovery count."""
    offset = subscriber.current_log_position()
    handle = publisher.background_psql("postgres", on_error_stop=False)
    handle.query_safe(
        "BEGIN;\nINSERT INTO test_tab_2 SELECT i FROM generate_series(1, 5000) s(i);"
    )
    subscriber.wait_for_log(_APPLIED_CHUNK, offset)
    publisher.safe_psql(conflicting_insert)
    handle.query_safe("COMMIT")
    handle.quit()
    subscriber.wait_for_log(_DEADLOCK, offset)

    # Drop the unique index so both transactions can complete without conflict.
    subscriber.safe_psql("DROP INDEX idx_tab")
    publisher.wait_for_catchup(appname)
    assert (
        subscriber.safe_psql("SELECT count(*) FROM test_tab_2") == expected
    ), "data replicated to subscriber after dropping index"


def _test_deadlocks(publisher, subscriber, appname):
    """Detect deadlocks between leader/parallel workers and between two parallel."""
    subscriber.append_conf("deadlock_timeout = 10ms")
    subscriber.reload()
    subscriber.safe_psql("SELECT 1")

    _deadlock_round(
        publisher, subscriber, appname, "INSERT INTO test_tab_2 values(1)", "5001"
    )

    publisher.safe_psql("TRUNCATE TABLE test_tab_2")
    publisher.wait_for_catchup(appname)
    subscriber.safe_psql("CREATE UNIQUE INDEX idx_tab on test_tab_2(a)")

    _deadlock_round(
        publisher,
        subscriber,
        appname,
        "INSERT INTO test_tab_2 SELECT i FROM generate_series(1, 5000) s(i)",
        "10000",
    )


def _test_serialize_to_file(publisher, subscriber, appname):
    """Serialize streamed changes to a file and apply at transaction end."""
    subscriber.append_conf("debug_logical_replication_streaming = immediate")
    subscriber.append_conf("log_min_messages = warning")
    subscriber.reload()
    subscriber.safe_psql("SELECT 1")

    offset = subscriber.current_log_position()
    publisher.safe_psql(
        "INSERT INTO test_tab_2 SELECT i FROM generate_series(1, 5000) s(i)"
    )
    subscriber.wait_for_log(_SERIALIZE, offset)
    publisher.wait_for_catchup(appname)
    assert (
        subscriber.safe_psql("SELECT count(*) FROM test_tab_2") == "15000"
    ), "parallel apply worker replayed all changes from file"


def test_015_stream(create_pg):
    """Large transactions stream in on/parallel modes with deadlock handling."""
    publisher, subscriber, appname = _setup(create_pg)

    assert (
        subscriber.safe_psql("SELECT count(*), count(c), count(d = 999) FROM test_tab")
        == "2|2|2"
    ), "check initial data was copied to subscriber"

    _test_streaming(publisher, subscriber, appname, False)

    _switch_to_parallel(publisher, subscriber, appname)
    _test_streaming(publisher, subscriber, appname, True)

    _test_deadlocks(publisher, subscriber, appname)
    _test_serialize_to_file(publisher, subscriber, appname)

    subscriber.stop()
    publisher.stop()
