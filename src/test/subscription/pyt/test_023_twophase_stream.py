# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/023_twophase_stream.pl.

Logical replication of 2PC with streaming (streaming=on and parallel), plus
serialize-to-file and parallel-apply retry on insufficient
max_prepared_transactions.
"""

_APP = "tap_sub"
_TWOPHASE = (
    "SELECT count(1) = 0 FROM pg_subscription WHERE subtwophasestate NOT IN ('e');"
)
_PREPARE_BLOCK = (
    "BEGIN;\n"
    "INSERT INTO test_tab SELECT i, sha256(i::text::bytea) "
    "FROM generate_series(3, 5) s(i);\n"
    "UPDATE test_tab SET b = sha256(b) WHERE mod(a,2) = 0;\n"
    "DELETE FROM test_tab WHERE mod(a,3) = 0;\n"
    "PREPARE TRANSACTION 'test_prepared_tab';"
)


def _prepared(node):
    return node.safe_psql("SELECT count(*) FROM pg_prepared_xacts;")


def _agg(node):
    return node.safe_psql("SELECT count(*), count(c), count(d = 999) FROM test_tab")


def _check_parallel(subscriber, offset, is_parallel):
    if is_parallel:
        subscriber.wait_for_log(
            r"DEBUG: ( [A-Z0-9]+:)? finished processing the STREAM PREPARE command",
            offset,
        )


def _test_streaming(publisher, subscriber, is_parallel):
    # 2PC PREPARE / COMMIT PREPARED.
    offset = subscriber.current_log_position()
    publisher.safe_psql(_PREPARE_BLOCK)
    publisher.wait_for_catchup(_APP)
    _check_parallel(subscriber, offset, is_parallel)
    assert _prepared(subscriber) == "1", "transaction is prepared on subscriber"
    publisher.safe_psql("COMMIT PREPARED 'test_prepared_tab';")
    publisher.wait_for_catchup(_APP)
    assert _agg(subscriber) == "4|4|4", "2PC committed; extra columns local defaults"
    assert _prepared(subscriber) == "0", "transaction is committed on subscriber"

    # 2PC PREPARE / ROLLBACK PREPARED.
    publisher.safe_psql("DELETE FROM test_tab WHERE a > 2;")
    offset = subscriber.current_log_position()
    publisher.safe_psql(_PREPARE_BLOCK)
    publisher.wait_for_catchup(_APP)
    _check_parallel(subscriber, offset, is_parallel)
    assert _prepared(subscriber) == "1", "transaction is prepared on subscriber"
    publisher.safe_psql("ROLLBACK PREPARED 'test_prepared_tab';")
    publisher.wait_for_catchup(_APP)
    assert _agg(subscriber) == "2|2|2", "2PC rolled back to the original 2 rows"
    assert _prepared(subscriber) == "0", "transaction is aborted on subscriber"

    # COMMIT PREPARED decoded across a crash restart of both nodes.
    offset = subscriber.current_log_position()
    publisher.safe_psql(_PREPARE_BLOCK)
    subscriber.stop("immediate")
    publisher.stop("immediate")
    publisher.start()
    subscriber.start()
    publisher.safe_psql("COMMIT PREPARED 'test_prepared_tab';")
    publisher.wait_for_catchup(_APP)
    assert _agg(subscriber) == "4|4|4", "2PC committed after crash restart"

    # INSERT after PREPARE, before ROLLBACK PREPARED.
    publisher.safe_psql("DELETE FROM test_tab WHERE a > 2;")
    offset = subscriber.current_log_position()
    publisher.safe_psql(_PREPARE_BLOCK)
    publisher.wait_for_catchup(_APP)
    _check_parallel(subscriber, offset, is_parallel)
    assert _prepared(subscriber) == "1", "transaction is prepared on subscriber"
    publisher.safe_psql("INSERT INTO test_tab VALUES (99999, 'foobar')")
    publisher.safe_psql("ROLLBACK PREPARED 'test_prepared_tab';")
    publisher.wait_for_catchup(_APP)
    assert _agg(subscriber) == "3|3|3", "the outside insert was copied to subscriber"
    assert _prepared(subscriber) == "0", "transaction is aborted on subscriber"

    # INSERT after PREPARE, before COMMIT PREPARED.
    publisher.safe_psql("DELETE FROM test_tab WHERE a > 2;")
    offset = subscriber.current_log_position()
    publisher.safe_psql(_PREPARE_BLOCK)
    publisher.wait_for_catchup(_APP)
    _check_parallel(subscriber, offset, is_parallel)
    assert _prepared(subscriber) == "1", "transaction is prepared on subscriber"
    publisher.safe_psql("INSERT INTO test_tab VALUES (99999, 'foobar')")
    publisher.safe_psql("COMMIT PREPARED 'test_prepared_tab';")
    publisher.wait_for_catchup(_APP)
    assert _agg(subscriber) == "5|5|5", "2PC plus outside insert committed"
    assert _prepared(subscriber) == "0", "transaction is committed on subscriber"

    publisher.safe_psql("DELETE FROM test_tab WHERE a > 2;")
    publisher.wait_for_catchup(_APP)


def _test_serialize_and_retry(publisher, subscriber):
    """Serialize-to-file path and parallel-apply retry on max_prepared=0."""
    subscriber.append_conf("debug_logical_replication_streaming = immediate")
    subscriber.append_conf("log_min_messages = warning")
    subscriber.reload()
    subscriber.safe_psql("SELECT 1")

    offset = subscriber.current_log_position()
    publisher.safe_psql(
        "BEGIN;\nINSERT INTO test_tab_2 values(1);\nPREPARE TRANSACTION 'xact';"
    )
    subscriber.wait_for_log(
        r"LOG: ( [A-Z0-9]+:)? logical replication apply worker will serialize the "
        r"remaining changes of remote transaction \d+ to a file",
        offset,
    )
    publisher.wait_for_catchup(_APP)
    assert _prepared(subscriber) == "1", "transaction is prepared on subscriber"
    publisher.safe_psql("COMMIT PREPARED 'xact';")
    publisher.wait_for_catchup(_APP)
    assert (
        subscriber.safe_psql("SELECT count(*) FROM test_tab_2") == "1"
    ), "transaction is committed on subscriber"

    # Parallel apply worker fails to PREPARE when max_prepared_transactions = 0,
    # then the transaction is re-applied after the setting is fixed.
    subscriber.append_conf(
        "max_prepared_transactions = 0\ndebug_logical_replication_streaming = buffered"
    )
    subscriber.restart()
    publisher.safe_psql(
        "BEGIN;\nINSERT INTO test_tab_2 values(2);\n"
        "PREPARE TRANSACTION 'xact';\nCOMMIT PREPARED 'xact';"
    )
    offset = subscriber.current_log_position()
    subscriber.wait_for_log(
        r"ERROR: ( [A-Z0-9]+:)? prepared transactions are disabled", offset
    )
    subscriber.wait_for_log(
        r"ERROR: .*logical replication parallel apply worker.*", offset
    )
    subscriber.append_conf("max_prepared_transactions = 10")
    subscriber.restart()
    publisher.wait_for_catchup(_APP)
    assert (
        subscriber.safe_psql("SELECT count(*) FROM test_tab_2") == "2"
    ), "transaction is committed on subscriber after retrying"


def test_twophase_stream(create_pg):
    """Streamed 2PC across streaming=on and parallel, serialize, and retry."""
    publisher = create_pg("publisher", allows_streaming="logical", start=False)
    publisher.append_conf(
        "max_prepared_transactions = 10\n"
        "debug_logical_replication_streaming = immediate"
    )
    publisher.start()
    subscriber = create_pg("subscriber", start=False)
    subscriber.append_conf("max_prepared_transactions = 10")
    subscriber.start()

    publisher.safe_psql("CREATE TABLE test_tab (a int primary key, b bytea)")
    publisher.safe_psql("INSERT INTO test_tab VALUES (1, 'foo'), (2, 'bar')")
    publisher.safe_psql("CREATE TABLE test_tab_2 (a int)")
    subscriber.safe_psql(
        "CREATE TABLE test_tab (a int primary key, b bytea, "
        "c timestamptz DEFAULT now(), d bigint DEFAULT 999)"
    )
    subscriber.safe_psql("CREATE TABLE test_tab_2 (a int)")

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION tap_pub FOR TABLE test_tab, test_tab_2")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION "
        "'{} application_name={}' PUBLICATION tap_pub "
        "WITH (streaming = on, two_phase = on)".format(connstr, _APP)
    )
    subscriber.wait_for_subscription_sync(publisher, _APP)
    assert subscriber.poll_query_until(_TWOPHASE), "twophase enabled"
    assert _agg(subscriber) == "2|2|2", "check initial data was copied to subscriber"

    _test_streaming(publisher, subscriber, False)

    # Switch to streaming = parallel.
    oldpid = publisher.safe_psql(
        "SELECT pid FROM pg_stat_replication WHERE application_name = '{}' "
        "AND state = 'streaming';".format(_APP)
    )
    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub SET(streaming = parallel)")
    assert publisher.poll_query_until(
        "SELECT pid != {} FROM pg_stat_replication WHERE application_name = '{}' "
        "AND state = 'streaming';".format(oldpid, _APP)
    ), "apply restarted after changing SUBSCRIPTION"
    subscriber.append_conf("log_min_messages = debug1")
    subscriber.reload()
    subscriber.safe_psql("SELECT 1")

    _test_streaming(publisher, subscriber, True)
    _test_serialize_and_retry(publisher, subscriber)

    subscriber.safe_psql("DROP SUBSCRIPTION tap_sub")
    assert subscriber.safe_psql("SELECT count(*) FROM pg_subscription") == "0", "sub"
    assert (
        publisher.safe_psql("SELECT count(*) FROM pg_replication_slots") == "0"
    ), "slot dropped on publisher"
    assert (
        subscriber.safe_psql("SELECT count(*) FROM pg_subscription_rel") == "0"
    ), "subscription relation status dropped"
    assert (
        subscriber.safe_psql("SELECT count(*) FROM pg_replication_origin") == "0"
    ), "replication origin dropped"

    subscriber.stop("fast")
    publisher.stop("fast")
