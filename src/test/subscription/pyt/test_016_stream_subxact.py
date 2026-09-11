# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/016_stream_subxact.pl.

Streaming of a transaction containing subtransactions, in both streaming=on
and streaming=parallel modes.
"""

_WORKLOAD = """\
BEGIN;
INSERT INTO test_tab SELECT i, sha256(i::text::bytea) FROM generate_series(3, 5) s(i);
UPDATE test_tab SET b = sha256(b) WHERE mod(a,2) = 0;
DELETE FROM test_tab WHERE mod(a,3) = 0;
SAVEPOINT s1;
INSERT INTO test_tab SELECT i, sha256(i::text::bytea) FROM generate_series(6, 8) s(i);
UPDATE test_tab SET b = sha256(b) WHERE mod(a,2) = 0;
DELETE FROM test_tab WHERE mod(a,3) = 0;
SAVEPOINT s2;
INSERT INTO test_tab SELECT i, sha256(i::text::bytea) FROM generate_series(9, 11) s(i);
UPDATE test_tab SET b = sha256(b) WHERE mod(a,2) = 0;
DELETE FROM test_tab WHERE mod(a,3) = 0;
SAVEPOINT s3;
INSERT INTO test_tab SELECT i, sha256(i::text::bytea) FROM generate_series(12, 14) s(i);
UPDATE test_tab SET b = sha256(b) WHERE mod(a,2) = 0;
DELETE FROM test_tab WHERE mod(a,3) = 0;
SAVEPOINT s4;
INSERT INTO test_tab SELECT i, sha256(i::text::bytea) FROM generate_series(15, 17) s(i);
UPDATE test_tab SET b = sha256(b) WHERE mod(a,2) = 0;
DELETE FROM test_tab WHERE mod(a,3) = 0;
COMMIT;
"""


def _test_streaming(publisher, subscriber, appname, is_parallel):
    offset = subscriber.current_log_position()

    publisher.safe_psql(_WORKLOAD)
    publisher.wait_for_catchup(appname)

    if is_parallel:
        subscriber.wait_for_log(
            r"DEBUG: ( [A-Z0-9]+:)? finished processing the STREAM COMMIT command",
            offset,
        )

    assert (
        subscriber.safe_psql("SELECT count(*), count(c), count(d = 999) FROM test_tab")
        == "12|12|12"
    ), "streaming mode copied data; extra columns get local defaults"

    publisher.safe_psql("DELETE FROM test_tab WHERE (a > 2)")
    publisher.wait_for_catchup(appname)


def test_stream_subxact(create_pg):
    """Subtransactions stream correctly in both on and parallel modes."""
    publisher = create_pg("publisher", allows_streaming="logical", start=False)
    publisher.append_conf("debug_logical_replication_streaming = immediate")
    publisher.start()
    subscriber = create_pg("subscriber")

    publisher.safe_psql("CREATE TABLE test_tab (a int primary key, b bytea)")
    publisher.safe_psql("INSERT INTO test_tab VALUES (1, 'foo'), (2, 'bar')")
    subscriber.safe_psql(
        "CREATE TABLE test_tab (a int primary key, b bytea, "
        "c timestamptz DEFAULT now(), d bigint DEFAULT 999)"
    )

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION tap_pub FOR TABLE test_tab")
    appname = "tap_sub"

    # Streaming mode 'on'.
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION "
        "'{} application_name={}' PUBLICATION tap_pub "
        "WITH (streaming = on)".format(connstr, appname)
    )
    subscriber.wait_for_subscription_sync(publisher, appname)
    assert (
        subscriber.safe_psql("SELECT count(*), count(c), count(d = 999) FROM test_tab")
        == "2|2|2"
    ), "check initial data was copied to subscriber"

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

    # Bump log verbosity so the parallel apply worker's DEBUG lines appear.
    subscriber.append_conf("log_min_messages = debug1")
    subscriber.reload()
    subscriber.safe_psql("SELECT 1")

    _test_streaming(publisher, subscriber, appname, True)
