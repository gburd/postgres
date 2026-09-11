# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/021_twophase.pl.

Logical replication of two-phase commit (PREPARE/COMMIT PREPARED/ROLLBACK
PREPARED), including crash restarts and ALTER SUBSCRIPTION two_phase changes.
"""

_APP = "tap_sub"
_APP_COPY = "appname_copy"
_TWOPHASE = (
    "SELECT count(1) = 0 FROM pg_subscription WHERE subtwophasestate NOT IN ('e');"
)
_NO_APPLY = (
    "SELECT count(*) = 0 FROM pg_stat_activity "
    "WHERE backend_type = 'logical replication apply worker'"
)


def _prepared(node):
    return node.safe_psql("SELECT count(*) FROM pg_prepared_xacts;")


def _setup(publisher, subscriber):
    publisher.safe_psql("CREATE TABLE tab_full (a int PRIMARY KEY)")
    publisher.safe_psql(
        "BEGIN;\n"
        "INSERT INTO tab_full SELECT generate_series(1,10);\n"
        "PREPARE TRANSACTION 'some_initial_data';\n"
        "COMMIT PREPARED 'some_initial_data';"
    )
    subscriber.safe_psql("CREATE TABLE tab_full (a int PRIMARY KEY)")

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION tap_pub FOR TABLE tab_full")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION "
        "'{} application_name={}' PUBLICATION tap_pub "
        "WITH (two_phase = on)".format(connstr, _APP)
    )
    subscriber.wait_for_subscription_sync(publisher, _APP)
    assert subscriber.poll_query_until(_TWOPHASE), "twophase enabled"
    return connstr


def _commit_then_rollback(publisher, subscriber):
    # max_prepared_transactions = 0 on the subscriber makes apply fail first.
    publisher.safe_psql(
        "BEGIN;\nINSERT INTO tab_full VALUES (11);\n"
        "PREPARE TRANSACTION 'test_prepared_tab_full';"
    )
    subscriber.wait_for_log(r"ERROR: ( [A-Z0-9]+:)? prepared transactions are disabled")
    subscriber.append_conf("max_prepared_transactions = 10")
    subscriber.restart()
    publisher.wait_for_catchup(_APP)
    assert _prepared(subscriber) == "1", "transaction is prepared on subscriber"

    publisher.safe_psql("COMMIT PREPARED 'test_prepared_tab_full';")
    publisher.wait_for_catchup(_APP)
    assert (
        subscriber.safe_psql("SELECT count(*) FROM tab_full where a = 11;") == "1"
    ), "Row inserted via 2PC has committed on subscriber"
    assert _prepared(subscriber) == "0", "transaction is committed on subscriber"

    publisher.safe_psql(
        "BEGIN;\nINSERT INTO tab_full VALUES (12);\n"
        "PREPARE TRANSACTION 'test_prepared_tab_full';"
    )
    publisher.wait_for_catchup(_APP)
    assert _prepared(subscriber) == "1", "transaction is prepared on subscriber"
    publisher.safe_psql("ROLLBACK PREPARED 'test_prepared_tab_full';")
    publisher.wait_for_catchup(_APP)
    assert (
        subscriber.safe_psql("SELECT count(*) FROM tab_full where a = 12;") == "0"
    ), "Row inserted via 2PC is not present on subscriber"
    assert _prepared(subscriber) == "0", "transaction is aborted on subscriber"


def _crash_restart(publisher, subscriber, vals, action, crash_pub, crash_sub):
    publisher.safe_psql(
        "BEGIN;\nINSERT INTO tab_full VALUES ({});\nINSERT INTO tab_full VALUES ({});\n"
        "PREPARE TRANSACTION 'test_prepared_tab';".format(*vals)
    )
    if crash_sub:
        subscriber.stop("immediate")
    if crash_pub:
        publisher.stop("immediate")
    if crash_pub:
        publisher.start()
    if crash_sub:
        subscriber.start()
    publisher.safe_psql("{} PREPARED 'test_prepared_tab';".format(action))
    publisher.wait_for_catchup(_APP)
    expected = "2" if action == "COMMIT" else "0"
    assert (
        subscriber.safe_psql(
            "SELECT count(*) FROM tab_full where a IN ({},{});".format(*vals)
        )
        == expected
    ), "2PC {} decoded across crash restart".format(action)


def _nested_and_empty_gid(publisher, subscriber):
    publisher.safe_psql(
        "BEGIN;\nINSERT INTO tab_full VALUES (21);\nSAVEPOINT sp_inner;\n"
        "INSERT INTO tab_full VALUES (22);\nROLLBACK TO SAVEPOINT sp_inner;\n"
        "PREPARE TRANSACTION 'outer';"
    )
    publisher.wait_for_catchup(_APP)
    assert _prepared(subscriber) == "1", "transaction is prepared on subscriber"
    publisher.safe_psql("COMMIT PREPARED 'outer';")
    publisher.wait_for_catchup(_APP)
    assert _prepared(subscriber) == "0", "transaction is ended on subscriber"
    assert (
        subscriber.safe_psql("SELECT a FROM tab_full where a IN (21,22);") == "21"
    ), "Rows committed are on the subscriber"

    publisher.safe_psql(
        "BEGIN;\nINSERT INTO tab_full VALUES (51);\nPREPARE TRANSACTION '';"
    )
    publisher.wait_for_catchup(_APP)
    assert _prepared(subscriber) == "1", "transaction is prepared on subscriber"
    publisher.safe_psql("ROLLBACK PREPARED '';")
    publisher.wait_for_catchup(_APP)
    assert _prepared(subscriber) == "0", "transaction is aborted on subscriber"


def _copy_data_false(publisher, subscriber, connstr):
    publisher.safe_psql("CREATE TABLE tab_copy (a int PRIMARY KEY)")
    publisher.safe_psql("INSERT INTO tab_copy SELECT generate_series(1,5);")
    subscriber.safe_psql("CREATE TABLE tab_copy (a int PRIMARY KEY)")
    subscriber.safe_psql("INSERT INTO tab_copy VALUES (88);")
    assert subscriber.safe_psql("SELECT count(*) FROM tab_copy;") == "1", "initial data"

    publisher.safe_psql("CREATE PUBLICATION tap_pub_copy FOR TABLE tab_copy;")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub_copy CONNECTION "
        "'{} application_name={}' PUBLICATION tap_pub_copy "
        "WITH (two_phase=on, copy_data=false);".format(connstr, _APP_COPY)
    )
    subscriber.wait_for_subscription_sync(publisher, _APP_COPY)
    assert subscriber.poll_query_until(_TWOPHASE), "twophase enabled"
    assert subscriber.safe_psql("SELECT count(*) FROM tab_copy;") == "1", "no copy_data"

    publisher.safe_psql(
        "BEGIN;\nINSERT INTO tab_copy VALUES (99);\nPREPARE TRANSACTION 'mygid';"
    )
    publisher.wait_for_catchup(_APP_COPY)
    publisher.wait_for_catchup(_APP)
    assert _prepared(subscriber) == "2", "transaction prepared for both subscriptions"
    publisher.safe_psql("COMMIT PREPARED 'mygid';")
    assert (
        publisher.safe_psql("SELECT count(*) FROM tab_copy;") == "6"
    ), "publisher inserted data"
    publisher.wait_for_catchup(_APP_COPY)
    publisher.wait_for_catchup(_APP)
    assert _prepared(subscriber) == "0", "no prepared transactions on subscriber"
    assert subscriber.safe_psql("SELECT count(*) FROM tab_copy;") == "2", "replicated"
    subscriber.safe_psql("DROP SUBSCRIPTION tap_sub")


def _alter_two_phase(publisher, subscriber):
    slot = (
        "SELECT two_phase FROM pg_replication_slots WHERE slot_name = 'tap_sub_copy';"
    )
    assert publisher.safe_psql(slot) == "t", "two-phase is enabled"

    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub_copy DISABLE;")
    subscriber.poll_query_until(_NO_APPLY)
    subscriber.safe_psql(
        "ALTER SUBSCRIPTION tap_sub_copy SET (two_phase = false);\n"
        "ALTER SUBSCRIPTION tap_sub_copy ENABLE;"
    )
    subscriber.wait_for_subscription_sync(publisher, _APP_COPY)
    assert (
        subscriber.safe_psql(
            "SELECT subtwophasestate FROM pg_subscription "
            "WHERE subname = 'tap_sub_copy';"
        )
        == "d"
    ), "two-phase subscription option disabled"
    assert publisher.safe_psql(slot) == "f", "two-phase slot option disabled"

    publisher.safe_psql(
        "BEGIN;\nINSERT INTO tab_copy VALUES (100);\nPREPARE TRANSACTION 'newgid';"
    )
    publisher.wait_for_catchup(_APP_COPY)
    assert _prepared(subscriber) == "0", "no prepared transactions on subscriber"

    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub_copy DISABLE;")
    subscriber.poll_query_until(_NO_APPLY)
    subscriber.safe_psql(
        "ALTER SUBSCRIPTION tap_sub_copy SET (two_phase = true, failover = true);\n"
        "ALTER SUBSCRIPTION tap_sub_copy ENABLE;"
    )
    publisher.safe_psql("COMMIT PREPARED 'newgid';")
    publisher.wait_for_catchup(_APP_COPY)
    assert subscriber.safe_psql("SELECT count(*) FROM tab_copy;") == "3", "replicated"
    assert (
        subscriber.safe_psql(
            "SELECT subtwophasestate FROM pg_subscription "
            "WHERE subname = 'tap_sub_copy';"
        )
        == "e"
    ), "two-phase should be enabled"
    subscriber.safe_psql("DROP SUBSCRIPTION tap_sub_copy;")
    publisher.safe_psql("DROP PUBLICATION tap_pub_copy;")


def test_twophase(create_pg):
    """Two-phase commit logical replication across commit/rollback and crashes."""
    publisher = create_pg("publisher", allows_streaming="logical", start=False)
    publisher.append_conf("max_prepared_transactions = 10")
    publisher.start()
    subscriber = create_pg("subscriber", start=False)
    subscriber.append_conf("max_prepared_transactions = 0")
    subscriber.start()

    connstr = _setup(publisher, subscriber)
    _commit_then_rollback(publisher, subscriber)

    _crash_restart(publisher, subscriber, (12, 13), "ROLLBACK", True, True)
    _crash_restart(publisher, subscriber, (12, 13), "COMMIT", True, True)
    _crash_restart(publisher, subscriber, (14, 15), "COMMIT", False, True)
    _crash_restart(publisher, subscriber, (16, 17), "COMMIT", True, False)

    _nested_and_empty_gid(publisher, subscriber)
    _copy_data_false(publisher, subscriber, connstr)
    _alter_two_phase(publisher, subscriber)

    assert (
        subscriber.safe_psql("SELECT count(*) FROM pg_subscription") == "0"
    ), "subscription dropped on subscriber"
    assert (
        publisher.safe_psql("SELECT count(*) FROM pg_replication_slots") == "0"
    ), "replication slot dropped on publisher"
    assert (
        subscriber.safe_psql("SELECT count(*) FROM pg_subscription_rel") == "0"
    ), "subscription relation status dropped on subscriber"
    assert (
        subscriber.safe_psql("SELECT count(*) FROM pg_replication_origin") == "0"
    ), "replication origin dropped on subscriber"

    subscriber.stop("fast")
    publisher.stop("fast")
