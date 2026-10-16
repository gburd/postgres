# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/026_stats.pl.

Subscription statistics (errors, conflicts, and resets).
"""

_DB = "postgres"
_ALL_NONZERO = (
    "SELECT apply_error_count > 0, sync_seq_error_count > 0, "
    "sync_table_error_count > 0, confl_insert_exists > 0, "
    "confl_delete_missing > 0, stats_reset IS NULL "
    "FROM pg_stat_subscription_stats WHERE subname = '{}'"
)
_ALL_ZERO = (
    "SELECT apply_error_count = 0, sync_seq_error_count = 0, "
    "sync_table_error_count = 0, confl_insert_exists = 0, "
    "confl_delete_missing = 0, stats_reset IS NOT NULL "
    "FROM pg_stat_subscription_stats WHERE subname = '{}'"
)
_SIX_T = "t|t|t|t|t|t"


def _create_sub_pub_w_errors(publisher, subscriber, table_name, sequence_name):
    """Set up a sub/pub that hits sync, sequencesync, apply and conflict errors."""
    publisher.safe_psql(
        "BEGIN;\n"
        "CREATE TABLE {0}(a int);\n"
        "ALTER TABLE {0} REPLICA IDENTITY FULL;\n"
        "INSERT INTO {0} VALUES (1);\n"
        "CREATE SEQUENCE {1};\n"
        "COMMIT;".format(table_name, sequence_name),
        dbname=_DB,
    )
    subscriber.safe_psql(
        "BEGIN;\n"
        "CREATE TABLE {0}(a int primary key);\n"
        "INSERT INTO {0} VALUES (1);\n"
        "CREATE SEQUENCE {1} INCREMENT BY 10;\n"
        "COMMIT;".format(table_name, sequence_name),
        dbname=_DB,
    )

    pub_name = table_name + "_pub"
    pub_seq_name = sequence_name + "_pub"
    connstr = publisher.connstr() + " dbname={}".format(_DB)
    publisher.safe_psql(
        "CREATE PUBLICATION {} FOR TABLE {};\n"
        "CREATE PUBLICATION {} FOR ALL SEQUENCES;".format(
            pub_name, table_name, pub_seq_name
        ),
        dbname=_DB,
    )
    sub_name = table_name + "_sub"
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION {} CONNECTION '{}' PUBLICATION {}, {}".format(
            sub_name, connstr, pub_name, pub_seq_name
        ),
        dbname=_DB,
    )
    publisher.wait_for_catchup(sub_name)

    assert subscriber.poll_query_until(
        "SELECT count(1) = 1 FROM pg_stat_subscription_stats WHERE subname = '{}' "
        "AND sync_seq_error_count > 0 AND sync_table_error_count > 0".format(sub_name),
        dbname=_DB,
    ), "sequencesync and tablesync errors reported"

    subscriber.safe_psql(
        "ALTER SEQUENCE {} INCREMENT 1".format(sequence_name), dbname=_DB
    )
    assert subscriber.poll_query_until(
        "SELECT count(1) = 1 FROM pg_subscription_rel "
        "WHERE srrelid = '{}'::regclass AND srsubstate = 'r'".format(sequence_name),
        dbname=_DB,
    ), "sequencesync finished"

    subscriber.safe_psql("TRUNCATE {}".format(table_name), dbname=_DB)
    assert subscriber.poll_query_until(
        "SELECT count(1) = 1 FROM pg_subscription_rel "
        "WHERE srrelid = '{}'::regclass AND srsubstate in ('r', 's')".format(
            table_name
        ),
        dbname=_DB,
    ), "tablesync finished"
    assert (
        subscriber.safe_psql("SELECT a FROM {}".format(table_name), dbname=_DB) == "1"
    ), "table now has 1 row"

    publisher.safe_psql("INSERT INTO {} VALUES (1)".format(table_name), dbname=_DB)
    assert subscriber.poll_query_until(
        "SELECT apply_error_count > 0 AND confl_insert_exists > 0 "
        "FROM pg_stat_subscription_stats WHERE subname = '{}'".format(sub_name),
        dbname=_DB,
    ), "apply error and insert_exists conflict reported"

    subscriber.safe_psql("TRUNCATE {}".format(table_name), dbname=_DB)
    publisher.safe_psql("DELETE FROM {};".format(table_name), dbname=_DB)
    assert subscriber.poll_query_until(
        "SELECT confl_delete_missing > 0 FROM pg_stat_subscription_stats "
        "WHERE subname = '{}'".format(sub_name),
        dbname=_DB,
    ), "delete_missing conflict reported"
    return pub_name, sub_name


def _reset_one(subscriber, sub_name):
    subscriber.safe_psql(
        "SELECT pg_stat_reset_subscription_stats((SELECT subid FROM "
        "pg_stat_subscription_stats WHERE subname = '{}'))".format(sub_name),
        dbname=_DB,
    )


def _reset_time(subscriber, sub_name):
    return subscriber.safe_psql(
        "SELECT stats_reset FROM pg_stat_subscription_stats "
        "WHERE subname = '{}'".format(sub_name),
        dbname=_DB,
    )


def test_stats(create_pg):
    """Subscription stat counters, conflicts, per-sub and global resets."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")

    assert (
        subscriber.safe_psql(
            "SELECT count(1) FROM pg_stat_subscription_stats", dbname=_DB
        )
        == "0"
    ), "no subscription errors before logical replication"

    _, sub1 = _create_sub_pub_w_errors(publisher, subscriber, "test_tab1", "test_seq1")
    assert (
        subscriber.safe_psql(_ALL_NONZERO.format(sub1), dbname=_DB) == _SIX_T
    ), "errors/conflicts > 0 and stats_reset NULL for {}".format(sub1)

    _reset_one(subscriber, sub1)
    assert (
        subscriber.safe_psql(_ALL_ZERO.format(sub1), dbname=_DB) == _SIX_T
    ), "errors/conflicts 0 and stats_reset not NULL after reset for {}".format(sub1)

    reset_time1 = _reset_time(subscriber, sub1)
    _reset_one(subscriber, sub1)
    assert (
        subscriber.safe_psql(
            "SELECT stats_reset > '{}'::timestamptz FROM pg_stat_subscription_stats "
            "WHERE subname = '{}'".format(reset_time1, sub1),
            dbname=_DB,
        )
        == "t"
    ), "reset timestamp newer after second reset for {}".format(sub1)

    _, sub2 = _create_sub_pub_w_errors(publisher, subscriber, "test_tab2", "test_seq2")
    assert (
        subscriber.safe_psql(_ALL_NONZERO.format(sub2), dbname=_DB) == _SIX_T
    ), "errors/conflicts > 0 and stats_reset NULL for {}".format(sub2)

    # Reset all subscriptions.
    subscriber.safe_psql("SELECT pg_stat_reset_subscription_stats(NULL)", dbname=_DB)
    assert (
        subscriber.safe_psql(_ALL_ZERO.format(sub1), dbname=_DB) == _SIX_T
    ), "errors/conflicts 0 after global reset for {}".format(sub1)
    assert (
        subscriber.safe_psql(_ALL_ZERO.format(sub2), dbname=_DB) == _SIX_T
    ), "errors/conflicts 0 after global reset for {}".format(sub2)

    reset_time1 = _reset_time(subscriber, sub1)
    reset_time2 = _reset_time(subscriber, sub2)
    subscriber.safe_psql("SELECT pg_stat_reset_subscription_stats(NULL)", dbname=_DB)
    for sub, when in ((sub1, reset_time1), (sub2, reset_time2)):
        assert (
            subscriber.safe_psql(
                "SELECT stats_reset > '{}'::timestamptz FROM "
                "pg_stat_subscription_stats WHERE subname = '{}'".format(when, sub),
                dbname=_DB,
            )
            == "t"
        ), "reset timestamp newer after second global reset for {}".format(sub)

    sub1_oid = subscriber.safe_psql(
        "SELECT oid FROM pg_subscription WHERE subname = '{}'".format(sub1), dbname=_DB
    )
    subscriber.safe_psql("DROP SUBSCRIPTION {}".format(sub1), dbname=_DB)
    assert (
        subscriber.safe_psql(
            "SELECT pg_stat_have_stats('subscription', 0, {})".format(sub1_oid),
            dbname=_DB,
        )
        == "f"
    ), "subscription stats for {} removed".format(sub1)

    sub2_oid = subscriber.safe_psql(
        "SELECT oid FROM pg_subscription WHERE subname = '{}'".format(sub2), dbname=_DB
    )
    subscriber.safe_psql(
        "ALTER SUBSCRIPTION {0} DISABLE;\n"
        "ALTER SUBSCRIPTION {0} SET (slot_name = NONE);\n"
        "DROP SUBSCRIPTION {0};".format(sub2),
        dbname=_DB,
    )
    assert (
        subscriber.safe_psql(
            "SELECT pg_stat_have_stats('subscription', 0, {})".format(sub2_oid),
            dbname=_DB,
        )
        == "f"
    ), "subscription stats for {} removed".format(sub2)

    assert publisher.poll_query_until(
        "SELECT EXISTS (SELECT 1 FROM pg_replication_slots "
        "WHERE slot_name = '{}' AND active_pid IS NULL)".format(sub2),
        dbname=_DB,
    ), "slot became inactive"
    publisher.safe_psql(
        "SELECT pg_drop_replication_slot('{}')".format(sub2), dbname=_DB
    )

    subscriber.stop("fast")
    publisher.stop("fast")
