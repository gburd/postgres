# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/004_sync.pl.

Logical replication table syncing.
"""

_STARTED_QUERY = "SELECT srsubstate = 'd' FROM pg_subscription_rel;"


def _count(node, table):
    return node.safe_psql("SELECT count(*) FROM {}".format(table))


def test_sync(create_pg):
    """Initial table copy across drop/recreate, refresh, and slot cleanup."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber", start=False)
    subscriber.append_conf("wal_retrieve_retry_interval = 1ms")
    subscriber.start()

    publisher.safe_psql("CREATE TABLE tab_rep (a int primary key)")
    publisher.safe_psql("INSERT INTO tab_rep SELECT generate_series(1,10)")
    subscriber.safe_psql("CREATE TABLE tab_rep (a int primary key)")

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION tap_pub FOR ALL TABLES")

    def create_sub(name="tap_sub", opts=""):
        subscriber.safe_psql(
            "CREATE SUBSCRIPTION {} CONNECTION '{}' PUBLICATION tap_pub{}".format(
                name, connstr, opts
            )
        )

    create_sub()
    subscriber.wait_for_subscription_sync(publisher, "tap_sub")
    assert _count(subscriber, "tab_rep") == "10", "initial data synced for first sub"

    # Drop subscription so that there is unreplicated data.
    subscriber.safe_psql("DROP SUBSCRIPTION tap_sub")
    publisher.safe_psql("INSERT INTO tab_rep SELECT generate_series(11,20)")

    # Recreate: initial copy gets stuck on the unique constraint.
    create_sub()
    assert subscriber.poll_query_until(_STARTED_QUERY), "subscriber started sync"
    subscriber.safe_psql("DELETE FROM tab_rep;")
    subscriber.wait_for_subscription_sync()
    assert _count(subscriber, "tab_rep") == "20", "initial data synced for second sub"

    # Another subscription for the same node pair.
    create_sub("tap_sub2", " WITH (copy_data = false)")
    assert subscriber.poll_query_until(
        "SELECT pid IS NOT NULL FROM pg_stat_subscription "
        "WHERE subname = 'tap_sub2' AND worker_type = 'apply'"
    ), "subscriber started"

    subscriber.safe_psql("DROP SUBSCRIPTION tap_sub")
    subscriber.safe_psql("DROP SUBSCRIPTION tap_sub2")
    assert (
        subscriber.safe_psql("SELECT count(*) FROM pg_subscription") == "0"
    ), "second and third sub are dropped"

    subscriber.safe_psql("DELETE FROM tab_rep;")
    create_sub()
    subscriber.wait_for_subscription_sync()
    assert _count(subscriber, "tab_rep") == "20", "initial data synced for fourth sub"

    # Table added after the subscription was initialized.
    subscriber.safe_psql("CREATE TABLE tab_rep_next (a int)")
    publisher.safe_psql("CREATE TABLE tab_rep_next (a) AS SELECT generate_series(1,10)")
    publisher.wait_for_catchup("tap_sub")
    assert _count(subscriber, "tab_rep_next") == "0", "no data for table added after"

    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub REFRESH PUBLICATION")
    subscriber.wait_for_subscription_sync()
    assert _count(subscriber, "tab_rep_next") == "10", "added table now synced"

    publisher.safe_psql("INSERT INTO tab_rep_next SELECT generate_series(1,10)")
    publisher.wait_for_catchup("tap_sub")
    assert _count(subscriber, "tab_rep_next") == "20", "added table changes replicated"

    # Clean up.
    publisher.safe_psql("DROP TABLE tab_rep_next")
    subscriber.safe_psql("DROP TABLE tab_rep_next")
    subscriber.safe_psql("DROP SUBSCRIPTION tap_sub")

    # Recreate: initial copy fails on the unique constraint (same rows present).
    create_sub()
    assert subscriber.poll_query_until(_STARTED_QUERY), "subscriber started sync"

    # DROP SUBSCRIPTION must clean up tablesync slots on the publisher.
    subscriber.safe_psql("DROP SUBSCRIPTION tap_sub")
    assert publisher.poll_query_until(
        "SELECT count(*) = 0 FROM pg_replication_slots"
    ), "DROP SUBSCRIPTION during error cleans up publisher slots"

    assert (
        subscriber.safe_psql("SELECT count(*) FROM pg_replication_origin_status") == "0"
    ), "all replication origins have been cleaned up"

    subscriber.stop("fast")
    publisher.stop("fast")
