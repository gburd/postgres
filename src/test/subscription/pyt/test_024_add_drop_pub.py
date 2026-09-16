# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/024_add_drop_pub.pl.

ALTER SUBSCRIPTION ... ADD/DROP/SET PUBLICATION, and that creating a missing
publication later does not break logical replication.
"""


def test_add_drop_pub(create_pg):
    """ADD/DROP/SET PUBLICATION and recovery after a missing publication."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")

    publisher.safe_psql("CREATE TABLE tab_1 (a int)")
    publisher.safe_psql("INSERT INTO tab_1 SELECT generate_series(1,10)")
    subscriber.safe_psql("CREATE TABLE tab_1 (a int)")

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION tap_pub_1 FOR TABLE tab_1")
    publisher.safe_psql("CREATE PUBLICATION tap_pub_2")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION '{}' "
        "PUBLICATION tap_pub_1, tap_pub_2".format(connstr)
    )

    subscriber.wait_for_subscription_sync(publisher, "tap_sub")
    assert (
        subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM tab_1") == "10|1|10"
    ), "check initial data is copied to subscriber"

    publisher.safe_psql("CREATE TABLE tab_2 (a int)")
    publisher.safe_psql("INSERT INTO tab_2 SELECT generate_series(1,10)")
    subscriber.safe_psql("CREATE TABLE tab_2 (a int)")
    publisher.safe_psql("ALTER PUBLICATION tap_pub_2 ADD TABLE tab_2")

    # Dropping tap_pub_1 refreshes the entire publication list.
    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub DROP PUBLICATION tap_pub_1")
    subscriber.wait_for_subscription_sync(publisher, "tap_sub")
    assert (
        subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM tab_2") == "10|1|10"
    ), "check initial data is copied to subscriber"

    # Re-adding tap_pub_1 refreshes the entire publication list.
    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub ADD PUBLICATION tap_pub_1")
    subscriber.wait_for_subscription_sync(publisher, "tap_sub")
    assert (
        subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM tab_1") == "20|1|10"
    ), "check initial data is copied to subscriber"

    # A missing publication should log a warning but not disrupt replication.
    publisher.safe_psql("CREATE TABLE tab_3 (a int)")
    subscriber.safe_psql("CREATE TABLE tab_3 (a int)")
    oldpid = publisher.safe_psql(
        "SELECT pid FROM pg_stat_replication "
        "WHERE application_name = 'tap_sub' AND state = 'streaming';"
    )
    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub SET PUBLICATION tap_pub_3")
    assert publisher.poll_query_until(
        "SELECT pid != {} FROM pg_stat_replication "
        "WHERE application_name = 'tap_sub' AND state = 'streaming';".format(oldpid)
    ), "apply worker to restart after altering the subscription"

    offset = publisher.current_log_position()
    publisher.safe_psql("INSERT INTO tab_3 values(1)")
    publisher.wait_for_log(
        r'WARNING: ( [A-Z0-9]+:)? skipped loading publication "tap_pub_3"', offset
    )

    publisher.safe_psql("CREATE PUBLICATION tap_pub_3 FOR TABLE tab_3")
    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub REFRESH  PUBLICATION")
    subscriber.wait_for_subscription_sync(publisher, "tap_sub")

    publisher.safe_psql("INSERT INTO tab_3 values(2)")
    publisher.wait_for_catchup("tap_sub")
    assert (
        subscriber.safe_psql("SELECT * FROM tab_3") == "1\n2"
    ), "incremental data replicated after the publication is created"

    subscriber.stop("fast")
    publisher.stop("fast")
