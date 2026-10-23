# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of contrib/postgres_fdw/t/010_subscription.pl.

Logical replication into a postgres_fdw foreign table is rejected/handled: changing a subscription parameter restarts the apply worker (verified via the publisher/subscriber log).
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_010_subscription(create_pg):
    """Logical replication into a postgres_fdw foreign table is rejected/handled."""
    node_publisher = create_pg("publisher", allows_streaming="logical", start=False)
    node_publisher.start()
    node_subscriber = create_pg("subscriber", start=False)
    node_subscriber.start()
    node_publisher.safe_psql(
        "CREATE TABLE tab_ins AS SELECT a, a + 1 as b FROM generate_series(1,1002) AS a"
    )
    node_subscriber.safe_psql("CREATE EXTENSION postgres_fdw")
    node_subscriber.safe_psql("CREATE TABLE tab_ins (a int, b int)")
    publisher_connstr = node_publisher.connstr() + " dbname=postgres"
    node_publisher.safe_psql("CREATE PUBLICATION tap_pub FOR TABLE tab_ins")
    publisher_host = node_publisher.host
    publisher_port = node_publisher.port
    node_subscriber.safe_psql(
        "CREATE SERVER tap_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '"
        + str(publisher_host)
        + "', port '"
        + str(publisher_port)
        + "', dbname 'postgres')"
    )
    node_subscriber.safe_psql("CREATE USER MAPPING FOR PUBLIC SERVER tap_server")
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub SERVER tap_server PUBLICATION tap_pub WITH (password_required=false)"
    )
    node_subscriber.wait_for_subscription_sync(node_publisher, "tap_sub")
    result = node_subscriber.safe_psql("SELECT MAX(a) FROM tab_ins")
    assert result == "1002", "check that initial data was copied to subscriber"
    node_publisher.safe_psql(
        "INSERT INTO tab_ins SELECT a, a + 1 FROM generate_series(1003,1050) a"
    )
    node_publisher.wait_for_catchup("tap_sub")
    result = node_subscriber.safe_psql("SELECT MAX(a) FROM tab_ins")
    assert result == "1050", "check that inserted data was copied to subscriber"
    log_offset = node_subscriber.current_log_position()
    node_subscriber.safe_psql(
        "ALTER SUBSCRIPTION tap_sub CONNECTION '" + publisher_connstr + "'"
    )
    node_subscriber.wait_for_log(
        r"""logical replication worker for subscription "tap_sub" will restart because of a parameter change""",
        log_offset,
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_ins SELECT a, a + 1 FROM generate_series(1051,1057) a"
    )
    node_publisher.wait_for_catchup("tap_sub")
    result = node_subscriber.safe_psql("SELECT MAX(a) FROM tab_ins")
    assert (
        result == "1057"
    ), "check subscription after ALTER SUBSCRIPTION ... CONNECTION"
    log_offset = node_subscriber.current_log_position()
    node_subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub SERVER tap_server")
    node_subscriber.wait_for_log(
        r"""logical replication worker for subscription "tap_sub" will restart because of a parameter change""",
        log_offset,
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_ins SELECT a, a + 1 FROM generate_series(1058,1073) a"
    )
    node_publisher.wait_for_catchup("tap_sub")
    result = node_subscriber.safe_psql("SELECT MAX(a) FROM tab_ins")
    assert result == "1073", "check subscription after ALTER SUBSCRIPTION ... SERVER"
