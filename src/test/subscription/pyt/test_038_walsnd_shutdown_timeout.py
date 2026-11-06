# Copyright (c) 2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/038_walsnd_shutdown_timeout.pl.

Checks that the publisher is able to shut down without waiting for sending of
all pending data to the subscriber when wal_sender_shutdown_timeout is set.
"""

import os
import signal
import time

import pypg

_SHUTDOWN_WARNING = (
    r"WARNING: .* terminating walsender process due to "
    + r"replication shutdown timeout"
)


def _wait_for_full_output_buffer(node):
    """Wait until the logical walsender's send position stops advancing."""
    last_sent_lsn = node.safe_psql(
        "SELECT sent_lsn FROM pg_stat_replication "
        "WHERE application_name = 'test_sub';"
    )
    max_attempts = pypg.test_timeout_default() * 10
    while max_attempts >= 0:
        max_attempts -= 1
        time.sleep(0.1)
        cur_sent_lsn = node.safe_psql(
            "SELECT sent_lsn FROM pg_stat_replication "
            "WHERE application_name = 'test_sub';"
        )
        diff = node.safe_psql(
            "SELECT pg_wal_lsn_diff('{}', '{}');".format(cur_sent_lsn, last_sent_lsn)
        )
        if diff == "0":
            break
        last_sent_lsn = cur_sent_lsn


def _stop_and_check_timeout(node, msg):
    """Fast-stop node and assert the shutdown-timeout warning was logged."""
    log_offset = node.current_log_position()
    node.stop("fast")
    assert node.log_matches(_SHUTDOWN_WARNING, log_offset), msg


def test_038_walsnd_shutdown_timeout(create_pg):
    """Publisher shuts down via wal_sender_shutdown_timeout when stalled."""
    node_publisher = create_pg("publisher", allows_streaming="logical", start=False)
    node_publisher.append_conf(
        "wal_sender_timeout = 1h\nwal_sender_shutdown_timeout = 10ms"
    )
    node_publisher.start()

    node_subscriber = create_pg("subscriber")

    node_publisher.safe_psql(
        "CREATE TABLE test_tab (id int PRIMARY KEY);\n"
        "CREATE PUBLICATION test_pub FOR TABLE test_tab;"
    )

    publisher_connstr = node_publisher.connstr() + " dbname=postgres"
    node_subscriber.safe_psql(
        "CREATE TABLE test_tab (id int PRIMARY KEY);\n"
        "CREATE SUBSCRIPTION test_sub CONNECTION '{}' "
        "PUBLICATION test_pub WITH (failover = true);".format(publisher_connstr)
    )

    node_subscriber.wait_for_subscription_sync(node_publisher, "test_sub")

    # Background session on the subscriber that will block the apply worker.
    sub_session = node_subscriber.background_psql("postgres")

    # Conflicting transactions block the apply worker on a lock, stalling
    # replication; shutting down the publisher must exit the walsender via
    # wal_sender_shutdown_timeout.
    sub_session.query_safe("BEGIN; INSERT INTO test_tab VALUES (0);")
    node_publisher.safe_psql("INSERT INTO test_tab VALUES (0);")

    _stop_and_check_timeout(
        node_publisher, "walsender exits due to wal_sender_shutdown_timeout"
    )

    sub_session.query_safe("ABORT;")
    node_publisher.start()
    node_publisher.wait_for_catchup("test_sub")

    # Same, but with the walsender's output buffer full.
    sub_session.query_safe("BEGIN; LOCK TABLE test_tab IN EXCLUSIVE MODE;")
    node_publisher.safe_psql("INSERT INTO test_tab VALUES (generate_series(1, 20000));")
    _wait_for_full_output_buffer(node_publisher)

    _stop_and_check_timeout(
        node_publisher,
        "walsender with full output buffer exits due to "
        + "wal_sender_shutdown_timeout",
    )

    sub_session.query_safe("ABORT;")
    node_publisher.start()

    # Both physical and logical replication active, with slot sync on the
    # standby; stall both and confirm shutdown still completes via timeout.
    node_publisher.backup(
        "publisher_backup",
        backup_options=[
            "--create-slot",
            "--slot",
            "test_slot",
            "-d",
            "dbname=postgres",
            "--write-recovery-conf",
        ],
    )

    node_publisher.append_conf("synchronized_standby_slots = 'test_slot'")
    node_publisher.reload()

    node_standby = create_pg(
        "standby",
        from_backup=(node_publisher, "publisher_backup"),
        start=False,
    )
    # The backup was taken with --write-recovery-conf, so primary_conninfo and
    # primary_slot_name are already in postgresql.auto.conf; re-place the
    # standby.signal that init_from_backup strips so the standby starts in
    # standby mode and connects a walreceiver.
    node_standby.set_standby_mode()
    node_standby.append_conf("sync_replication_slots = on\nhot_standby_feedback = on")
    node_standby.start()

    node_publisher.wait_for_catchup("test_sub")
    sub_session.query_safe("BEGIN; LOCK TABLE test_tab IN EXCLUSIVE MODE;")
    node_publisher.safe_psql("INSERT INTO test_tab VALUES (-1); ")

    # The remaining scenario stalls physical replication by sending SIGSTOP to
    # the standby's walreceiver, which is not portable to Windows; end the test
    # here on that platform.
    if os.name == "nt":
        sub_session.quit()
        node_subscriber.stop("fast")
        node_standby.stop("fast")
        return

    # Block the standby's walreceiver with SIGSTOP, stalling physical
    # replication.
    assert node_standby.poll_query_until(
        "SELECT EXISTS(SELECT 1 FROM pg_stat_wal_receiver)"
    )
    receiverpid = node_standby.safe_psql("SELECT pid FROM pg_stat_wal_receiver")
    assert receiverpid.isdigit(), "have walreceiver pid {}".format(receiverpid)
    os.kill(int(receiverpid), signal.SIGSTOP)

    log_offset = node_publisher.current_log_position()
    node_publisher.safe_psql("INSERT INTO test_tab VALUES (-2);")
    node_publisher.stop("fast")
    assert node_publisher.log_matches(_SHUTDOWN_WARNING, log_offset), (
        "walsender exits due to wal_sender_shutdown_timeout even when both "
        "physical and logical replication are stalled"
    )

    os.kill(int(receiverpid), signal.SIGCONT)
    sub_session.quit()

    node_subscriber.stop("fast")
    node_standby.stop("fast")
