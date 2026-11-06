# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_checksums/t/008_pitr.pl.

Point-in-time recovery across a checksum flip: with concurrent pgbench write
load, checksums are enabled on the primary and the exact LSN of the transition
is captured. A PITR replica restored to that LSN must come up with checksums in
the expected state and no page-verification errors. Gated on PG_TEST_EXTRA
containing checksum or checksum_extended (expensive).
"""

import os
import random
import re
import subprocess

import pytest

import pypg

import datachecksums_utils as dcu  # pyrefly: ignore


def _extra_tokens():
    return os.environ.get("PG_TEST_EXTRA", "").split()


def _flip_data_checksums(node, state):
    """Flip checksums on/off, returning (lsn_before, lsn_after) and new state."""
    dcu.test_checksum_state(node, state)
    lsn_pre = node.safe_psql("SELECT pg_current_wal_lsn()")
    if state == "off":
        dcu.enable_data_checksums(node, wait="on")
        new_state = "on"
    else:
        dcu.disable_data_checksums(node, wait=1)
        new_state = "off"
    lsn_post = node.safe_psql("SELECT pg_current_wal_lsn()")
    return lsn_pre, lsn_post, new_state


def _start_bg_pgbench(node, extended):
    """Start a fire-and-forget read/write pgbench load against node."""
    clients = 1 + random.randrange(15) if extended else 1
    runtime = 600 if extended else 5
    cmd = [
        "pgbench",
        "-h",
        str(node.host),
        "-p",
        str(node.port),
        "-T",
        str(runtime),
        "-c",
        str(clients),
    ]
    if extended and dcu.cointoss():
        cmd.append("-C")
    cmd.append("postgres")
    return subprocess.Popen(  # pylint: disable=consider-using-with
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def test_008_pitr(create_pg):
    """PITR to a checksum-flip LSN restores the expected checksum state."""
    tokens = _extra_tokens()
    if "checksum" not in tokens and "checksum_extended" not in tokens:
        pytest.skip("Expensive data checksums test disabled")
    extended = "checksum_extended" in tokens
    node_primary = create_pg(
        "pitr_main",
        has_archiving=True,
        allows_streaming=True,
        no_data_checksums=True,
        start=False,
    )
    timeout = pypg.test_timeout_default()
    node_primary.append_conf(
        "\nmax_connections = 100\nlog_statement = none\n"
        "wal_sender_timeout = {t}s\nwal_receiver_timeout = {t}s\n".format(t=timeout)
    )
    node_primary.start()
    node_primary.safe_psql("CREATE TABLE t AS SELECT generate_series(1, 100000) AS a;")
    scalefactor = 10 if extended else 1
    node_primary.command_ok(
        [
            "pgbench",
            "-p",
            str(node_primary.port),
            "-i",
            "-s",
            str(scalefactor),
            "-q",
            "postgres",
        ]
    )
    pgbench = _start_bg_pgbench(node_primary, extended)
    try:
        backup_name = "my_backup"
        node_primary.backup(backup_name)
        _pre_lsn, post_lsn, state = _flip_data_checksums(node_primary, "off")
        node_primary.safe_psql("UPDATE t SET a = a + 1;")
        node_primary.safe_psql("SELECT pg_create_restore_point('a');")
        node_primary.safe_psql("UPDATE t SET a = a + 1;")
        node_primary.stop("fast")
    finally:
        pgbench.terminate()
        pgbench.wait()
    node_pitr = create_pg(
        "pitr_backup",
        from_backup=(node_primary, backup_name),
        standby=False,
        has_restoring=True,
        start=False,
    )
    node_pitr.append_conf(
        "\nrecovery_target_lsn = '{}'\n"
        "recovery_target_action = 'promote'\n"
        "recovery_target_inclusive = on\n".format(post_lsn)
    )
    node_pitr.start()
    assert node_pitr.poll_query_until(
        "SELECT pg_is_in_recovery() = 'f';"
    ), "Timed out while waiting for PITR promotion"
    dcu.test_checksum_state(node_pitr, state)
    result = node_pitr.safe_psql("SELECT count(*) FROM t WHERE a > 1")
    assert result == "99999", "ensure data pages can be read back on primary"
    node_pitr.stop()
    log = pypg.slurp_file(node_pitr.log, 0)
    assert not re.search(
        r"page verification failed,.+\d$", log, re.MULTILINE
    ), "no checksum validation errors in pitr log"
