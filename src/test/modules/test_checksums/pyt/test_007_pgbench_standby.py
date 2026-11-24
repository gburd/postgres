# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_checksums/t/007_pgbench_standby.pl.

Stress online checksum flipping on a primary with a streaming standby, both
under concurrent pgbench load (read/write on the primary, read-only on the
standby) and random crash/restart cycles. After each flip the standby must move
through inprogress-on to on (and back), all rows read back, and neither node's
log reports a page-verification failure. Gated on PG_TEST_EXTRA checksum/
checksum_extended and an injection-points build.
"""

import os
import random
import re
import subprocess

import pytest

import pypg

import datachecksums_utils as dcu  # pyrefly: ignore

_NO_CSUM_ERR = r"page verification failed,.+\d$"


def _extended():
    return "checksum_extended" in os.environ.get("PG_TEST_EXTRA", "").split()


def _bg_pgbench(node, standby):
    """Start a fire-and-forget pgbench (read-only on a standby); return Popen."""
    extended = _extended()
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
    if standby:
        cmd += ["-S", "-n"]
    cmd.append("postgres")
    return subprocess.Popen(  # pylint: disable=consider-using-with
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _assert_clean(node, location, where):
    log = pypg.slurp_file(node.log, location)
    assert not re.search(
        _NO_CSUM_ERR, log, re.MULTILINE
    ), "no checksum validation errors in {} log{}".format(
        where, " (during WAL recovery)" if where.endswith("rec") else ""
    )


def _flip(primary, standby, state):
    """Flip checksums on the primary and verify standby propagation."""
    dcu.test_checksum_state(primary, state)
    dcu.test_checksum_state(standby, state)
    if state == "off":
        temptablewait = dcu.cointoss()
        if temptablewait:
            primary.safe_psql(
                "SELECT injection_points_attach("
                "'datachecksumsworker-fake-temptable-wait', 'notice');"
            )
        dcu.enable_data_checksums(primary, wait="inprogress-on")
        primary.wait_for_catchup(standby, "replay")
        assert standby.poll_query_until(
            "SELECT setting = 'off' FROM pg_catalog.pg_settings "
            "WHERE name = 'data_checksums';",
            "f",
        ), "ensure standby has absorbed the inprogress-on barrier"
        sstate = standby.safe_psql(
            "SELECT setting FROM pg_catalog.pg_settings "
            "WHERE name = 'data_checksums';"
        )
        assert sstate in ("inprogress-on", "on"), (
            "ensure checksums are on, or in progress, on standby_1, got: " + sstate
        )
        dcu.wait_for_checksum_state(primary, "on")
        dcu.wait_for_checksum_state(standby, "on")
        if temptablewait:
            primary.safe_psql(
                "SELECT injection_points_detach("
                "'datachecksumsworker-fake-temptable-wait');"
            )
        return "on"
    dcu.disable_data_checksums(primary)
    primary.wait_for_catchup(standby, "replay")
    dcu.wait_for_checksum_state(primary, "off")
    dcu.wait_for_checksum_state(standby, "off")
    return "off"


def test_007_pgbench_standby(create_pg):
    """Checksum flips under load + crash/restart stay correct across replication."""
    tokens = os.environ.get("PG_TEST_EXTRA", "").split()
    if "checksum" not in tokens and "checksum_extended" not in tokens:
        pytest.skip("Expensive data checksums test disabled")
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    extended = _extended()
    iterations = 5 if extended else 1
    slotname = "physical_slot"
    state = "off"
    primary = create_pg(
        "pgbench_standby_main",
        allows_streaming=True,
        no_data_checksums=True,
        start=False,
    )
    primary.append_conf(
        "\nmax_connections = 30\nlog_statement = none\nhot_standby_feedback = on\n"
    )
    primary.start()
    primary.safe_psql("CREATE EXTENSION test_checksums;")
    primary.safe_psql("CREATE EXTENSION injection_points;")
    primary.safe_psql("CREATE TABLE t AS SELECT generate_series(1, 100000) AS a;")
    primary.safe_psql(
        "SELECT pg_create_physical_replication_slot('{}');".format(slotname)
    )
    backup_name = "primary_backup"
    primary.backup(backup_name)
    standby = create_pg(
        "pgbench_standby_standby",
        from_backup=(primary, backup_name),
        has_streaming=True,
        start=False,
    )
    standby.append_conf("\nprimary_slot_name = '{}'\n".format(slotname))
    standby.start()
    scalefactor = 10 if extended else 1
    primary.command_ok(
        [
            "pgbench",
            "-p",
            str(primary.port),
            "-i",
            "-s",
            str(scalefactor),
            "-q",
            "postgres",
        ]
    )
    primary.wait_for_catchup(standby, "replay")
    bg = [_bg_pgbench(standby, True), _bg_pgbench(primary, False)]
    try:
        for _ in range(iterations):
            primary.safe_psql("UPDATE t SET a = a + 1;")
            primary.wait_for_catchup(standby, "write")
            state = _flip(primary, standby, state)
            assert (
                primary.safe_psql("SELECT count(*) FROM t WHERE a > 1") == "100000"
            ), "ensure data pages can be read back on primary"
            dcu.random_sleep()
    finally:
        for proc in bg:
            proc.terminate()
            proc.wait()
    if not primary.is_alive():
        primary.start()
    if not standby.is_alive():
        standby.start()
    assert (
        primary.safe_psql("SELECT count(*) FROM t WHERE a > 1") == "100000"
    ), "ensure data pages can be read back on primary"
    dcu.test_checksum_state(primary, state)
    dcu.test_checksum_state(standby, state)
    _assert_clean(primary, 0, "primary")
    _assert_clean(standby, 0, "standby_1")
    standby.teardown_node()
    primary.teardown_node()
