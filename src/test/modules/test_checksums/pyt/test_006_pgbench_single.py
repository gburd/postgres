# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_checksums/t/006_pgbench_single.pl.

Stress single-node online checksum flipping under concurrent pgbench load with
random crash/restart cycles, verifying after each that all rows read back and
the server log never reports a page-verification (checksum) failure -- including
during WAL recovery. Gated on PG_TEST_EXTRA checksum/checksum_extended and an
injection-points build.
"""

import os
import random
import re

import pytest

import pypg

import datachecksums_utils as dcu  # pyrefly: ignore

_NO_CSUM_ERR = r"page verification failed,.+\d$"


def _extended():
    return "checksum_extended" in os.environ.get("PG_TEST_EXTRA", "").split()


def _start_bg_pgbench(node):
    """Start a fire-and-forget pgbench load against node; return the Popen."""
    extended = _extended()
    clients = 1 + random.randrange(15) if extended else 1
    runtime = 600 if extended else 2
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
    # Resolve pgbench against the node's bindir and pass its connection env, so
    # the just-built pgbench is found (it is not on the ambient PATH) and a
    # launch failure is not silently swallowed by the discarded stderr.
    return node.bin.popen(cmd)


def _flip_data_checksums(node, state):
    """Flip checksums to the opposite of state (off<->on); return new state."""
    dcu.test_checksum_state(node, state)
    if state == "off":
        temptablewait = dcu.cointoss()
        if temptablewait:
            node.safe_psql(
                "SELECT injection_points_attach("
                "'datachecksumsworker-fake-temptable-wait', 'notice');"
            )
        dcu.enable_data_checksums(node, wait="inprogress-on")
        dcu.wait_for_checksum_state(node, "on")
        if temptablewait:
            node.safe_psql(
                "SELECT injection_points_detach("
                "'datachecksumsworker-fake-temptable-wait');"
            )
        return "on"
    dcu.disable_data_checksums(node)
    dcu.wait_for_checksum_state(node, "off")
    return "off"


def test_006_pgbench_single(create_pg):
    """Online checksum flips survive crash/restart under load, no csum errors."""
    tokens = os.environ.get("PG_TEST_EXTRA", "").split()
    if "checksum" not in tokens and "checksum_extended" not in tokens:
        pytest.skip("Expensive data checksums test disabled")
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    extended = _extended()
    iterations = 10 if extended else 1
    state = "off"
    node = create_pg(
        "pgbench_single_main",
        allows_streaming=True,
        no_data_checksums=True,
        start=False,
    )
    node.append_conf("\nmax_connections = 100\nlog_statement = none\n")
    node.start()
    node.safe_psql("CREATE EXTENSION test_checksums;")
    node.safe_psql("CREATE EXTENSION injection_points;")
    node.safe_psql("CREATE TABLE t AS SELECT generate_series(1, 100000) AS a;")
    scalefactor = 10 if extended else 1
    node.command_ok(
        [
            "pgbench",
            "-p",
            str(node.port),
            "-i",
            "-s",
            str(scalefactor),
            "-q",
            "postgres",
        ]
    )
    loglocation = 0
    pgbench = _start_bg_pgbench(node)
    try:
        for _ in range(iterations):
            if not node.is_alive():
                node.start()
                node.stop("fast")
                log = pypg.slurp_file(node.log, loglocation)
                _assert_no_csum_errors(log, "during WAL recovery")
                loglocation = node.current_log_position()
                node.append_conf(
                    "max_wal_size = {}".format(64 + random.randrange(1024))
                )
                node.start()
                pgbench = _restart_bg(pgbench, node)
            node.safe_psql("UPDATE t SET a = a + 1;")
            state = _flip_data_checksums(node, state)
            result = node.safe_psql("SELECT count(*) FROM t WHERE a > 1")
            assert result == "100000", "ensure data pages can be read back on primary"
            if dcu.cointoss():
                node.stop(dcu.stopmode())
                node.bin.run_command(["pg_controldata", str(node.datadir)])
                log = pypg.slurp_file(node.log, loglocation)
                _assert_no_csum_errors(log, "outside WAL recovery")
                loglocation = node.current_log_position()
    finally:
        pgbench.terminate()
        pgbench.wait()
    if not node.is_alive():
        node.start()
    result = node.safe_psql("SELECT count(*) FROM t WHERE a > 1")
    assert result == "100000", "ensure data pages can be read back on primary"
    dcu.test_checksum_state(node, state)
    log = pypg.slurp_file(node.log, loglocation)
    _assert_no_csum_errors(log, "")
    node.stop()


def _restart_bg(pgbench, node):
    """Stop the previous background pgbench and start a fresh one."""
    pgbench.terminate()
    pgbench.wait()
    return _start_bg_pgbench(node)


def _assert_no_csum_errors(log, where):
    suffix = " ({})".format(where) if where else ""
    assert not re.search(_NO_CSUM_ERR, log, re.MULTILINE), (
        "no checksum validation errors in primary log" + suffix
    )
