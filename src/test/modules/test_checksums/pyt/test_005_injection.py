# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_checksums/t/005_injection.pl.

Uses injection points to exercise the data-checksum worker's error and retry
paths synthetically: a forced per-database failure aborts enabling, and (with
checksum_extended) an injected barrier delay and a faked temporary-table wait
drive the worker's retry loop. Requires an injection-points build.
"""

import os

import pytest

import datachecksums_utils as dcu  # pyrefly: ignore


def _checksum_extended():
    return "checksum_extended" in os.environ.get("PG_TEST_EXTRA", "").split()


def test_005_injection(create_pg):
    """Injection points drive the checksum worker's failure and retry paths."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    node = create_pg("injection_node", no_data_checksums=True)
    node.safe_psql("CREATE EXTENSION test_checksums;")
    node.safe_psql("CREATE EXTENSION injection_points;")
    dcu.disable_data_checksums(node, wait=1)
    node.safe_psql(
        "SELECT injection_points_attach('datachecksumsworker-fail-db-result','notice');"
    )
    dcu.enable_data_checksums(node, wait="off")
    node.safe_psql(
        "SELECT injection_points_detach('datachecksumsworker-fail-db-result');"
    )
    dcu.disable_data_checksums(node)
    dcu.test_checksum_state(node, "off")
    if _checksum_extended():
        dcu.disable_data_checksums(node, wait=1)
        node.safe_psql("SELECT dcw_inject_delay_barrier();")
        dcu.enable_data_checksums(node, wait="on")
        dcu.disable_data_checksums(node, wait=1)
        node.safe_psql(
            "SELECT injection_points_attach("
            "'datachecksumsworker-fake-temptable-wait', 'notice');"
        )
        dcu.enable_data_checksums(node, wait="on")
    node.stop()
