# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_checksums/t/002_restarts.pl.

Online checksum enabling blocked by an open temporary table stays in the
inprogress-on state and does not persist across a restart; once the blocker is
gone, enabling completes and the worker/launcher terminate. The blocked-table
scenario runs only with checksum_extended in PG_TEST_EXTRA (it relies on the
worker's retry timing); the completion path always runs.
"""

import os
import time

import datachecksums_utils as dcu  # pyrefly: ignore


def _checksum_extended():
    extra = os.environ.get("PG_TEST_EXTRA", "")
    return "checksum_extended" in extra.split()


def test_002_restarts(create_pg):
    """Checksum enabling blocks on open temp tables and survives restart off."""
    node = create_pg("restarts_node", no_data_checksums=True)
    node.safe_psql("CREATE TABLE t AS SELECT generate_series(1,10000) AS a;")
    dcu.test_checksum_state(node, "off")
    if _checksum_extended():
        bsession = node.background_psql("postgres")
        bsession.query_safe("CREATE TEMPORARY TABLE tt (a integer);")
        result = node.safe_psql(
            "SELECT relpersistence FROM pg_catalog.pg_class WHERE relname = 'tt';"
        )
        assert result == "t", "ensure we can see the temporary table"
        dcu.enable_data_checksums(node, wait="inprogress-on")
        node.poll_query_until(
            "SELECT wait_event FROM pg_catalog.pg_stat_activity "
            "WHERE backend_type = 'datachecksums worker';",
            "ChecksumEnableTemptableWait",
        )
        time.sleep(4)
        result = node.safe_psql(
            "SELECT wait_event FROM pg_catalog.pg_stat_activity "
            "WHERE backend_type = 'datachecksums worker';"
        )
        assert (
            result == "ChecksumEnableTemptableWait"
        ), "ensure the correct wait condition is set"
        dcu.test_checksum_state(node, "inprogress-on")
        node.stop()
        bsession.quit()
        node.start()
        dcu.test_checksum_state(node, "off")
    dcu.enable_data_checksums(node, wait="on")
    result = node.safe_psql("SELECT count(*) FROM t WHERE a > 1")
    assert result == "9999", "ensure checksummed pages can be read back"
    assert node.poll_query_until(
        "SELECT count(*) FROM pg_stat_activity "
        "WHERE backend_type LIKE 'datachecksums%';",
        "0",
    ), "await datachecksums worker/launcher termination"
    dcu.disable_data_checksums(node, wait=1)
    node.stop()
