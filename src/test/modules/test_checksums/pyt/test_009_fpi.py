# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_checksums/t/009_fpi.pl.

Checksum enabling remains correct across full_page_writes toggling and restarts:
after several enable/disable cycles and updates with FPWs off then on, all rows
read back and the server log shows no page-verification (checksum) errors.
"""

import re

import pypg

import datachecksums_utils as dcu  # pyrefly: ignore


def test_009_fpi(create_pg):
    """No checksum validation errors across full_page_writes toggling."""
    node = create_pg("fpi_node", allows_streaming=True, no_data_checksums=True)
    node.append_conf("\nmax_connections = 100\nlog_statement = none\n")
    node.safe_psql("CREATE EXTENSION test_checksums;")
    node.safe_psql("CREATE TABLE t AS SELECT generate_series(1, 1000000) AS a;")
    dcu.enable_data_checksums(node, wait="on")
    node.safe_psql("UPDATE t SET a = a + 1;")
    dcu.disable_data_checksums(node, wait=1)
    node.append_conf("full_page_writes = off")
    node.restart()
    dcu.test_checksum_state(node, "off")
    node.safe_psql("UPDATE t SET a = a + 1;")
    node.safe_psql("DELETE FROM t WHERE a < 10000;")
    node.adjust_conf("full_page_writes", "on")
    node.restart()
    dcu.test_checksum_state(node, "off")
    dcu.enable_data_checksums(node, wait="on")
    result = node.safe_psql("SELECT count(*) FROM t;")
    assert result == "990003", "Reading back all data from table t"
    node.stop()
    log = pypg.slurp_file(node.log, 0)
    assert not re.search(
        r"page verification failed,.+\d$", log, re.MULTILINE
    ), "no checksum validation errors in server log"
