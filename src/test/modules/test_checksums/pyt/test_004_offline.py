# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_checksums/t/004_offline.pl.

Offline checksum enable/disable via pg_checksums interoperates with the online
state machine: pages are read back correctly after offline enabling, and an
online enable left in the inprogress-on state can be completed offline.
"""

import datachecksums_utils as dcu  # pyrefly: ignore


def test_004_offline(create_pg):
    """Offline pg_checksums enable/disable round-trips with online state."""
    node = create_pg("offline_node", no_data_checksums=True)
    node.safe_psql("CREATE TABLE t AS SELECT generate_series(1,10000) AS a;")
    dcu.test_checksum_state(node, "off")
    node.stop()
    node.checksum_enable_offline()
    node.start()
    dcu.test_checksum_state(node, "on")
    result = node.safe_psql("SELECT count(*) FROM t WHERE a > 1")
    assert result == "9999", "ensure checksummed pages can be read back"
    node.stop()
    node.checksum_disable_offline()
    node.start()
    dcu.test_checksum_state(node, "off")
    bsession = node.background_psql("postgres")
    bsession.query_safe("CREATE TEMPORARY TABLE tt (a integer);")
    result = node.safe_psql(
        "SELECT relpersistence FROM pg_catalog.pg_class WHERE relname = 'tt';"
    )
    assert result == "t", "ensure we can see the temporary table"
    dcu.enable_data_checksums(node, wait="inprogress-on")
    node.stop("fast")
    bsession.quit()
    node.checksum_enable_offline()
    node.start()
    dcu.test_checksum_state(node, "on")
    result = node.safe_psql("SELECT count(*) FROM t WHERE a > 1")
    assert result == "9999", "ensure checksummed pages can be read back"
    node.stop()
