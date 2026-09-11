# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_checksums/t/001_basic.pl.

Online enabling and disabling of data checksums on a single node: pages written
while checksums were off are read back correctly after checksums are enabled,
re-enabling is idempotent, and disabling then re-enabling preserves the data.
"""

import datachecksums_utils as dcu  # pyrefly: ignore


def test_001_basic(create_pg):
    """Data checksums can be enabled/disabled online without data loss."""
    node = create_pg("basic_node", no_data_checksums=True)
    node.safe_psql("CREATE TABLE t AS SELECT generate_series(1,10000) AS a;")
    dcu.test_checksum_state(node, "off")
    dcu.enable_data_checksums(node, wait="on")
    result = node.safe_psql("SELECT count(*) FROM t WHERE a > 1 ")
    assert result == "9999", "ensure checksummed pages can be read back"
    dcu.enable_data_checksums(node)
    dcu.test_checksum_state(node, "on")
    node.safe_psql("UPDATE t SET a = a + 1;")
    result = node.safe_psql("SELECT count(*) FROM t WHERE a > 1")
    assert result == "10000", "ensure checksummed pages can be read back"
    dcu.disable_data_checksums(node, wait=1)
    result = node.safe_psql("SELECT count(*) FROM t WHERE a > 1")
    assert result == "10000", "ensure previously checksummed pages can be read back"
    node.safe_psql("UPDATE t SET a = a + 1;")
    dcu.enable_data_checksums(node, wait="on")
    result = node.safe_psql("SELECT count(*) FROM t WHERE a > 1")
    assert result == "10000", "ensure checksummed pages can be read back"
    node.stop()
