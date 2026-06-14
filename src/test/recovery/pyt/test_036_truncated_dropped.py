# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/036_truncated_dropped.pl.

Recovery where files are shorter than usual: replaying WAL for a relation that
was subsequently truncated or dropped.
"""

_MAKE = (
    "CREATE TABLE truncme(i int) WITH (fillfactor = 50);\n"
    "INSERT INTO truncme SELECT generate_series(1, 1000);\n"
    "UPDATE truncme SET i = 1;\n"
)


def _crash_recover(node):
    node.stop("immediate")
    node.start()


def test_truncated_dropped(create_pg):
    """PRUNE/TRUNCATE/DROP WAL replays cleanly after an immediate crash."""
    node = create_pg("n1", start=False)
    # Disable autovacuum so VACUUM deterministically prunes/truncates.
    node.append_conf("wal_level = 'replica'\nautovacuum = off")
    node.start()

    # PRUNE records for a pre-existing, then dropped, relation.
    node.safe_psql(_MAKE + "CHECKPOINT;\nVACUUM truncme;\nDROP TABLE truncme;\n")
    _crash_recover(node)

    # PRUNE records for a newly created, then dropped, relation.
    node.safe_psql(_MAKE + "VACUUM truncme;\nDROP TABLE truncme;\n")
    _crash_recover(node)

    # PRUNE records affecting a truncated block, with FPIs.
    node.safe_psql(
        _MAKE + "CHECKPOINT;\nVACUUM truncme;\nTRUNCATE truncme;\n"
        "INSERT INTO truncme SELECT generate_series(1, 10);\n"
    )
    _crash_recover(node)
    assert (
        node.safe_psql("select count(*), sum(i) FROM truncme") == "10|55"
    ), "table contents as expected after recovery"
    node.safe_psql("DROP TABLE truncme")

    # PRUNE records for blocks later truncated, without FPIs.
    node.safe_psql(
        _MAKE + "VACUUM truncme;\nTRUNCATE truncme;\n"
        "INSERT INTO truncme SELECT generate_series(1, 10);\n"
    )
    _crash_recover(node)
    assert (
        node.safe_psql("select count(*), sum(i) FROM truncme") == "10|55"
    ), "table contents as expected after recovery"
    node.safe_psql("DROP TABLE truncme")

    # Partial truncation via VACUUM.
    node.safe_psql(
        "CREATE TABLE truncme(i int) WITH (fillfactor = 50);\n"
        "INSERT INTO truncme SELECT generate_series(1, 1000);\n"
        "UPDATE truncme SET i = i + 1;\n"
        "DELETE FROM truncme WHERE i > 500;\n"
        "VACUUM truncme;\n"
        "INSERT INTO truncme SELECT generate_series(1000, 1010);\n"
    )
    _crash_recover(node)
    assert (
        node.safe_psql("select count(*), sum(i), min(i), max(i) FROM truncme")
        == "510|136304|2|1010"
    ), "table contents as expected after recovery"
    node.safe_psql("DROP TABLE truncme")
