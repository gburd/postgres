# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/xid_wraparound/t/003_wraparounds.pl.

Full XID-space wraparound: consume 100 batches of 100 million XIDs (~10 billion
total, several wraparounds) while autovacuum keeps the cluster alive, inserting
a marker row after each batch. All 101 rows (the initial plus 100 markers) must
survive. Gated on PG_TEST_EXTRA=xid_wraparound (very slow).
"""

import pypg

pytestmark = pypg.require_test_extras("xid_wraparound")


def test_003_wraparounds(create_pg):
    """The cluster survives repeated XID wraparounds; all marker rows persist."""
    node = create_pg("wraparound", start=False)
    node.append_conf(
        "\n"
        "autovacuum_naptime = 1s\n"
        "autovacuum_max_workers = 1\n"
        "log_autovacuum_min_duration = 0\n"
    )
    node.start()
    node.safe_psql("CREATE EXTENSION xid_wraparound")
    node.safe_psql(
        "\nCREATE TABLE wraparoundtest(t text) WITH (autovacuum_enabled = off);\n"
        "INSERT INTO wraparoundtest VALUES ('beginning');\n"
    )
    psql_timeout_secs = 4 * pypg.test_timeout_default()
    for i in range(1, 101):
        node.safe_psql("SELECT consume_xids(100000000)", timeout=psql_timeout_secs)
        node.safe_psql(
            "INSERT INTO wraparoundtest VALUES ('after {} batches')".format(i)
        )
    ret = node.safe_psql("SELECT COUNT(*) FROM wraparoundtest")
    assert ret == "101"
    node.stop()
