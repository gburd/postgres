# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of contrib/pg_visibility/t/001_concurrent_transaction.pl.

pg_check_visible reports no errors for a vacuumed table on both primary and a streaming standby, even with a concurrent open transaction (held via a background psql session) affecting visibility-map computation.
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_001_concurrent_transaction(create_pg):
    """pg_check_visible clean on primary+standby with a concurrent transaction."""
    node = create_pg("main", allows_streaming=True, start=False)
    node.start()
    backup_name = "my_backup"
    node.backup(backup_name)
    standby = create_pg(
        "standby", from_backup=(node, backup_name), has_streaming=True, start=False
    )
    standby.start()
    node.safe_psql("CREATE DATABASE other_database;")
    bsession = node.background_psql("other_database")
    bsession.query_safe("BEGIN;\n\tSELECT txid_current();")
    node.safe_psql(
        "CREATE EXTENSION pg_visibility;\nCREATE TABLE vacuum_test AS SELECT 42 i;\nVACUUM (disable_page_skipping) vacuum_test;"
    )
    result = node.safe_psql("SELECT * FROM pg_check_visible('vacuum_test');")
    assert result == "", "pg_check_visible() detects no errors"
    node.wait_for_catchup(standby)
    result = standby.safe_psql("SELECT * FROM pg_check_visible('vacuum_test');")
    assert result == "", "pg_check_visible() detects no errors"
    bsession.query_safe("COMMIT;")
    bsession.quit()
    node.stop()
    standby.stop()
