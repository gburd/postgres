# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of contrib/amcheck/t/005_pitr.pl.

Test integrity of intermediate states by PITR to those states. An origin node
generates WAL with an interrupted btree leaf-page deletion, then a replica
recovers to exactly the UNLINK_PAGE LSN and promotes; amcheck's
bt_index_parent_check must detect the interrupted page deletion and still pass.
"""

_SETUP = """\
BEGIN;
CREATE EXTENSION amcheck;
CREATE EXTENSION pg_walinspect;
CREATE TABLE not_leftmost (c text STORAGE PLAIN);
INSERT INTO not_leftmost
  SELECT repeat(n::text, database_block_size / 4)
  FROM generate_series(1,6) t(n), pg_control_init();
ALTER TABLE not_leftmost ADD CONSTRAINT not_leftmost_pk PRIMARY KEY (c);
DELETE FROM not_leftmost WHERE c ~ '^[1-4]';
SELECT pg_create_physical_replication_slot('for_walinspect', true, false);
COMMIT;
"""


def _vacuum_sql(before_vacuum_lsn):
    """SQL that VACUUMs the leaf page and returns the UNLINK_PAGE LSN."""
    return (
        "SET synchronous_commit = off;\n"
        "VACUUM (VERBOSE, INDEX_CLEANUP ON) not_leftmost;\n"
        "CREATE TABLE XLogFlush ();\n"
        "DROP TABLE XLogFlush;\n"
        "SELECT max(start_lsn)\n"
        "  FROM pg_get_wal_records_info('{}', 'FFFFFFFF/FFFFFFFF')\n"
        "  WHERE resource_manager = 'Btree' "
        "AND record_type = 'UNLINK_PAGE';".format(before_vacuum_lsn)
    )


def test_005_pitr(create_pg):
    """PITR to an interrupted-page-deletion state; amcheck detects and passes."""
    origin = create_pg("origin", has_archiving=True, allows_streaming=True, start=False)
    origin.append_conf("autovacuum = off")
    origin.start()
    origin.backup("my_backup")

    origin.safe_psql(_SETUP)
    before_vacuum_lsn = origin.safe_psql("SELECT pg_current_wal_lsn()")
    unlink_lsn = origin.safe_psql(_vacuum_sql(before_vacuum_lsn))
    origin.stop()
    assert unlink_lsn, "did not find UNLINK_PAGE record"

    replica = create_pg(
        "replica",
        from_backup=(origin, "my_backup"),
        has_restoring=True,
        start=False,
    )
    replica.append_conf("recovery_target_lsn = '{}'".format(unlink_lsn))
    replica.append_conf("recovery_target_inclusive = off")
    replica.append_conf("recovery_target_action = promote")
    replica.start()
    assert replica.poll_query_until(
        "SELECT pg_is_in_recovery() = 'f';"
    ), "Timed out while waiting for PITR promotion"

    debug = "SET client_min_messages = 'debug1'"
    result = replica.psql_capture(
        "{}; SELECT bt_index_parent_check('not_leftmost_pk', true)".format(debug)
    )
    assert result.exit_code == 0, "bt_index_parent_check passes"
    assert (
        "interrupted page deletion detected" in result.stderr
    ), "bt_index_parent_check: interrupted page deletion detected"

    result = replica.psql_capture(
        "{}; SELECT bt_index_check('not_leftmost_pk', true)".format(debug)
    )
    assert result.exit_code == 0, "bt_index_check passes"
