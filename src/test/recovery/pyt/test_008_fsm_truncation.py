# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/008_fsm_truncation.pl.

FSM-driven INSERT just after truncation clears FSM slots for removed blocks;
the FSM must not return a page that no longer exists.
"""


def test_fsm_truncation(create_pg):
    """An INSERT succeeds on a promoted standby with a truncated relation FSM."""
    primary = create_pg("primary", allows_streaming=True, start=False)
    primary.append_conf(
        "wal_log_hints = on\nmax_prepared_transactions = 5\nautovacuum = off"
    )
    primary.start()

    primary.backup("primary_backup")
    standby = create_pg(
        "standby", from_backup=(primary, "primary_backup"), has_streaming=True
    )

    primary.safe_psql(
        "create table testtab (a int, b char(100));\n"
        "insert into testtab select generate_series(1,1000), 'foo';\n"
        "insert into testtab select generate_series(1,1000), 'foo';\n"
        "delete from testtab where ctid > '(8,0)';"
    )

    # Take a lock on the table to prevent the following vacuum from truncating.
    primary.safe_psql(
        """begin;
lock table testtab in row share mode;
prepare transaction 'p1';"""
    )

    # Vacuum, update FSM without truncation.
    primary.safe_psql("vacuum verbose testtab")
    primary.safe_psql("checkpoint")

    # More insert/deletes and another vacuum to ensure full-page writes.
    primary.safe_psql(
        "insert into testtab select generate_series(1,1000), 'foo';\n"
        "delete from testtab where ctid > '(8,0)';\n"
        "vacuum verbose testtab;"
    )

    # Ensure all buffers are clean on the standby.
    standby.safe_psql("checkpoint")

    # Release the lock; vacuum again, which should lead to truncation.
    primary.safe_psql("rollback prepared 'p1';\nvacuum verbose testtab;")
    primary.safe_psql("checkpoint")

    until_lsn = primary.safe_psql("SELECT pg_current_wal_lsn();")
    assert standby.poll_query_until(
        "SELECT '{}'::pg_lsn <= pg_last_wal_replay_lsn()".format(until_lsn)
    ), "standby to catch up"

    standby.promote()
    standby.safe_psql("checkpoint")

    # Restart to discard the in-memory copy of the FSM.
    standby.restart()

    # INSERT should work on the standby.
    standby.safe_psql("insert into testtab select generate_series(1,1000), 'foo';")
