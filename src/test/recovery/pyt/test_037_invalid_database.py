# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/037_invalid_database.pl.

Handling of interrupted DROP DATABASE and access to invalid databases.
"""

import re

_CANCEL_DO = """\
DO $$
BEGIN
    WHILE NOT EXISTS(SELECT * FROM pg_locks WHERE NOT granted AND relation = 'pg_tablespace'::regclass AND mode = 'AccessShareLock') LOOP
        PERFORM pg_sleep(.1);
    END LOOP;
END$$;
SELECT pg_cancel_backend({pid});"""


def _mark_invalid_checks(node):
    node.safe_psql(
        "CREATE DATABASE regression_invalid;\n"
        "UPDATE pg_database SET datconnlimit = -2 "
        "WHERE datname = 'regression_invalid';"
    )

    result = node.psql_capture("", dbname="regression_invalid")
    assert result.exit_code == 2, "can't connect to invalid database - error code"
    assert re.search(
        r'FATAL:\s+cannot connect to invalid database "regression_invalid"',
        result.stderr,
    ), "can't connect to invalid database - error message"

    assert (
        node.psql_capture(
            "ALTER DATABASE regression_invalid CONNECTION LIMIT 10"
        ).exit_code
        == 2
    ), "can't ALTER invalid database"
    assert (
        node.psql_capture(
            "CREATE DATABASE copy_invalid TEMPLATE regression_invalid"
        ).exit_code
        == 3
    ), "can't use invalid database as template"

    # VACUUM must ignore an invalid database when truncating the clog.
    result = node.psql_capture(
        "UPDATE pg_database SET datfrozenxid = '123456' "
        "WHERE datname = 'regression_invalid';\n"
        "DROP TABLE IF EXISTS foo_tbl; CREATE TABLE foo_tbl();\n"
        "VACUUM FREEZE;"
    )
    assert not re.search(
        r"some databases have not been vacuumed in over 2 billion transactions",
        result.stderr,
    ), "invalid databases are ignored by vac_truncate_clog"

    assert (
        node.psql_capture("DROP DATABASE regression_invalid").exit_code == 0
    ), "can DROP invalid database"
    assert (
        node.psql_capture("DROP DATABASE regression_invalid").exit_code == 3
    ), "can't drop already dropped database"


def test_invalid_database(create_pg):
    """Invalid databases reject connections/ALTER; interrupted DROP is handled."""
    node = create_pg("node", start=False)
    node.append_conf(
        "autovacuum = off\n"
        "max_prepared_transactions=5\n"
        "log_min_duration_statement=0\n"
        "log_connections=receipt\n"
        "log_disconnections=on"
    )
    node.start()

    _mark_invalid_checks(node)

    # Interrupt DROP DATABASE while it waits on a lock held by a 2PC xact.
    cancel = node.background_psql(on_error_stop=True)
    bgpsql = node.background_psql(on_error_stop=False)
    pid = bgpsql.query("SELECT pg_backend_pid()").strip()

    bgpsql.query(
        "CREATE DATABASE regression_invalid_interrupt;\n"
        "BEGIN;\n"
        "LOCK pg_tablespace;\n"
        "PREPARE TRANSACTION 'lock_tblspc';"
    )
    # Fire the DROP; it blocks on the still-held lock.
    bgpsql.query_until(r"", "DROP DATABASE regression_invalid_interrupt;\n")

    # Wait until the DROP is blocked, then cancel it.
    cancel.query(_CANCEL_DO.format(pid=pid))
    cancel.quit()

    bgpsql.wait_for_stderr(r"canceling statement due to user request")
    bgpsql.clear()

    assert (
        node.psql_capture("", dbname="regression_invalid_interrupt").exit_code == 2
    ), "can't connect to invalid_interrupt database"

    # Release the lock and drop the database for real.
    bgpsql.query("ROLLBACK PREPARED 'lock_tblspc'")
    bgpsql.query("DROP DATABASE regression_invalid_interrupt")
    bgpsql.quit()
