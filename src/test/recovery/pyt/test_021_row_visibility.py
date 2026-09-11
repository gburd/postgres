# Copyright (c) 2017-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/021_row_visibility.pl.

Row visibility on a hot standby tracks the primary through streaming: rows are
invisible until their inserting/updating transaction commits and the change is
replayed, uncommitted updates and prepared (two-phase) transactions are
invisible until COMMIT/COMMIT PREPARED, and an aborted prepared transaction
never becomes visible. Driven via interactive psql sessions on both nodes.
"""

import pypg


def _send_wait(session, query, pattern):
    """Send query to an interactive session and wait for pattern in its output."""
    session.query_until(pattern, query + "\n")


def test_021_row_visibility(create_pg):
    """A hot standby reflects primary row visibility as transactions resolve."""
    primary = create_pg("primary", allows_streaming=True, start=False)
    primary.append_conf("max_prepared_transactions=10")
    primary.start()
    primary.safe_psql("CREATE TABLE public.test_visibility (data text not null)")
    backup_name = "my_backup"
    primary.backup(backup_name)
    standby = create_pg(
        "standby", from_backup=(primary, backup_name), has_streaming=True, start=False
    )
    standby.append_conf("max_prepared_transactions=10")
    standby.start()
    timeout = 2 * pypg.test_timeout_default()
    psql_primary = primary.background_psql(
        "postgres",
        on_error_stop=False,
        tuples_only=False,
        quiet=False,
        timeout=timeout,
    )
    psql_standby = standby.background_psql(
        "postgres",
        on_error_stop=False,
        tuples_only=False,
        quiet=False,
        timeout=timeout,
    )
    _send_wait(
        psql_standby,
        "SELECT * FROM test_visibility ORDER BY data;",
        r"(?m)^\(0 rows\)$",
    )
    primary.psql_capture("INSERT INTO test_visibility VALUES ('first insert')")
    primary.wait_for_catchup(standby)
    _send_wait(
        psql_standby,
        "SELECT * FROM test_visibility ORDER BY data;",
        r"(?m)first insert.*\n\(1 row\)",
    )
    _send_wait(
        psql_primary,
        "\nBEGIN;\nUPDATE test_visibility SET data = 'first update' "
        "RETURNING data;\n",
        r"(?m)^UPDATE 1$",
    )
    primary.psql_capture("SELECT txid_current();")
    primary.wait_for_catchup(standby)
    _send_wait(
        psql_standby,
        "SELECT * FROM test_visibility ORDER BY data;",
        r"(?m)first insert.*\n\(1 row\)",
    )
    _send_wait(psql_primary, "COMMIT;", r"(?m)^COMMIT$")
    primary.wait_for_catchup(standby)
    _send_wait(
        psql_standby,
        "SELECT * FROM test_visibility ORDER BY data;",
        r"(?m)first update\n\(1 row\)$",
    )
    _send_wait(
        psql_primary,
        "\nDELETE from test_visibility;\n"
        "BEGIN;\n"
        "INSERT INTO test_visibility VALUES('inserted in prepared will_commit');\n"
        "PREPARE TRANSACTION 'will_commit';",
        r"(?m)^PREPARE TRANSACTION$",
    )
    _send_wait(
        psql_primary,
        "\nBEGIN;\n"
        "INSERT INTO test_visibility VALUES('inserted in prepared will_abort');\n"
        "PREPARE TRANSACTION 'will_abort';\n",
        r"(?m)^PREPARE TRANSACTION$",
    )
    primary.wait_for_catchup(standby)
    _send_wait(
        psql_standby,
        "SELECT * FROM test_visibility ORDER BY data;",
        r"(?m)^\(0 rows\)$",
    )
    primary.safe_psql("COMMIT PREPARED 'will_commit';")
    primary.safe_psql("ROLLBACK PREPARED 'will_abort';")
    primary.wait_for_catchup(standby)
    _send_wait(
        psql_standby,
        "SELECT * FROM test_visibility ORDER BY data;",
        r"(?m)will_commit.*\n\(1 row\)$",
    )
    psql_primary.quit()
    psql_standby.quit()
    primary.stop()
    standby.stop()
