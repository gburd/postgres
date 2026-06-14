# Copyright (c) 2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/053_standby_login_event_trigger.pl.

Connecting to a standby still works after a login event trigger has been
created and dropped on the primary, leaving a dangling pg_database.dathasloginevt
flag that replicates to the standby.
"""

import re

_HASLOGINEVT = (
    "SELECT dathasloginevt FROM pg_database WHERE datname = 'regress_login_evt'"
)


def test_standby_login_event_trigger(create_pg):
    """A standby tolerates a dangling dathasloginevt flag on a login to that DB."""
    primary = create_pg("primary", allows_streaming=True)
    primary.backup("login_evt_backup")
    standby = create_pg(
        "standby",
        from_backup=(primary, "login_evt_backup"),
        has_streaming=True,
        start=False,
    )
    standby.start()

    # A dedicated database isolates the dangling flag from helpers that connect
    # to "postgres".
    primary.safe_psql("CREATE DATABASE regress_login_evt")
    primary.wait_for_catchup(standby)
    standby.safe_psql("SELECT 1", dbname="regress_login_evt")

    # Create then drop a login event trigger in that database; the flag stays
    # set on disk until a later login on the primary clears it.
    primary.safe_psql(
        "CREATE FUNCTION init_session() RETURNS event_trigger "
        "LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'init_session'; END $$;\n"
        "CREATE EVENT TRIGGER init_session ON login "
        "EXECUTE FUNCTION init_session();\n"
        "ALTER EVENT TRIGGER init_session ENABLE ALWAYS;\n"
        "DROP EVENT TRIGGER init_session;\n"
        "DROP FUNCTION init_session();",
        dbname="regress_login_evt",
    )
    primary.wait_for_catchup(standby)

    assert (
        primary.safe_psql(_HASLOGINEVT) == "t"
    ), "dathasloginevt remains set on primary after DROP EVENT TRIGGER"
    assert (
        standby.safe_psql(_HASLOGINEVT) == "t"
    ), "dathasloginevt replicated to standby"

    # A login to that DB on the standby must not try to clear the flag (which
    # would need AccessExclusiveLock, forbidden during recovery).
    result = standby.psql_capture("SELECT 1", dbname="regress_login_evt")
    assert result.rc == 0, "standby accepts connection to DB with dangling flag"
    assert not re.search(
        r"cannot acquire lock mode AccessExclusiveLock", result.stderr
    ), "no AccessExclusiveLock FATAL on standby login"

    # A login on the primary clears the flag in place.
    primary.safe_psql("SELECT 1", dbname="regress_login_evt")
    assert (
        primary.safe_psql(_HASLOGINEVT) == "f"
    ), "primary clears dathasloginevt on next login after DROP"

    # The in-place update isn't auto-flushed; force a flush so it reaches the
    # standby.
    primary.safe_psql("SELECT pg_switch_wal()")
    primary.wait_for_catchup(standby)
    assert (
        standby.safe_psql(_HASLOGINEVT) == "f"
    ), "cleared dathasloginevt replicates to standby"
