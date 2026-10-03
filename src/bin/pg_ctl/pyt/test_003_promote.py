# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""
Port of src/bin/pg_ctl/t/003_promote.pl.

Exercises pg_ctl promote: failures against a nonexistent directory, a stopped
instance, and a primary; and successful promotion of a streaming standby both
with --no-wait and with the default (waiting) behavior.
"""


def test_promote(pg_bin, create_pg, tmp_path):
    """pg_ctl promote against bad targets fails; promoting a standby works."""
    pg_bin.command_fails_like(
        ["pg_ctl", "--pgdata", tmp_path / "nonexistent", "promote"],
        r"directory .* does not exist",
        "pg_ctl promote with nonexistent directory",
    )

    primary = create_pg("primary", allows_streaming=True, start=False)

    pg_bin.command_fails_like(
        ["pg_ctl", "--pgdata", primary.datadir, "promote"],
        r"PID file .* does not exist",
        "pg_ctl promote of not running instance fails",
    )

    primary.start()

    pg_bin.command_fails_like(
        ["pg_ctl", "--pgdata", primary.datadir, "promote"],
        r"not in standby mode",
        "pg_ctl promote of primary instance fails",
    )

    primary.backup("my_backup")
    standby = create_pg(
        "standby", from_backup=(primary, "my_backup"), has_streaming=True
    )

    assert standby.safe_psql("SELECT pg_is_in_recovery()") == "t", "standby in recovery"

    pg_bin.command_ok(
        ["pg_ctl", "--pgdata", standby.datadir, "--no-wait", "promote"],
        "pg_ctl --no-wait promote of standby runs",
    )

    assert standby.poll_query_until(
        "SELECT NOT pg_is_in_recovery()"
    ), "promoted standby is not in recovery"

    # Same again with the default wait option.
    standby2 = create_pg(
        "standby2", from_backup=(primary, "my_backup"), has_streaming=True
    )

    assert (
        standby2.safe_psql("SELECT pg_is_in_recovery()") == "t"
    ), "standby is in recovery"

    pg_bin.command_ok(
        ["pg_ctl", "--pgdata", standby2.datadir, "promote"],
        "pg_ctl promote of standby runs",
    )

    # No wait here: the default promote already waited.
    assert (
        standby2.safe_psql("SELECT pg_is_in_recovery()") == "f"
    ), "promoted standby is not in recovery"
