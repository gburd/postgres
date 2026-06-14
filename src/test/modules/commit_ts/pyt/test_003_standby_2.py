# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/commit_ts/t/003_standby_2.pl.

Commit timestamps and the track_commit_timestamp setting interact correctly across a standby promotion: after promotion the standby returns valid commit timestamps.
Generated from the Perl original via .agent/gen_golden.py.
"""

import re


def test_003_standby_2(create_pg):
    """Commit timestamps remain valid after standby promotion."""
    bkplabel = "backup"
    primary = create_pg("primary", allows_streaming=True, start=False)
    primary.append_conf("\n\ttrack_commit_timestamp = on\n\tmax_wal_senders = 5\n\t")
    primary.start()
    primary.backup(bkplabel)
    standby = create_pg(
        "standby", from_backup=(primary, bkplabel), has_streaming=True, start=False
    )
    standby.start()
    for i in range(1, 11):
        primary.safe_psql("create table t" + str(i) + "()")
    primary.append_conf("track_commit_timestamp = off")
    primary.restart()
    primary.safe_psql("checkpoint")
    primary_lsn = primary.safe_psql("select pg_current_wal_lsn()")
    assert standby.poll_query_until(
        "SELECT '" + str(primary_lsn) + "'::pg_lsn <= pg_last_wal_replay_lsn()"
    ), "standby never caught up"
    standby.safe_psql("checkpoint")
    standby.restart()
    result = standby.psql_capture(
        "SELECT ts.* FROM pg_class, pg_xact_commit_timestamp(xmin) AS ts WHERE relname = 't10'"
    )
    assert (
        result.exit_code == 3
    ), "expect error when getting commit timestamp after restart"
    assert result.stdout == "", "standby does not return a value after restart"
    assert re.search(
        r"""could not get commit timestamp data""",
        result.stderr,
    ), "expected err msg after restart"
    primary.append_conf("track_commit_timestamp = on")
    primary.restart()
    primary.append_conf("track_commit_timestamp = off")
    primary.restart()
    standby.promote()
    standby.safe_psql("create table t11()")
    standby_ts = standby.safe_psql(
        "SELECT ts.* FROM pg_class, pg_xact_commit_timestamp(xmin) AS ts WHERE relname = 't11'"
    )
    assert standby_ts != "", "standby gives valid value ($standby_ts) after promotion"
