# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/commit_ts/t/002_standby.pl.

Commit timestamps replicate to a streaming standby: a transaction's commit timestamp queried on the standby matches the value on the primary.
Generated from the Perl original via .agent/gen_golden.py.
"""

import re


def test_002_standby(create_pg):
    """Commit timestamps replicate to a streaming standby."""
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
    primary_ts = primary.safe_psql(
        "SELECT ts.* FROM pg_class, pg_xact_commit_timestamp(xmin) AS ts WHERE relname = 't10'"
    )
    primary_lsn = primary.safe_psql("select pg_current_wal_lsn()")
    assert standby.poll_query_until(
        "SELECT '" + str(primary_lsn) + "'::pg_lsn <= pg_last_wal_replay_lsn()"
    ), "standby never caught up"
    standby_ts = standby.safe_psql(
        "select ts.* from pg_class, pg_xact_commit_timestamp(xmin) ts where relname = 't10'"
    )
    assert primary_ts == standby_ts, "standby gives same value as primary"
    primary.append_conf("track_commit_timestamp = off")
    primary.restart()
    primary.safe_psql("checkpoint")
    primary_lsn = primary.safe_psql("select pg_current_wal_lsn()")
    assert standby.poll_query_until(
        "SELECT '" + str(primary_lsn) + "'::pg_lsn <= pg_last_wal_replay_lsn()"
    ), "standby never caught up"
    standby.safe_psql("checkpoint")
    result = standby.psql_capture(
        "select ts.* from pg_class, pg_xact_commit_timestamp(xmin) ts where relname = 't10'"
    )
    assert result.rc == 3, "standby errors when primary turned feature off"
    assert result.stdout == "", "standby gives no value when primary turned feature off"
    assert re.search(
        r"""could not get commit timestamp data""",
        result.stderr,
    ), "expected error when primary turned feature off"
