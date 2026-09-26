# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/042_low_level_backup.pl.

The low-level base-backup API (pg_backup_start/pg_backup_stop) plus a manual
filesystem copy: without the backup_label the copy recovers as a crash (the
post-backup 'canary' table is absent), but with the backup_label appended it
performs proper backup recovery (the canary is present), as shown by the
distinct log messages each path emits.
"""

import os
import shutil


def test_042_low_level_backup(create_pg):
    """A low-level backup recovers as crash without, and properly with, label."""
    primary = create_pg("primary", has_archiving=True, allows_streaming=True)
    backup_name = "backup1"
    psql = primary.background_psql("postgres")
    psql.query_safe("SET client_min_messages TO WARNING")
    psql.set_query_timer_restart()
    psql.query_safe("select pg_backup_start('test label')")
    backup_dir = str(primary.backup_path(backup_name))
    shutil.copytree(primary.datadir, backup_dir, ignore_dangling_symlinks=True)
    os.unlink("{}/postmaster.pid".format(backup_dir))
    os.unlink("{}/postmaster.opts".format(backup_dir))
    os.unlink("{}/global/pg_control".format(backup_dir))
    shutil.rmtree("{}/pg_wal".format(backup_dir))
    os.mkdir("{}/pg_wal".format(backup_dir))
    primary.safe_psql("create table canary (id int)")
    segment_name = primary.safe_psql("select pg_walfile_name(pg_switch_wal())")
    assert primary.poll_query_until(
        "SELECT last_archived_wal FROM pg_stat_archiver", segment_name
    ), "Timed out while waiting for archiving of switched segment to finish"
    primary.safe_psql("checkpoint")
    shutil.copy(
        "{}/global/pg_control".format(primary.datadir),
        "{}/global/pg_control".format(backup_dir),
    )
    stop_segment_name = primary.safe_psql(
        "SELECT pg_walfile_name(pg_current_wal_lsn())"
    )
    backup_label = psql.query_safe("select labelfile from pg_backup_stop()")
    psql.quit()
    canary_query = "select count(*) from pg_class where relname = 'canary'"
    replica = create_pg("replica_fail", from_backup=(primary, backup_name), start=False)
    replica.append_conf("archive_mode = off")
    shutil.copy(
        "{}/{}".format(primary.archive_dir, stop_segment_name),
        "{}/pg_wal/{}".format(replica.datadir, stop_segment_name),
    )
    replica.start()
    assert replica.safe_psql(canary_query) == "0", "canary is missing"
    assert replica.log_contains(
        "database system was not properly shut down; automatic recovery in progress"
    ), "verify backup recovery performed with crash recovery"
    replica.teardown_node()
    replica.clean_node()
    with open("{}/backup_label".format(backup_dir), "a", encoding="utf-8") as fh:
        fh.write(backup_label)
    replica = create_pg(
        "replica_success",
        from_backup=(primary, backup_name),
        has_restoring=True,
        start=False,
    )
    replica.start()
    assert replica.safe_psql(canary_query) == "1", "canary is present"
    assert replica.log_contains(
        "starting backup recovery with redo LSN"
    ), "verify backup recovery performed with backup_label"
