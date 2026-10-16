# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/bin/pg_combinebackup/t/002_compare_backups.pl.

A full backup and an incremental backup (taken after a variety of changes,
including tablespace activity) are each restored to a PITR standby at the same
LSN -- one directly from the full backup, the other by combining full +
incremental. The two restored clusters must be logically identical, as shown by
matching pg_dumpall output.
"""

import os
import re
import tempfile

import pypg


def _normalize(line_a, line_b):
    # tspitr1 vs tspitr2 differ only in the trailing digit of the location.
    pat = re.compile(r"(create tablespace .* location .*\btspitr)[12]", re.IGNORECASE)
    return pat.sub(r"\1N", line_a), pat.sub(r"\1N", line_b)


def test_002_compare_backups(create_pg):
    """Direct-restore and combined-restore PITR clusters dump identically."""
    tempdir = tempfile.mkdtemp(prefix="cmpbk_")
    mode = os.environ.get("PG_TEST_PG_COMBINEBACKUP_MODE") or "--copy"
    primary = create_pg(
        "primary", has_archiving=True, allows_streaming=True, start=False
    )
    primary.append_conf("summarize_wal = on")
    primary.start()
    tsprimary = tempdir + "/ts"
    os.mkdir(tsprimary)
    primary.safe_psql(
        "CREATE TABLE will_change (a int, b text);\n"
        "INSERT INTO will_change VALUES (1, 'initial test row');\n"
        "CREATE TABLE will_grow (a int, b text);\n"
        "INSERT INTO will_grow VALUES (1, 'initial test row');\n"
        "CREATE TABLE will_shrink (a int, b text);\n"
        "INSERT INTO will_shrink VALUES (1, 'initial test row');\n"
        "CREATE TABLE will_get_vacuumed (a int, b text);\n"
        "INSERT INTO will_get_vacuumed VALUES (1, 'initial test row');\n"
        "CREATE TABLE will_get_dropped (a int, b text);\n"
        "INSERT INTO will_get_dropped VALUES (1, 'initial test row');\n"
        "CREATE TABLE will_get_rewritten (a int, b text);\n"
        "INSERT INTO will_get_rewritten VALUES (1, 'initial test row');\n"
        "CREATE DATABASE db_will_get_dropped;\n"
        "CREATE TABLESPACE ts1 LOCATION '{}';\n"
        "CREATE TABLE will_not_change_in_ts (a int, b text) TABLESPACE ts1;\n"
        "INSERT INTO will_not_change_in_ts VALUES (1, 'initial test row');\n"
        "CREATE TABLE will_change_in_ts (a int, b text) TABLESPACE ts1;\n"
        "INSERT INTO will_change_in_ts VALUES (1, 'initial test row');\n"
        "CREATE TABLE will_get_dropped_in_ts (a int, b text);\n"
        "INSERT INTO will_get_dropped_in_ts VALUES (1, 'initial test row');".format(
            tsprimary
        )
    )
    tsoids = [
        d
        for d in pypg.slurp_dir("{}/pg_tblspc".format(primary.datadir))
        if d and d[0].isdigit()
    ]
    assert len(tsoids) == 1, "exactly one user-defined tablespace"
    tsoid = tsoids[0]
    backup1path = "{}/backup1".format(primary.backup_dir)
    tsbackup1path = tempdir + "/ts1backup"
    os.mkdir(tsbackup1path)
    primary.command_ok(
        [
            "pg_basebackup",
            "--no-sync",
            "--pgdata",
            backup1path,
            "--checkpoint",
            "fast",
            "--tablespace-mapping",
            "{}={}".format(tsprimary, tsbackup1path),
        ],
        "full backup",
    )
    primary.safe_psql(
        "UPDATE will_change SET b = 'modified value' WHERE a = 1;\n"
        "UPDATE will_change_in_ts SET b = 'modified value' WHERE a = 1;\n"
        "INSERT INTO will_grow SELECT g, 'additional row' "
        "FROM generate_series(2, 5000) g;\n"
        "TRUNCATE will_shrink;\n"
        "VACUUM will_get_vacuumed;\n"
        "DROP TABLE will_get_dropped;\n"
        "DROP TABLE will_get_dropped_in_ts;\n"
        "CREATE TABLE newly_created (a int, b text);\n"
        "INSERT INTO newly_created VALUES (1, 'row for new table');\n"
        "CREATE TABLE newly_created_in_ts (a int, b text) TABLESPACE ts1;\n"
        "INSERT INTO newly_created_in_ts VALUES (1, 'row for new table');\n"
        "VACUUM FULL will_get_rewritten;\n"
        "DROP DATABASE db_will_get_dropped;\n"
        "CREATE DATABASE db_newly_created;"
    )
    backup2path = "{}/backup2".format(primary.backup_dir)
    tsbackup2path = tempdir + "/tsbackup2"
    os.mkdir(tsbackup2path)
    primary.command_ok(
        [
            "pg_basebackup",
            "--no-sync",
            "--pgdata",
            backup2path,
            "--checkpoint",
            "fast",
            "--tablespace-mapping",
            "{}={}".format(tsprimary, tsbackup2path),
            "--incremental",
            backup1path + "/backup_manifest",
        ],
        "incremental backup",
    )
    lsn = primary.safe_psql("SELECT pg_current_wal_lsn();")
    primary.safe_psql("SELECT txid_current();")
    primary.safe_psql("SELECT pg_switch_wal()")
    assert primary.poll_query_until(
        "SELECT pg_walfile_name('{}') <= last_archived_wal "
        "FROM pg_stat_archiver;".format(lsn)
    ), "Timed out while waiting for WAL segment to be archived"
    tspitr1path = tempdir + "/tspitr1"
    pitr1 = create_pg(
        "pitr1",
        from_backup=(primary, "backup1"),
        standby=True,
        has_restoring=True,
        tablespace_map={tsoid: tspitr1path},
        start=False,
    )
    pitr1.append_conf(
        "\nrecovery_target_lsn = '{}'\n"
        "recovery_target_action = 'promote'\narchive_mode = 'off'\n".format(lsn)
    )
    pitr1.start()
    tspitr2path = tempdir + "/tspitr2"
    pitr2 = create_pg(
        "pitr2",
        from_backup=(primary, "backup2"),
        standby=True,
        has_restoring=True,
        combine_with_prior=["backup1"],
        tablespace_map={tsbackup2path: tspitr2path},
        combine_mode=mode,
        start=False,
    )
    pitr2.append_conf(
        "\nrecovery_target_lsn = '{}'\n"
        "recovery_target_action = 'promote'\narchive_mode = 'off'\n".format(lsn)
    )
    pitr2.start()
    assert pitr1.poll_query_until(
        "SELECT NOT pg_is_in_recovery();"
    ), "Timed out while waiting apply to reach LSN {}".format(lsn)
    assert pitr2.poll_query_until(
        "SELECT NOT pg_is_in_recovery();"
    ), "Timed out while waiting apply to reach LSN {}".format(lsn)
    backupdir = primary.backup_dir
    dump1 = "{}/pitr1.dump".format(backupdir)
    dump2 = "{}/pitr2.dump".format(backupdir)
    pitr1.command_ok(
        [
            "pg_dumpall",
            "--restrict-key",
            "test",
            "--no-sync",
            "--no-unlogged-table-data",
            "--file",
            dump1,
            "--dbname",
            pitr1.connstr("postgres"),
        ],
        "dump from PITR 1",
    )
    pitr2.command_ok(
        [
            "pg_dumpall",
            "--restrict-key",
            "test",
            "--no-sync",
            "--no-unlogged-table-data",
            "--file",
            dump2,
            "--dbname",
            pitr2.connstr("postgres"),
        ],
        "dump from PITR 2",
    )
    pypg.compare_files(
        dump1, dump2, "contents of dumps match for both PITRs", _normalize
    )
    pitr1.stop()
    pitr2.stop()
    primary.stop()
