# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_combinebackup/t/005_integrity.pl.

pg_combinebackup integrity checks: it rejects combining two full backups, a
non-full first backup, backups from different nodes (system identifier
mismatch), a manifest whose system identifier disagrees with the control file,
an omitted required backup, backups given out of order, and a synthetic backup
re-combined with an already-included incremental -- while accepting valid
full+incremental chains (including stepwise synthetic combination).
"""

import os
import shutil


def _fails(node, args, pattern, msg, mode):
    node.command_fails_like(["pg_combinebackup"] + args + [mode], pattern, msg)


def test_005_integrity(create_pg):
    """pg_combinebackup rejects malformed chains and accepts valid ones."""
    mode = os.environ.get("PG_TEST_PG_COMBINEBACKUP_MODE") or "--copy"
    node1 = create_pg("node1", has_archiving=True, allows_streaming=True, start=False)
    node1.append_conf("summarize_wal = on")
    node1.start()
    with open("{}/INCREMENTAL.config".format(node1.datadir), "w", encoding="utf-8"):
        pass
    node2 = create_pg(
        "node2",
        force_initdb=True,
        has_archiving=True,
        allows_streaming=True,
        start=False,
    )
    node2.append_conf("summarize_wal = on")
    node2.start()
    bdir = node1.backup_dir
    backup1 = "{}/backup1".format(bdir)
    backup2 = "{}/backup2".format(bdir)
    backup3 = "{}/backup3".format(bdir)
    other1 = "{}/backupother1".format(bdir)
    other2 = "{}/backupother2".format(bdir)
    result = "{}/result".format(bdir)
    node1.command_ok(
        ["pg_basebackup", "--pgdata", backup1, "--no-sync", "--checkpoint", "fast"],
        "full backup from node1",
    )
    node1.command_ok(
        [
            "pg_basebackup",
            "--pgdata",
            backup2,
            "--no-sync",
            "--checkpoint",
            "fast",
            "--incremental",
            backup1 + "/backup_manifest",
        ],
        "incremental backup from node1",
    )
    node1.command_ok(
        [
            "pg_basebackup",
            "--pgdata",
            backup3,
            "--no-sync",
            "--checkpoint",
            "fast",
            "--incremental",
            backup2 + "/backup_manifest",
        ],
        "another incremental backup from node1",
    )
    node2.command_ok(
        ["pg_basebackup", "--pgdata", other1, "--no-sync", "--checkpoint", "fast"],
        "full backup from node2",
    )
    node2.command_ok(
        [
            "pg_basebackup",
            "--pgdata",
            other2,
            "--no-sync",
            "--checkpoint",
            "fast",
            "--incremental",
            other1 + "/backup_manifest",
        ],
        "incremental backup from node2",
    )
    _fails(
        node1,
        [backup1, backup1, "--output", result],
        r"is a full backup, but only the first backup should be a full backup",
        "can't combine full backups",
        mode,
    )
    _fails(
        node1,
        [backup2, backup2, "--output", result],
        r"is an incremental backup, but the first backup should be a full backup",
        "can't combine full backups",
        mode,
    )
    _fails(
        node1,
        [backup1, other2, "--output", result],
        r"expected system identifier.*but found",
        "can't combine backups from different nodes",
        mode,
    )
    os.rename(
        "{}/backup_manifest".format(backup2), "{}/backup_manifest.orig".format(backup2)
    )
    shutil.copy(
        "{}/backup_manifest".format(other2), "{}/backup_manifest".format(backup2)
    )
    _fails(
        node1,
        [backup1, backup2, backup3, "--output", result],
        r" manifest system identifier is .*, but control file has ",
        "can't combine backups with different manifest system identifier ",
        mode,
    )
    shutil.move(
        "{}/backup_manifest.orig".format(backup2), "{}/backup_manifest".format(backup2)
    )
    _fails(
        node1,
        [backup1, backup3, "--output", result],
        r"starts at LSN.*but expected",
        "can't omit a required backup",
        mode,
    )
    _fails(
        node1,
        [backup1, backup3, backup2, "--output", result],
        r"starts at LSN.*but expected",
        "can't combine backups in the wrong order",
        mode,
    )
    node1.command_ok(
        ["pg_combinebackup", backup1, backup2, backup3, "--output", result, mode],
        "can combine 3 matching backups",
    )
    shutil.rmtree(result)
    synthetic12 = "{}/synthetic12".format(bdir)
    node1.command_ok(
        ["pg_combinebackup", backup1, backup2, "--output", synthetic12, mode],
        "can combine 2 matching backups",
    )
    node1.command_ok(
        ["pg_combinebackup", synthetic12, backup3, "--output", result, mode],
        "can combine synthetic backup with later incremental",
    )
    shutil.rmtree(result)
    _fails(
        node1,
        [synthetic12, backup2, "--output", result],
        r"starts at LSN.*but expected",
        "can't combine synthetic backup with included incremental",
        mode,
    )
    node1.stop()
    node2.stop()
