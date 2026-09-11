# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_combinebackup/t/009_no_full_file.pl.

pg_combinebackup must reject a chain where the supposed full backup actually
contains an incremental file: after corrupting the full backup so a file is
replaced by its INCREMENTAL.* counterpart, combining fails with a clear error.
"""

import os
import shutil

import pypg


def test_009_no_full_file(create_pg):
    """pg_combinebackup fails when the full backup contains an incremental file."""
    primary = create_pg(
        "primary", has_archiving=True, allows_streaming=True, start=False
    )
    primary.append_conf("summarize_wal = on")
    primary.start()
    backup1path = "{}/backup1".format(primary.backup_dir)
    primary.command_ok(
        ["pg_basebackup", "--pgdata", backup1path, "--no-sync", "--checkpoint", "fast"],
        "full backup",
    )
    backup2path = "{}/backup2".format(primary.backup_dir)
    primary.command_ok(
        [
            "pg_basebackup",
            "--pgdata",
            backup2path,
            "--no-sync",
            "--checkpoint",
            "fast",
            "--incremental",
            backup1path + "/backup_manifest",
        ],
        "incremental backup",
    )
    filelist = [
        f
        for f in pypg.slurp_dir("{}/base/1".format(backup2path))
        if f.startswith("INCREMENTAL.")
    ]
    for iname in filelist:
        name = iname[len("INCREMENTAL.") :]
        full_file = "{}/base/1/{}".format(backup1path, name)
        if os.path.isfile(full_file):
            shutil.copy(
                "{}/base/1/{}".format(backup2path, iname),
                "{}/base/1/{}".format(backup1path, iname),
            )
            os.unlink(full_file)
            break
    outpath = "{}/out".format(primary.backup_dir)
    primary.command_fails_like(
        ["pg_combinebackup", backup1path, backup2path, "--output", outpath],
        r"full backup contains unexpected incremental file",
        "pg_combinebackup fails",
    )
