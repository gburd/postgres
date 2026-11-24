# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_rewind/t/003_extrafiles.pl.

pg_rewind reconciles extra files/directories: files present only on the old
primary (created after promotion) are removed, files present on the standby are
copied in, and files present in both are kept. Exercised for the 'local' and
'remote' source modes.
"""

import os


def _make_tree(base, spec):
    for relpath, content in spec:
        full = os.path.join(base, relpath)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as fh:
            fh.write(content)


def _run_test(rt, test_mode):
    rt.setup_cluster(test_mode)
    rt.start_primary()
    primary_dir = str(rt.primary.datadir)
    # Files present in both primary and standby (created before the backup).
    _make_tree(
        primary_dir,
        [
            ("tst_both_dir/both_file1", "in both1"),
            ("tst_both_dir/both_file2", "in both2"),
            ("tst_both_dir/both_subdir/both_file3", "in both3"),
        ],
    )
    rt.create_standby(test_mode)
    standby_dir = str(rt.standby.datadir)
    # Files only on the standby (after the backup).
    _make_tree(
        standby_dir,
        [
            ("tst_standby_dir/standby_file1", "in standby1"),
            ("tst_standby_dir/standby_file2", "in standby2"),
            ("tst_standby_dir/standby_file3 with 'quotes'", "in standby3"),
            ("tst_standby_dir/standby_subdir/standby_file4", "in standby4"),
        ],
    )
    # Files only on the primary (after promotion); pg_rewind should remove them.
    _make_tree(
        primary_dir,
        [
            ("tst_primary_dir/primary_file1", "in primary1"),
            ("tst_primary_dir/primary_file2", "in primary2"),
            ("tst_primary_dir/primary_subdir/primary_file3", "in primary3"),
        ],
    )
    rt.promote_standby()
    rt.run_pg_rewind(test_mode)
    # Every tst_* path remaining under the primary's data dir.
    paths = []
    for dirpath, dirs, files in os.walk(primary_dir):
        for name in dirs + files:
            full = os.path.join(dirpath, name)
            if "tst_" in full:
                paths.append(full)
    expected = [
        primary_dir + suffix
        for suffix in [
            "/tst_both_dir",
            "/tst_both_dir/both_file1",
            "/tst_both_dir/both_file2",
            "/tst_both_dir/both_subdir",
            "/tst_both_dir/both_subdir/both_file3",
            "/tst_standby_dir",
            "/tst_standby_dir/standby_file1",
            "/tst_standby_dir/standby_file2",
            "/tst_standby_dir/standby_file3 with 'quotes'",
            "/tst_standby_dir/standby_subdir",
            "/tst_standby_dir/standby_subdir/standby_file4",
        ]
    ]
    assert sorted(paths) == sorted(expected), "file lists match"
    rt.clean_rewind_test()


def test_003_extrafiles(rewind_test):
    """pg_rewind reconciles extra files/dirs (local and remote modes)."""
    _run_test(rewind_test, "local")
    _run_test(rewind_test, "remote")
