# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_rewind/t/011_wal_copy.pl.

pg_rewind copies the WAL segments it needs from the source: a segment already
present and identical on the target is skipped (NONE), a segment that differs
(here deliberately corrupted on the target) is copied (COPY), and a
new-timeline segment absent on the target is copied (COPY). After the rewind the
corrupted target segment matches the source size.
"""

import os


def test_011_wal_copy(rewind_test):
    """pg_rewind reports NONE/COPY per WAL segment and copies the needed ones."""
    rewind_test.setup_cluster()
    rewind_test.start_primary()
    rewind_test.create_standby()
    primary = rewind_test.primary
    standby = rewind_test.standby
    rewind_test.primary_psql("CREATE TABLE t(a int)")
    rewind_test.primary_psql("INSERT INTO t VALUES(0)")
    wal_seg_skipped = primary.safe_psql("SELECT pg_walfile_name(pg_current_wal_lsn())")
    rewind_test.primary_psql("SELECT pg_switch_wal()")
    rewind_test.primary_psql("INSERT INTO t VALUES(0)")
    corrupt_wal_seg = primary.safe_psql("SELECT pg_walfile_name(pg_current_wal_lsn())")
    rewind_test.primary_psql("SELECT pg_switch_wal()")
    rewind_test.primary_psql("CHECKPOINT")
    rewind_test.promote_standby()
    new_tl_seg = standby.safe_psql("SELECT pg_walfile_name(pg_current_wal_lsn())")
    corrupt_target = primary.datadir / "pg_wal" / corrupt_wal_seg
    with open(corrupt_target, "a", encoding="utf-8") as fh:
        fh.write("a")
    assert corrupt_target.is_file(), "segment {} exists in target before rewind".format(
        corrupt_wal_seg
    )
    new_tl_target = primary.datadir / "pg_wal" / new_tl_seg
    assert (
        not new_tl_target.exists()
    ), "segment {} does not exist in target before rewind".format(new_tl_seg)
    size_before = os.path.getsize(corrupt_target)
    standby.stop()
    primary.stop()
    primary.command_checks_all(
        [
            "pg_rewind",
            "--debug",
            "--source-pgdata",
            str(standby.datadir),
            "--target-pgdata",
            str(primary.datadir),
            "--no-sync",
        ],
        0,
        [r""],
        [
            r"pg_wal/{} \(NONE\)".format(wal_seg_skipped),
            r"pg_wal/{} \(COPY\)".format(corrupt_wal_seg),
            r"pg_wal/{} \(COPY\)".format(new_tl_seg),
        ],
        "run pg_rewind",
    )
    assert (
        new_tl_target.is_file()
    ), "new timeline segment {} exists in target after rewind".format(new_tl_seg)
    corrupt_source = standby.datadir / "pg_wal" / corrupt_wal_seg
    assert (
        corrupt_source.is_file()
    ), "corrupted {} exists in source after rewind".format(corrupt_wal_seg)
    assert (
        corrupt_target.is_file()
    ), "corrupted {} exists in target after rewind".format(corrupt_wal_seg)
    source_size = os.path.getsize(corrupt_source)
    assert (
        size_before != source_size
    ), "different size of corrupted {} in source vs target before rewind".format(
        corrupt_wal_seg
    )
    assert (
        os.path.getsize(corrupt_target) == source_size
    ), "same size of corrupted {} in source and target after rewind".format(
        corrupt_wal_seg
    )
