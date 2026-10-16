# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_rewind/t/009_growing_files.pl.

pg_rewind must error out if a source file grows while it is being copied. The
source file's own size is made to change mid-copy by redirecting pg_rewind's
stderr (--debug) into that very file, so the file the rewind is copying keeps
growing; pg_rewind detects the mismatch and fails with "size of source file".
"""

import os


def test_009_growing_files(rewind_test, pg_bin):
    """pg_rewind errors when a source file grows during the copy."""
    rewind_test.setup_cluster("local")
    rewind_test.start_primary()
    rewind_test.primary_psql("CREATE TABLE tbl1 (d text)")
    rewind_test.primary_psql("INSERT INTO tbl1 VALUES ('in primary')")
    rewind_test.primary_psql("CHECKPOINT")
    rewind_test.create_standby("local")
    rewind_test.primary_psql("INSERT INTO tbl1 values ('in primary, before promotion')")
    rewind_test.primary_psql("CHECKPOINT")
    rewind_test.promote_standby()
    rewind_test.primary_psql("INSERT INTO tbl1 VALUES ('in primary, after promotion')")
    rewind_test.standby_psql("INSERT INTO tbl1 VALUES ('in standby, after promotion')")
    primary = rewind_test.primary
    standby = rewind_test.standby
    standby.stop()
    primary.stop()
    primary_pgdata = str(primary.datadir)
    standby_pgdata = str(standby.datadir)
    both_dir = os.path.join(standby_pgdata, "tst_both_dir")
    os.mkdir(both_dir)
    file1 = os.path.join(both_dir, "file1")
    with open(file1, "w", encoding="utf-8") as fh:
        fh.write("a")
    rc = pg_bin.run_redirect_stderr(
        [
            "pg_rewind",
            "--debug",
            "--source-pgdata",
            standby_pgdata,
            "--target-pgdata",
            primary_pgdata,
            "--no-sync",
        ],
        file1,
    )
    assert rc != 0, "Error out on copying growing file"
    primary_size = os.path.getsize(
        os.path.join(primary_pgdata, "tst_both_dir", "file1")
    )
    standby_size = os.path.getsize(file1)
    assert standby_size != primary_size, "File sizes should differ"
    last = ""
    with open(file1, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            last = line
    assert "error: size of source file" in last, "Check error message"
