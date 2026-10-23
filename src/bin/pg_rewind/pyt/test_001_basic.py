# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_rewind/t/001_basic.pl.

End-to-end pg_rewind: after the standby is promoted and the old primary
diverges (inserts, truncation, copy-tail, dropped table, in-place tablespace),
pg_rewind brings the old primary back in line with the promoted standby. The
'local' mode additionally checks pg_rewind's refusal to run against a running
target/source and its --dry-run. Exercised for 'local', 'remote', and
'archive' source modes.
"""

import platform

import pypg


def _setup_diverge(rt, test_mode):
    rt.setup_cluster(test_mode)
    rt.start_primary()
    # In-place tablespace with some data.
    rt.primary_psql("CREATE TABLESPACE space_test LOCATION ''")
    rt.primary_psql("CREATE TABLE space_tbl (d text) TABLESPACE space_test")
    rt.primary_psql("INSERT INTO space_tbl VALUES ('in primary, before promotion')")
    rt.primary_psql("CREATE TABLE tbl1 (d text)")
    rt.primary_psql("INSERT INTO tbl1 VALUES ('in primary')")
    rt.primary_psql("CREATE TABLE trunc_tbl (d text)")
    rt.primary_psql("INSERT INTO trunc_tbl VALUES ('in primary')")
    rt.primary_psql("CREATE TABLE tail_tbl (id integer, d text)")
    rt.primary_psql("INSERT INTO tail_tbl VALUES (0, 'in primary')")
    rt.primary_psql("CREATE TABLE drop_tbl (d text)")
    rt.primary_psql("INSERT INTO drop_tbl VALUES ('in primary')")
    rt.primary_psql("CHECKPOINT")
    rt.create_standby(test_mode)
    # Data replicated to the standby before promotion.
    rt.primary_psql("INSERT INTO tbl1 values ('in primary, before promotion')")
    rt.primary_psql("INSERT INTO trunc_tbl values ('in primary, before promotion')")
    rt.primary_psql(
        "INSERT INTO tail_tbl SELECT g, 'in primary, before promotion: ' || g "
        "FROM generate_series(1, 10000) g"
    )
    rt.primary_psql("CHECKPOINT")
    rt.promote_standby()
    # Diverge the old primary from the promoted standby.
    rt.primary_psql("INSERT INTO tbl1 VALUES ('in primary, after promotion')")
    rt.standby_psql("INSERT INTO tbl1 VALUES ('in standby, after promotion')")
    rt.primary_psql(
        "INSERT INTO trunc_tbl SELECT 'in primary, after promotion: ' || g "
        "FROM generate_series(1, 10000) g"
    )
    rt.primary_psql("DELETE FROM tail_tbl WHERE id > 10")
    rt.primary_psql("VACUUM tail_tbl")
    rt.primary_psql("insert into drop_tbl values ('in primary, after promotion')")
    rt.primary_psql("DROP TABLE drop_tbl")
    rt.primary_psql("INSERT INTO space_tbl VALUES ('in primary, after promotion')")
    rt.standby_psql("INSERT INTO space_tbl VALUES ('in standby, after promotion')")


def _local_negative_checks(rt, pg_bin):
    primary_pgdata = str(rt.primary.datadir)
    standby_pgdata = str(rt.standby.datadir)
    base = [
        "pg_rewind",
        "--debug",
        "--source-pgdata",
        standby_pgdata,
        "--target-pgdata",
        primary_pgdata,
        "--no-sync",
    ]
    pg_bin.command_fails(base, "pg_rewind with running target")
    pg_bin.command_fails(
        base + ["--no-ensure-shutdown"],
        "pg_rewind --no-ensure-shutdown with running target",
    )
    rt.primary.stop()
    pg_bin.command_fails(
        base + ["--no-ensure-shutdown"], "pg_rewind with unexpected running source"
    )
    rt.standby.stop()
    pg_bin.command_ok(base + ["--dry-run"], "pg_rewind --dry-run")
    rt.standby.start()
    rt.primary.start()


def _check_results(rt):
    rt.check_query(
        "SELECT * FROM space_tbl ORDER BY d",
        "in primary, before promotion\nin standby, after promotion",
        "table content",
    )
    rt.check_query(
        "SELECT * FROM tbl1",
        "in primary\nin primary, before promotion\nin standby, after promotion",
        "table content",
    )
    rt.check_query(
        "SELECT * FROM trunc_tbl",
        "in primary\nin primary, before promotion",
        "truncation",
    )
    rt.check_query("SELECT count(*) FROM tail_tbl", "10001", "tail-copy")
    rt.check_query("SELECT * FROM drop_tbl", "in primary", "drop")
    if platform.system() != "Windows":
        # unix-style permissions are not supported on Windows (cf. the SKIP
        # block in the Perl original).
        assert pypg.check_mode_recursive(
            rt.primary.datadir, 0o700, 0o600
        ), "check PGDATA permissions"


def _run_test(rt, pg_bin, test_mode):
    _setup_diverge(rt, test_mode)
    if test_mode == "local":
        _local_negative_checks(rt, pg_bin)
    rt.run_pg_rewind(test_mode)
    _check_results(rt)
    rt.clean_rewind_test()


def test_001_basic(rewind_test, pg_bin):
    """Full pg_rewind divergence reconciliation (local, remote, archive)."""
    _run_test(rewind_test, pg_bin, "local")
    _run_test(rewind_test, pg_bin, "remote")
    _run_test(rewind_test, pg_bin, "archive")
