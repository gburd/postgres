# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_dump/t/004_pg_dump_parallel.pl.

A parallel directory-format pg_dump (--jobs) of a database with hash-partitioned
tables (whose unique constraints make restore ordering non-trivial) restores
cleanly with a parallel pg_restore (--jobs), both in COPY form and with
--inserts.
"""

_SETUP = """
create type digit as enum ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9');
create table tplain (en digit, data int unique);
insert into tplain select (x%10)::text::digit, x from generate_series(1,1000) x;
create table ths (mod int, data int, unique(mod, data)) partition by hash(mod);
create table ths_p1 partition of ths for values with (modulus 3, remainder 0);
create table ths_p2 partition of ths for values with (modulus 3, remainder 1);
create table ths_p3 partition of ths for values with (modulus 3, remainder 2);
insert into ths select (x%10), x from generate_series(1,1000) x;
create table tht (en digit, data int, unique(en, data)) partition by hash(en);
create table tht_p1 partition of tht for values with (modulus 3, remainder 0);
create table tht_p2 partition of tht for values with (modulus 3, remainder 1);
create table tht_p3 partition of tht for values with (modulus 3, remainder 2);
insert into tht select (x%10)::text::digit, x from generate_series(1,1000) x;
"""


def test_004_pg_dump_parallel(create_pg):
    """Parallel directory dump/restore of hash-partitioned tables, COPY+inserts."""
    src, dest1, dest2 = "regression_src", "regression_dest1", "regression_dest2"
    node = create_pg("main")
    backupdir = str(node.backup_dir)
    for db in (src, dest1, dest2):
        node.bin.run_command(["createdb", db])
    node.safe_psql(_SETUP, dbname=src)
    node.command_ok(
        [
            "pg_dump",
            "--format",
            "directory",
            "--no-sync",
            "--jobs",
            "2",
            "--file",
            backupdir + "/dump1",
            node.connstr(src),
        ],
        "parallel dump",
    )
    node.command_ok(
        [
            "pg_restore",
            "--verbose",
            "--dbname",
            node.connstr(dest1),
            "--jobs",
            "3",
            backupdir + "/dump1",
        ],
        "parallel restore",
    )
    node.command_ok(
        [
            "pg_dump",
            "--format",
            "directory",
            "--no-sync",
            "--jobs",
            "2",
            "--file",
            backupdir + "/dump2",
            "--inserts",
            node.connstr(src),
        ],
        "parallel dump as inserts",
    )
    node.command_ok(
        [
            "pg_restore",
            "--verbose",
            "--dbname",
            node.connstr(dest2),
            "--jobs",
            "3",
            backupdir + "/dump2",
        ],
        "parallel restore as inserts",
    )
