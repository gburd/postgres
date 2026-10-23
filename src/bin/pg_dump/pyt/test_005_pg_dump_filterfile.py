# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_dump/t/005_pg_dump_filterfile.pl.

Exercises the --filter file support of pg_dump, pg_dumpall and pg_restore:
include/exclude of tables, schemas, foreign data, functions, indexes, triggers,
table-and-children variants, comment/whitespace tolerance, quoted and multiline
identifiers, --strict-names interaction, and the many invalid-syntax errors.
"""

import re

import pypg


def _write_filter(path, content):
    """Write filter-file content verbatim (newlines preserved)."""
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)


def _setup_objects(node):
    """Create the tables/functions/schemas/etc. the filter tests dump."""
    node.safe_psql("CREATE FOREIGN DATA WRAPPER dummy;")
    node.safe_psql("CREATE SERVER dummyserver FOREIGN DATA WRAPPER dummy;")
    for tbl in ("table_one", "table_two", "table_three", "table_three_one", "footab"):
        node.safe_psql("CREATE TABLE {}(a varchar)".format(tbl))
    node.safe_psql("CREATE TABLE bootab() inherits (footab)")
    node.safe_psql('CREATE TABLE "strange aaa\nname"(a varchar)')
    node.safe_psql('CREATE TABLE "\nt\nt\n"(a int)')
    node.safe_psql("INSERT INTO table_one VALUES('*** TABLE ONE ***')")
    node.safe_psql("INSERT INTO table_two VALUES('*** TABLE TWO ***')")
    node.safe_psql("INSERT INTO table_three VALUES('*** TABLE THREE ***')")
    node.safe_psql("INSERT INTO table_three_one VALUES('*** TABLE THREE_ONE ***')")
    node.safe_psql("INSERT INTO bootab VALUES(10)")
    node.safe_psql("CREATE DATABASE sourcedb")
    node.safe_psql("CREATE DATABASE targetdb")
    _setup_sourcedb(node)


def _setup_sourcedb(node):
    """Create the sourcedb objects exercised by the pg_restore filter tests."""
    node.safe_psql(
        "CREATE FUNCTION foo1(a int) RETURNS int AS $$ select $1 $$ LANGUAGE sql",
        dbname="sourcedb",
    )
    node.safe_psql(
        "CREATE FUNCTION foo2(a int) RETURNS int AS $$ select $1 $$ LANGUAGE sql",
        dbname="sourcedb",
    )
    node.safe_psql(
        "CREATE FUNCTION foo3(a double precision, b int) RETURNS double precision "
        "AS $$ select $1 + $2 $$ LANGUAGE sql",
        dbname="sourcedb",
    )
    node.safe_psql(
        "CREATE FUNCTION foo_trg() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ "
        "LANGUAGE plpgsql",
        dbname="sourcedb",
    )
    node.safe_psql(
        "CREATE SCHEMA s1;\nCREATE SCHEMA s2;\n"
        "CREATE TABLE s1.t1(a int);\nCREATE SEQUENCE s1.s1;\n"
        "CREATE TABLE s2.t2(a int);\n"
        "CREATE TABLE t1(a int, b int);\nCREATE TABLE t2(a int, b int);\n"
        "CREATE INDEX t1_idx1 ON t1(a);\nCREATE INDEX t1_idx2 ON t1(b);\n"
        "CREATE TRIGGER trg1 BEFORE INSERT ON t1 EXECUTE FUNCTION foo_trg();\n"
        "CREATE TRIGGER trg2 BEFORE INSERT ON t1 EXECUTE FUNCTION foo_trg();",
        dbname="sourcedb",
    )


class _Filter:
    """Bundles a node, filter-file path, output file path and dump helpers."""

    def __init__(self, node, tempdir, plainfile):
        self.node = node
        self.port = node.port
        self.inputfile = tempdir + "/inputfile.txt"
        self.inputfile2 = tempdir + "/inputfile2.txt"
        self.plainfile = plainfile

    def write(self, content, path=None):
        """Write filter content to the (default) filter file."""
        _write_filter(path or self.inputfile, content)

    def dump_ok(self, msg, db="postgres", extra=None):
        """Run pg_dump with the current filter file; assert success."""
        cmd = [
            "pg_dump",
            "--port",
            str(self.port),
            "--file",
            self.plainfile,
            "--filter",
            self.inputfile,
        ]
        cmd += extra or []
        cmd.append(db)
        self.node.command_ok(cmd, msg)

    def dump_fails(self, pattern, msg, db="postgres"):
        """Run pg_dump with the current filter file; assert failure + stderr."""
        self.node.command_fails_like(
            [
                "pg_dump",
                "--port",
                str(self.port),
                "--file",
                self.plainfile,
                "--filter",
                self.inputfile,
                db,
            ],
            pattern,
            msg,
        )

    def slurp(self):
        """Return the dump output written to the plain file."""
        return pypg.slurp_file(self.plainfile)


def _like(dump, pattern, msg, flags=re.MULTILINE):
    """Assert dump matches pattern."""
    assert re.search(pattern, dump, flags), msg


def _unlike(dump, pattern, msg, flags=re.MULTILINE):
    """Assert dump does not match pattern."""
    assert not re.search(pattern, dump, flags), msg


def _test_basic_filters(flt):
    """Empty filter, mixed comments/whitespace, qualified names, exclusions."""
    flt.write("\n # a comment and nothing more\n\n")
    flt.dump_ok("filter file without patterns")
    dump = flt.slurp()
    for tbl in ("table_one", "table_two", "table_three", "table_three_one"):
        _like(dump, r"^CREATE TABLE public\." + tbl, "{} dumped".format(tbl))

    flt.write(
        "  include   table table_one    #comment\n"
        "include table table_two\n"
        "# skip this line\n"
        "\n"
        "\t\n"
        "  \t# another comment\n"
        "exclude table_data table_one\n"
    )
    flt.dump_ok("dump tables with filter patterns as well as comments")
    dump = flt.slurp()
    _like(dump, r"^CREATE TABLE public\.table_one", "dumped table one")
    _like(dump, r"^CREATE TABLE public\.table_two", "dumped table two")
    _unlike(dump, r"^CREATE TABLE public\.table_three", "table three not dumped")
    _unlike(
        dump, r"^CREATE TABLE public\.table_three_one", "table three_one not dumped"
    )
    _unlike(dump, r"^COPY public\.table_one", "content of table one is not included")
    _like(dump, r"^COPY public\.table_two", "content of table two is included")

    flt.write(
        "include table public.table_one\n"
        'include table "public"."table_two"\n'
        'include table "public". table_three\n'
    )
    flt.dump_ok("filter file with qualified names")
    dump = flt.slurp()
    _like(dump, r"^CREATE TABLE public\.table_one", "dumped table one")
    _like(dump, r"^CREATE TABLE public\.table_two", "dumped table two")
    _like(dump, r"^CREATE TABLE public\.table_three", "dumped table three")

    flt.write("exclude table table_one\n")
    flt.dump_ok("dump tables with exclusion of a single table")
    dump = flt.slurp()
    _unlike(dump, r"^CREATE TABLE public\.table_one", "table one not dumped")
    _like(dump, r"^CREATE TABLE public\.table_two", "dumped table two")
    _like(dump, r"^CREATE TABLE public\.table_three", "dumped table three")
    _like(dump, r"^CREATE TABLE public\.table_three_one", "dumped table three_one")

    flt.write("include table table_thre*\n")
    flt.dump_ok("dump tables with wildcard in pattern")
    dump = flt.slurp()
    _unlike(dump, r"^CREATE TABLE public\.table_one", "table one not dumped")
    _unlike(dump, r"^CREATE TABLE public\.table_two", "table two not dumped")
    _like(dump, r"^CREATE TABLE public\.table_three", "dumped table three")
    _like(dump, r"^CREATE TABLE public\.table_three_one", "dumped table three_one")


def _test_multiline_names(flt):
    """Multiline quoted table names, schema exclusion, multiple filters."""
    flt.write('include table "strange aaa\nname"')
    flt.dump_ok("dump tables with multiline names requiring quoting")
    _like(
        flt.slurp(),
        r"^CREATE TABLE public.\"strange aaa",
        "dump table with new line in name",
    )

    flt.write('exclude table "strange aaa\\nname"')
    flt.dump_ok("dump tables with filter")
    _unlike(
        flt.slurp(),
        r"^CREATE TABLE public.\"strange aaa",
        "exclude table with new line in name",
    )

    flt.write("exclude schema public\n")
    flt.dump_ok("exclude the public schema")
    _unlike(flt.slurp(), r"^CREATE TABLE", "no table dumped")

    flt.write("include schema public\n")
    flt.write("exclude schema public\n", path=flt.inputfile2)
    flt.node.command_ok(
        [
            "pg_dump",
            "--port",
            str(flt.port),
            "--file",
            flt.plainfile,
            "--filter",
            flt.inputfile,
            "--filter",
            flt.inputfile2,
            "postgres",
        ],
        "exclude the public schema with multiple filters",
    )
    _unlike(flt.slurp(), r"^CREATE TABLE", "no table dumped")

    flt.write('include table "\nt\nt\n"')
    flt.dump_ok("dump tables with multiline leading-newline name")
    _like(
        flt.slurp(),
        r"^CREATE TABLE public.\"\nt\nt\n\" \($",
        "dump table with multiline strange name",
        flags=re.MULTILINE | re.DOTALL,
    )

    flt.write('include table "\\nt\\nt\\n"')
    flt.dump_ok("dump tables with escaped multiline name")
    _like(
        flt.slurp(),
        r"^CREATE TABLE public.\"\nt\nt\n\" \($",
        "dump table with multiline strange name",
        flags=re.MULTILINE | re.DOTALL,
    )


def _test_foreign_data_and_syntax(flt):
    """Foreign-data filters and the invalid-syntax dump errors."""
    flt.write("include foreign_data doesnt_exists\n")
    flt.dump_fails(
        r"pg_dump: error: no matching foreign servers were found for pattern",
        "dump nonexisting foreign server",
    )

    flt.write("include foreign_data dummyserver\n")
    flt.dump_ok("dump foreign_data with filter")
    _like(flt.slurp(), r"^CREATE SERVER dummyserver", "dump foreign server")

    flt.write("exclude foreign_data dummy*\n")
    flt.dump_fails(
        r'exclude filter for "foreign data" is not allowed',
        "erroneously exclude foreign server",
    )

    flt.write("k")
    flt.dump_fails(r"invalid filter command", "invalid syntax: incorrect command")

    flt.write("exclude table-data one")
    flt.dump_fails(
        r'unsupported filter object type: "table-data"',
        "invalid syntax: invalid object type specified",
    )

    flt.write("include table")
    flt.dump_fails(r"missing object name", "invalid syntax: missing pattern")

    flt.write("include table table one")
    flt.dump_fails(
        r"no matching tables were found",
        "invalid syntax: extra content after pattern",
    )


def _test_strict_names(flt):
    """--strict-names with matching and non-matching patterns."""
    flt.write("include table table_one\n")
    flt.dump_ok("strict names with matching pattern", extra=["--strict-names"])
    _like(flt.slurp(), r"^CREATE TABLE public\.table_one", "table one dumped")

    with open(flt.inputfile, "a", encoding="utf-8") as fh:
        fh.write("include table table_nonexisting_name")
    flt.node.command_fails_like(
        [
            "pg_dump",
            "--port",
            str(flt.port),
            "--file",
            flt.plainfile,
            "--filter",
            flt.inputfile,
            "--strict-names",
            "postgres",
        ],
        r"no matching tables were found",
        "inclusion of non-existing objects with --strict names",
    )


def _test_pg_dumpall(flt):
    """pg_dumpall --filter database exclusion and invalid-syntax errors."""
    node = flt.node

    def _dumpall_ok(msg, extra=None):
        node.command_ok(
            [
                "pg_dumpall",
                "--port",
                str(flt.port),
                "--file",
                flt.plainfile,
                "--filter",
                flt.inputfile,
            ]
            + (extra or []),
            msg,
        )

    def _dumpall_fails(pattern, msg, extra=None):
        node.command_fails_like(
            [
                "pg_dumpall",
                "--port",
                str(flt.port),
                "--file",
                flt.plainfile,
                "--filter",
                flt.inputfile,
            ]
            + (extra or []),
            pattern,
            msg,
        )

    flt.write("exclude database postgres\n")
    _dumpall_ok("dump tables with exclusion of a database")
    dump = flt.slurp()
    _unlike(dump, r"^\\connect postgres", "database postgres is not dumped")
    _like(dump, r"^\\connect template1", "database template1 is dumped")
    _dumpall_fails(
        r"pg_dumpall: error: options --exclude-database and -g/--globals-only "
        r"cannot be used together",
        "pg_dumpall: --exclude-database and --globals-only cannot be used together",
        extra=["--globals-only"],
    )

    flt.write("k")
    _dumpall_fails(r"invalid filter command", "invalid syntax: incorrect command")
    flt.write("exclude xxx")
    _dumpall_fails(
        r'unsupported filter object type: "xxx"',
        "invalid syntax: exclusion of non-existing object type",
    )
    flt.write("exclude table foo")
    _dumpall_fails(
        r"pg_dumpall: error: invalid format in filter",
        "invalid syntax: exclusion of unsupported object type",
    )


def _restore_ok(flt, dumpfile, msg, fmt="custom"):
    """Run pg_restore with the current filter file; assert success."""
    cmd = [
        "pg_restore",
        "--port",
        str(flt.port),
        "--file",
        flt.plainfile,
        "--filter",
        flt.inputfile,
    ]
    if fmt:
        cmd += ["--format", fmt, dumpfile]
    flt.node.command_ok(cmd, msg)


def _restore_fails(flt, pattern, msg):
    """Run pg_restore with the current filter file; assert failure + stderr."""
    flt.node.command_fails_like(
        [
            "pg_restore",
            "--port",
            str(flt.port),
            "--file",
            flt.plainfile,
            "--filter",
            flt.inputfile,
        ],
        pattern,
        msg,
    )


def _test_pg_restore_tables(flt, tempdir):
    """pg_restore --filter table inclusion plus the disallowed-object errors."""
    node = flt.node
    dumpfile = tempdir + "/filter_test.dump"
    node.command_ok(
        [
            "pg_dump",
            "--port",
            str(flt.port),
            "--file",
            dumpfile,
            "--format",
            "custom",
            "postgres",
        ],
        "dump all tables",
    )
    flt.write("include table table_two")
    _restore_ok(flt, dumpfile, "restore tables with filter")
    dump = flt.slurp()
    _like(dump, r"^CREATE TABLE public\.table_two", "wanted table restored")
    _unlike(dump, r"^CREATE TABLE public\.table_one", "unwanted table not restored")

    for content, obj in (
        ("include table_data xxx", "table data"),
        ("include extension xxx", "extension"),
    ):
        flt.write(content)
        _restore_fails(
            flt,
            r'include filter for "{}" is not allowed'.format(obj),
            "invalid syntax: inclusion of unallowed object",
        )
    for content, obj in (
        ("exclude extension xxx", "extension"),
        ("exclude table_data xxx", "table data"),
    ):
        flt.write(content)
        _restore_fails(
            flt,
            r'exclude filter for "{}" is not allowed'.format(obj),
            "invalid syntax: exclusion of unallowed object",
        )


def _test_pg_restore_objects(flt, tempdir):
    """pg_restore --filter for functions, indexes, triggers and schemas."""
    node = flt.node
    dumpfile = tempdir + "/filter_test.dump"
    node.command_ok(
        [
            "pg_dump",
            "--port",
            str(flt.port),
            "--file",
            dumpfile,
            "--format",
            "custom",
            "sourcedb",
        ],
        "dump all objects from sourcedb",
    )

    flt.write("include function foo1(integer)")
    _restore_ok(flt, dumpfile, "restore function with filter")
    dump = flt.slurp()
    _like(dump, r"^CREATE FUNCTION public\.foo1", "wanted function restored")
    _unlike(dump, r"^CREATE TABLE public\.foo2", "unwanted function not restored")

    flt.write("include function  foo3 ( double  precision ,   integer)  ")
    _restore_ok(flt, dumpfile, "restore function with whitespace-tolerant filter")
    _like(flt.slurp(), r"^CREATE FUNCTION public\.foo3", "wanted function restored")

    flt.write("include index t1_idx1\ninclude trigger t1 trg1\n")
    _restore_ok(flt, dumpfile, "restore index/trigger with filter")
    dump = flt.slurp()
    _like(dump, r"^CREATE INDEX t1_idx1", "wanted index restored")
    _unlike(dump, r"^CREATE INDEX t2_idx2", "unwanted index not restored")
    _like(dump, r"^CREATE TRIGGER trg1", "wanted trigger restored")
    _unlike(dump, r"^CREATE TRIGGER trg2", "unwanted trigger not restored")

    flt.write("include schema s1\n")
    _restore_ok(flt, dumpfile, "restore schema with filter")
    dump = flt.slurp()
    _like(dump, r"^CREATE TABLE s1\.t1", "wanted table from schema restored")
    _like(dump, r"^CREATE SEQUENCE s1\.s1", "wanted sequence from schema restored")
    _unlike(dump, r"^CREATE TABLE s2\t2", "unwanted table not restored")

    flt.write("exclude schema s1\n")
    _restore_ok(flt, dumpfile, "restore with schema exclusion filter")
    dump = flt.slurp()
    _unlike(dump, r"^CREATE TABLE s1\.t1", "unwanted table from schema not restored")
    _unlike(dump, r"^CREATE SEQUENCE s1\.s1", "unwanted sequence not restored")
    _like(dump, r"^CREATE TABLE s2\.t2", "wanted table restored")
    _like(dump, r"^CREATE TABLE public\.t1", "wanted table restored")


def _test_table_and_children(flt):
    """table_and_children / table_data_and_children filters and extensions."""
    flt.write("include table_and_children footab\n")
    flt.dump_ok("filter table_and_children include")
    _like(flt.slurp(), r"^CREATE TABLE public\.bootab", "dumped children table")

    flt.write("exclude table_and_children footab\n")
    flt.dump_ok("filter table_and_children exclude")
    _unlike(flt.slurp(), r"^CREATE TABLE public\.bootab", "exclude children table")

    flt.write("exclude table_data_and_children footab\n")
    flt.dump_ok("filter table_data_and_children exclude")
    dump = flt.slurp()
    _like(dump, r"^CREATE TABLE public\.bootab", "dumped children table")
    _unlike(dump, r"^COPY public\.bootab", "exclude children table data")

    flt.write("include extension doesnt_exists\n")
    flt.dump_fails(
        r"pg_dump: error: no matching extensions were found",
        "dump nonexisting extension",
    )


def test_005_pg_dump_filterfile(create_pg, tmp_path):
    """pg_dump/pg_dumpall/pg_restore --filter file behavior and errors."""
    tempdir = str(tmp_path)
    node = create_pg("main")
    backupdir = str(node.backup_dir)
    plainfile = backupdir + "/plain.sql"

    _setup_objects(node)

    flt = _Filter(node, tempdir, plainfile)
    _test_basic_filters(flt)
    _test_multiline_names(flt)
    _test_foreign_data_and_syntax(flt)
    _test_strict_names(flt)
    _test_pg_dumpall(flt)
    _test_pg_restore_tables(flt, tempdir)
    _test_pg_restore_objects(flt, tempdir)
    _test_table_and_children(flt)
