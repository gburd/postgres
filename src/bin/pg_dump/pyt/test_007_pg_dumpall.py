# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_dump/t/007_pg_dumpall.pl.

pg_dumpall in directory/tar/custom formats round-trips roles, tablespaces,
grants, multiple databases (with --exclude-database), globals handling, and the
map.dat database listing; pg_restore -C reproduces the expected SQL. Also covers
the many pg_restore option-combination errors that apply only to pg_dumpall
archives, and that --clean implies --if-exists.
"""

import os
import re
from typing import Dict, List, Optional, Tuple

import pypg

_RUN_DB = "postgres"


def _q(*literals):
    """Join re.escape'd literal segments (the \\Q...\\E parts of a /x regex)."""
    return "".join(re.escape(lit) for lit in literals)


def _setup_sql():
    """Per-run setup SQL keyed by run name (executed before any test)."""
    return {
        "restore_roles": (
            "CREATE ROLE dumpall WITH ENCRYPTED PASSWORD 'admin' SUPERUSER;\n"
            "CREATE ROLE dumpall2 WITH REPLICATION CONNECTION LIMIT 10;"
        ),
        "restore_grants": (
            "CREATE DATABASE tapgrantsdb;\n"
            "CREATE SCHEMA private;\n"
            "CREATE SEQUENCE serial START 101;\n"
            "CREATE FUNCTION fn() RETURNS void AS $$\n"
            "BEGIN\n"
            "END;\n"
            "$$ LANGUAGE plpgsql;\n"
            "CREATE ROLE super;\n"
            "CREATE ROLE grant1;\n"
            "CREATE ROLE grant2;\n"
            "CREATE ROLE grant3;\n"
            "CREATE ROLE grant4;\n"
            "CREATE ROLE grant5;\n"
            "CREATE ROLE grant6;\n"
            "CREATE ROLE grant7;\n"
            "CREATE ROLE grant8;\n"
            "CREATE TABLE t (id int);\n"
            "INSERT INTO t VALUES (1), (2), (3), (4);\n"
            "GRANT SELECT ON TABLE t TO grant1;\n"
            "GRANT INSERT ON TABLE t TO grant2;\n"
            "GRANT ALL PRIVILEGES ON TABLE t to grant3;\n"
            "GRANT CONNECT, CREATE ON DATABASE tapgrantsdb TO grant4;\n"
            "GRANT USAGE, CREATE ON SCHEMA private TO grant5;\n"
            "GRANT USAGE, SELECT, UPDATE ON SEQUENCE serial TO grant6;\n"
            "GRANT super TO grant7;\n"
            "GRANT EXECUTE ON FUNCTION fn() TO grant8;"
        ),
        "excluding_databases": _EXCLUDING_DBS_SQL,
        "format_directory": (
            "CREATE TABLE format_directory(a int, b boolean, c text);\n"
            "INSERT INTO format_directory VALUES (1, true, 'name1'), "
            "(2, false, 'name2');"
        ),
        "format_tar": (
            "CREATE TABLE format_tar(a int, b boolean, c text);\n"
            "INSERT INTO format_tar VALUES (1, false, 'name3'), (2, true, 'name4');"
        ),
        "format_custom": (
            "CREATE TABLE format_custom(a int, b boolean, c text);\n"
            "INSERT INTO format_custom VALUES (1, false, 'name5'), "
            "(2, true, 'name6');"
        ),
        "dump_globals_only": (
            "CREATE TABLE format_dir(a int, b boolean, c text);\n"
            "INSERT INTO format_dir VALUES (1, false, 'name5'), (2, true, 'name6');"
        ),
        "restore_no_globals": (
            "CREATE TABLE no_globals_test(a int, b text);\n"
            "INSERT INTO no_globals_test VALUES (1, 'hello'), (2, 'world');"
        ),
    }


_EXCLUDING_DBS_SQL = """\
CREATE DATABASE db1;
\\c db1
CREATE TABLE t1 (id int);
INSERT INTO t1 VALUES (1), (2), (3), (4);
CREATE TABLE t2 (id int);
INSERT INTO t2 VALUES (1), (2), (3), (4);

CREATE DATABASE db2;
\\c db2
CREATE TABLE t3 (id int);
INSERT INTO t3 VALUES (1), (2), (3), (4);
CREATE TABLE t4 (id int);
INSERT INTO t4 VALUES (1), (2), (3), (4);

CREATE DATABASE dbex3;
\\c dbex3
CREATE TABLE t5 (id int);
INSERT INTO t5 VALUES (1), (2), (3), (4);
CREATE TABLE t6 (id int);
INSERT INTO t6 VALUES (1), (2), (3), (4);

CREATE DATABASE dbex4;
\\c dbex4
CREATE TABLE t7 (id int);
INSERT INTO t7 VALUES (1), (2), (3), (4);
CREATE TABLE t8 (id int);
INSERT INTO t8 VALUES (1), (2), (3), (4);

CREATE DATABASE db5;
\\c db5
CREATE TABLE t9 (id int);
INSERT INTO t9 VALUES (1), (2), (3), (4);
CREATE TABLE t10 (id int);
INSERT INTO t10 VALUES (1), (2), (3), (4);
"""

_ANY = r"(.*\n)*"


def _build_runs(tempdir, tablespace1, tablespace2):
    """Return the ordered (name, dump_cmd, restore_cmd, like, unlike) runs."""
    runs: Dict[
        str,
        Tuple[
            List[str],
            List[str],
            Optional["re.Pattern[str]"],
            Optional["re.Pattern[str]"],
        ],
    ] = {}
    runs["restore_roles"] = (
        ["pg_dumpall", "--format", "directory", "--file", tempdir + "/restore_roles"],
        [
            "pg_restore",
            "-C",
            "--format",
            "directory",
            "--file",
            tempdir + "/restore_roles.sql",
            tempdir + "/restore_roles",
        ],
        re.compile(
            r"\s*"
            + _q("CREATE ROLE dumpall2;")
            + r"\s*"
            + _q(
                "ALTER ROLE dumpall2 WITH NOSUPERUSER INHERIT NOCREATEROLE "
                "NOCREATEDB NOLOGIN REPLICATION NOBYPASSRLS CONNECTION LIMIT 10;"
            ),
            re.MULTILINE,
        ),
        None,
    )
    runs["restore_tablespace"] = (
        [
            "pg_dumpall",
            "--format",
            "directory",
            "--file",
            tempdir + "/restore_tablespace",
        ],
        [
            "pg_restore",
            "-C",
            "--format",
            "directory",
            "--file",
            tempdir + "/restore_tablespace.sql",
            tempdir + "/restore_tablespace",
        ],
        re.compile(
            r"^\n"
            + _q("CREATE TABLESPACE tbl2 OWNER tap LOCATION ")
            + r"(?:E)?"
            + _q("'{}';".format(tablespace2))
            + r"\n"
            + _q("ALTER TABLESPACE tbl2 SET (seq_page_cost=1.0);"),
            re.MULTILINE,
        ),
        None,
    )
    runs["restore_grants"] = (
        ["pg_dumpall", "--format", "directory", "--file", tempdir + "/restore_grants"],
        [
            "pg_restore",
            "-C",
            "--format",
            "directory",
            "--file",
            tempdir + "/restore_grants.sql",
            tempdir + "/restore_grants",
        ],
        re.compile(_GRANTS_LIKE, re.MULTILINE),
        None,
    )
    runs["excluding_databases"] = _excluding_databases_run(tempdir)
    for fmt, name in (
        ("directory", "format_directory"),
        ("tar", "format_tar"),
        ("custom", "format_custom"),
    ):
        runs[name] = _format_run(tempdir, fmt, name)
    runs["dump_globals_only"] = (
        [
            "pg_dumpall",
            "--format",
            "directory",
            "--globals-only",
            "--file",
            tempdir + "/dump_globals_only",
        ],
        [
            "pg_restore",
            "-C",
            "--globals-only",
            "--format",
            "directory",
            "--file",
            tempdir + "/dump_globals_only.sql",
            tempdir + "/dump_globals_only",
        ],
        re.compile(r"^\s*" + _q("CREATE ROLE dumpall;") + r"\s*\n", re.MULTILINE),
        None,
    )
    runs["restore_no_globals"] = (
        [
            "pg_dumpall",
            "--format",
            "directory",
            "--file",
            tempdir + "/restore_no_globals",
        ],
        [
            "pg_restore",
            "-C",
            "--no-globals",
            "--format",
            "directory",
            "--file",
            tempdir + "/restore_no_globals.sql",
            tempdir + "/restore_no_globals",
        ],
        re.compile(
            r"^\n" + _q("COPY public.no_globals_test (a, b) FROM stdin;"),
            re.MULTILINE,
        ),
        re.compile(r"^" + _q("CREATE ROLE dumpall;"), re.MULTILINE),
    )
    return runs


_GRANTS_LIKE = (
    r"^\n"
    + _q("GRANT ALL ON SCHEMA private TO grant5;")
    + _ANY
    + r"\n"
    + _q("GRANT ALL ON FUNCTION public.fn() TO grant8;")
    + _ANY
    + r"\n"
    + _q("GRANT ALL ON SEQUENCE public.serial TO grant6;")
    + _ANY
    + r"\n"
    + _q("GRANT SELECT ON TABLE public.t TO grant1;")
    + r"\n"
    + _q("GRANT INSERT ON TABLE public.t TO grant2;")
    + r"\n"
    + _q("GRANT ALL ON TABLE public.t TO grant3;")
    + _ANY
    + r"\n"
    + _q("GRANT CREATE,CONNECT ON DATABASE tapgrantsdb TO grant4;")
)


def _excluding_databases_run(tempdir):
    """The excluding_databases run tuple (dump/restore cmds + like/unlike)."""
    like = re.compile(
        r"^\n"
        + _q("CREATE DATABASE db1")
        + _ANY
        + r"\n"
        + _q("CREATE TABLE public.t1 (")
        + _ANY
        + r"\n"
        + _q("CREATE TABLE public.t2 (")
        + _ANY
        + r"\n"
        + _q("CREATE DATABASE db2")
        + _ANY
        + r"\n"
        + _q("CREATE TABLE public.t3 (")
        + _ANY
        + r"\n"
        + _q("CREATE TABLE public.t4 ("),
        re.MULTILINE,
    )
    unlike = re.compile(
        r"^\n"
        + _q("CREATE DATABASE db3")
        + _ANY
        + r"\n"
        + _q("CREATE TABLE public.t5 (")
        + _ANY
        + r"\n"
        + _q("CREATE TABLE public.t6 (")
        + _ANY
        + r"\n"
        + _q("CREATE DATABASE db4")
        + _ANY
        + r"\n"
        + _q("CREATE TABLE public.t7 (")
        + _ANY
        + r"\n"
        + _q("CREATE TABLE public.t8 (")
        + r"\n"
        + _q("CREATE DATABASE db5")
        + _ANY
        + r"\n"
        + _q("CREATE TABLE public.t9 (")
        + _ANY
        + r"\n"
        + _q("CREATE TABLE public.t10 ("),
        re.MULTILINE,
    )
    return (
        [
            "pg_dumpall",
            "--format",
            "directory",
            "--file",
            tempdir + "/excluding_databases",
            "--exclude-database",
            "dbex*",
        ],
        [
            "pg_restore",
            "-C",
            "--format",
            "directory",
            "--file",
            tempdir + "/excluding_databases.sql",
            "--exclude-database",
            "db5",
            tempdir + "/excluding_databases",
        ],
        like,
        unlike,
    )


def _format_run(tempdir, fmt, name):
    """A format_directory/tar/custom run tuple, asserting the COPY line."""
    table = name
    return (
        ["pg_dumpall", "--format", fmt, "--file", tempdir + "/" + name],
        [
            "pg_restore",
            "-C",
            "--format",
            fmt,
            "--file",
            tempdir + "/" + name + ".sql",
            tempdir + "/" + name,
        ],
        re.compile(
            r"^\n" + _q("COPY public.{} (a, b, c) FROM stdin;".format(table)),
            re.MULTILINE,
        ),
        None,
    )


def _run_dump_restore_cases(node, create_pg, tempdir, runs):
    """Execute every dump/restore run and assert its like/unlike patterns."""
    for run in sorted(runs):
        setup = _setup_sql().get(run)
        if setup:
            node.safe_psql(setup, dbname=_RUN_DB)
    for run in sorted(runs):
        dump_cmd, restore_cmd, like, unlike = runs[run]
        target = create_pg("target_" + run)
        node.command_ok(dump_cmd, "{}: pg_dumpall runs".format(run))
        node.bin.run_command(
            restore_cmd + ["--host", str(target.host), "--port", str(target.port)]
        )
        output = pypg.slurp_file("{}/{}.sql".format(tempdir, run))
        assert like or unlike, 'missing "like" or "unlike" in test "{}"'.format(run)
        if like:
            assert like.search(output), "should dump {}".format(run)
        if unlike:
            assert not unlike.search(output), "should not dump {}".format(run)
        target.stop()
        target.clean_node()


_ERR_PREFIX = "pg_restore: error: "
_PG_DUMPALL_ERRORS = [
    (
        [],
        _ERR_PREFIX + "option -C/--create must be specified when restoring an archive "
        "created by pg_dumpall",
        "When -C is not used in pg_restore with dump of pg_dumpall",
    ),
    (
        ["-C", "--list"],
        _ERR_PREFIX + "option -l/--list cannot be used when restoring an archive "
        "created by pg_dumpall",
        "When --list is used in pg_restore with dump of pg_dumpall",
    ),
    (
        ["-C", "--use-list", "use"],
        _ERR_PREFIX + "option -L/--use-list cannot be used when restoring an archive "
        "created by pg_dumpall",
        "When -L/--use-list is used in pg_restore with dump of pg_dumpall",
    ),
    (
        ["-C", "--strict-names"],
        _ERR_PREFIX + "option --strict-names cannot be used when restoring an archive "
        "created by pg_dumpall",
        "When --strict-names is used in pg_restore with dump of pg_dumpall",
    ),
    (
        ["-C", "--clean", "--globals-only"],
        _ERR_PREFIX + "options --clean and -g/--globals-only cannot be used together "
        "when restoring an archive created by pg_dumpall",
        "When --clean and -g/--globals-only are used in pg_restore",
    ),
    (
        ["-C", "--no-schema"],
        _ERR_PREFIX + "option --no-schema cannot be used when restoring an archive "
        "created by pg_dumpall",
        "When --no-schema is used in pg_restore with dump of pg_dumpall",
    ),
    (
        ["-C", "--data-only"],
        _ERR_PREFIX + "option -a/--data-only cannot be used when restoring an archive "
        "created by pg_dumpall",
        "When --data-only is used in pg_restore with dump of pg_dumpall",
    ),
    (
        ["-C", "--statistics-only"],
        _ERR_PREFIX
        + "option --statistics-only cannot be used when restoring an archive "
        "created by pg_dumpall",
        "When --statistics-only is used in pg_restore with dump of pg_dumpall",
    ),
    (
        ["-C", "--section", "post-data"],
        _ERR_PREFIX + "option --section cannot exclude --pre-data when restoring a "
        "pg_dumpall archive",
        "When --section=post-data is used in pg_restore",
    ),
    (
        ["-C", "--globals-only", "--data-only"],
        _ERR_PREFIX
        + "options -a/--data-only and -g/--globals-only cannot be used together",
        "When --globals-only and --data-only are used together",
    ),
    (
        ["-C", "--globals-only", "--schema-only"],
        _ERR_PREFIX
        + "options -g/--globals-only and -s/--schema-only cannot be used together",
        "When --globals-only and --schema-only are used together",
    ),
    (
        ["-C", "--globals-only", "--statistics-only"],
        _ERR_PREFIX
        + "options -g/--globals-only and --statistics-only cannot be used together",
        "When --globals-only and --statistics-only are used together",
    ),
    (
        ["-C", "--globals-only", "--statistics"],
        _ERR_PREFIX
        + "options --statistics and -g/--globals-only cannot be used together",
        "When --globals-only and --statistics are used together",
    ),
    (
        ["-C", "--globals-only", "--exit-on-error"],
        _ERR_PREFIX
        + "options --exit-on-error and -g/--globals-only cannot be used together",
        "When --globals-only and --exit-on-error are used together",
    ),
    (
        ["-C", "--globals-only", "--single-transaction"],
        _ERR_PREFIX + "options -g/--globals-only and -1/--single-transaction cannot be "
        "used together",
        "When --globals-only and --single-transaction are used together",
    ),
    (
        ["-C", "--globals-only", "--transaction-size", "100"],
        _ERR_PREFIX
        + "options -g/--globals-only and --transaction-size cannot be used together",
        "When --globals-only and --transaction-size are used together",
    ),
]


def _check_pg_dumpall_errors(node, tempdir):
    """pg_restore option errors that apply only to pg_dumpall archives."""
    archive = tempdir + "/format_custom"
    for extra_opts, err, msg in _PG_DUMPALL_ERRORS:
        cmd = ["pg_restore", archive, "--format", "custom"] + extra_opts
        if "-d" not in extra_opts:
            cmd += ["--file", tempdir + "/error_test.sql"]
        node.command_fails_like(cmd, re.escape(err), msg)
    node.command_fails_like(
        ["pg_restore", archive, "-C", "--format", "custom", "-d", "dbpq"],
        re.escape('FATAL:  database "dbpq" does not exist'),
        "When non-existent database is given with -d option",
    )


def _check_map_dat_and_clean(node, create_pg, tempdir):
    """map.dat preamble, commenting out a db, and --clean implies --if-exists."""
    map_dat = pypg.slurp_file(tempdir + "/format_directory/map.dat")
    assert re.search(
        r"^# map\.dat\n.*# This file maps oids to database names",
        map_dat,
        re.DOTALL | re.MULTILINE,
    ), "map.dat contains expected preamble"

    node.safe_psql(
        "CREATE DATABASE comment_test_db;\n"
        "\\c comment_test_db\n"
        "CREATE TABLE comment_test_table (id int);",
        dbname=_RUN_DB,
    )
    node.command_ok(
        ["pg_dumpall", "--format", "directory", "--file", tempdir + "/comment_test"],
        "pg_dumpall for comment test",
    )
    map_path = tempdir + "/comment_test/map.dat"
    map_content = pypg.slurp_file(map_path)
    map_content = re.sub(
        r"^(\d+ comment_test_db)$", r"# \1", map_content, flags=re.MULTILINE
    )
    with open(map_path, "w", encoding="utf-8") as fh:
        fh.write(map_content)

    target_comment = create_pg("target_comment")
    node.command_ok(
        [
            "pg_restore",
            "-C",
            "--format",
            "directory",
            "--file",
            tempdir + "/comment_test_restore.sql",
            "--host",
            str(target_comment.host),
            "--port",
            str(target_comment.port),
            tempdir + "/comment_test",
        ],
        "pg_restore with commented out database in map.dat",
    )
    restore_output = pypg.slurp_file(tempdir + "/comment_test_restore.sql")
    assert not re.search(
        r"CREATE DATABASE comment_test_db", restore_output
    ), "commented out database in map.dat is not restored"

    node.command_ok(
        [
            "pg_restore",
            "-C",
            "--format",
            "custom",
            "--clean",
            "--file",
            tempdir + "/clean_test.sql",
            tempdir + "/format_custom",
        ],
        "pg_restore with --clean on pg_dumpall archive",
    )
    clean_output = pypg.slurp_file(tempdir + "/clean_test.sql")
    assert re.search(
        r"DROP ROLE IF EXISTS", clean_output
    ), "--clean implies --if-exists: DROP ROLE IF EXISTS in output"


def test_007_pg_dumpall(create_pg, tmp_path):
    """pg_dumpall format round-trips, exclusions, and pg_restore-only errors."""
    tempdir = str(tmp_path)
    tablespace1 = tempdir + "/tbl1"
    tablespace2 = tempdir + "/tbl2"
    os.mkdir(tablespace1)
    os.mkdir(tablespace2)

    node = create_pg("node")

    # restore_tablespace setup needs the (escaped) tablespace locations.
    node.safe_psql(
        "CREATE ROLE tap;\n"
        "CREATE TABLESPACE tbl1 OWNER tap LOCATION '{}';\n"
        "CREATE TABLESPACE tbl2 OWNER tap LOCATION '{}' "
        "WITH (seq_page_cost=1.0);".format(tablespace1, tablespace2),
        dbname=_RUN_DB,
    )

    runs = _build_runs(tempdir, tablespace1, tablespace2)
    _run_dump_restore_cases(node, create_pg, tempdir, runs)
    _check_pg_dumpall_errors(node, tempdir)
    _check_map_dat_and_clean(node, create_pg, tempdir)

    node.stop("fast")
