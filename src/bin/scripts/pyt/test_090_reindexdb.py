# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/scripts/t/090_reindexdb.pl."""

# Save the relfilenodes of a set of indexes and compare them across REINDEX.
_FETCH_TOAST = (
    "SELECT b.oid::regclass, c.oid::regclass::text, c.oid, c.relfilenode"
    " FROM pg_class a"
    " JOIN pg_class b ON (a.oid = b.reltoastrelid)"
    " JOIN pg_index i on (a.oid = i.indrelid)"
    " JOIN pg_class c on (i.indexrelid = c.oid)"
    " WHERE b.oid IN ('pg_constraint'::regclass, 'test1'::regclass)"
)
_FETCH_INDEX = (
    "SELECT i.indrelid, a.oid::regclass::text, a.oid, a.relfilenode"
    " FROM pg_class a"
    " JOIN pg_index i ON (i.indexrelid = a.oid)"
    " WHERE a.relname IN ('pg_constraint_oid_index', 'test1x')"
)
_SAVE_RELFILENODES = "INSERT INTO index_relfilenodes {};".format(
    _FETCH_TOAST
) + "INSERT INTO index_relfilenodes {};".format(_FETCH_INDEX)
_COMPARE_RELFILENODES = (
    "SELECT b.parent::regclass,"
    " regexp_replace(b.indname::text, '(pg_toast.pg_toast_)\\d+(_index)',"
    " '\\1<oid>\\2'),"
    " CASE WHEN a.oid = b.indoid THEN 'OID is unchanged'"
    " ELSE 'OID has changed' END,"
    " CASE WHEN a.relfilenode = b.relfilenode THEN 'relfilenode is unchanged'"
    " ELSE 'relfilenode has changed' END"
    " FROM index_relfilenodes b"
    " JOIN pg_class a ON b.indname::text = a.oid::regclass::text"
    " ORDER BY b.parent::text, b.indname::text"
)


def test_reindexdb(pg_bin, create_pg, tmp_path, monkeypatch):
    """reindexdb across object types, tablespaces, --concurrently and --jobs."""
    pg_bin.program_help_ok("reindexdb")
    pg_bin.program_version_ok("reindexdb")
    pg_bin.program_options_handling_ok("reindexdb")

    node = create_pg("main")
    monkeypatch.setenv("PGOPTIONS", "--client-min-messages=WARNING")

    tbspace_path = tmp_path / "regress_reindex_tbspace"
    tbspace_path.mkdir()
    tbspace_name = "reindex_tbspace"
    node.safe_psql(
        "CREATE TABLESPACE {} LOCATION '{}';".format(tbspace_name, tbspace_path)
    )

    # Use text as data type to get a toast table.
    node.safe_psql("CREATE TABLE test1 (a text); CREATE INDEX test1x ON test1 (a);")
    toast_table = node.safe_psql(
        "SELECT reltoastrelid::regclass FROM pg_class WHERE oid = 'test1'::regclass;"
    )
    toast_index = node.safe_psql(
        "SELECT indexrelid::regclass FROM pg_index "
        "WHERE indrelid = '{}'::regclass;".format(toast_table)
    )

    node.safe_psql(
        "CREATE TABLE index_relfilenodes "
        "(parent regclass, indname text, indoid oid, relfilenode oid);"
    )

    node.safe_psql(_SAVE_RELFILENODES)
    node.issues_sql_like(
        ["reindexdb", "postgres"],
        r"statement: REINDEX DATABASE postgres;",
        "SQL REINDEX run",
    )
    assert node.safe_psql(_COMPARE_RELFILENODES) == (
        "pg_constraint|pg_constraint_oid_index|OID is unchanged|"
        "relfilenode is unchanged\n"
        "pg_constraint|pg_toast.pg_toast_<oid>_index|OID is unchanged|"
        "relfilenode is unchanged\n"
        "test1|pg_toast.pg_toast_<oid>_index|OID is unchanged|"
        "relfilenode has changed\n"
        "test1|test1x|OID is unchanged|relfilenode has changed"
    ), "relfilenode change after REINDEX DATABASE"

    node.safe_psql("TRUNCATE index_relfilenodes; " + _SAVE_RELFILENODES)
    node.issues_sql_like(
        ["reindexdb", "--system", "postgres"],
        r"statement: REINDEX SYSTEM postgres;",
        "reindex system tables",
    )
    assert node.safe_psql(_COMPARE_RELFILENODES) == (
        "pg_constraint|pg_constraint_oid_index|OID is unchanged|"
        "relfilenode has changed\n"
        "pg_constraint|pg_toast.pg_toast_<oid>_index|OID is unchanged|"
        "relfilenode has changed\n"
        "test1|pg_toast.pg_toast_<oid>_index|OID is unchanged|"
        "relfilenode is unchanged\n"
        "test1|test1x|OID is unchanged|relfilenode is unchanged"
    ), "relfilenode change after REINDEX SYSTEM"

    node.issues_sql_like(
        ["reindexdb", "--table", "test1", "postgres"],
        r"statement: REINDEX TABLE public\.test1;",
        "reindex specific table",
    )
    node.issues_sql_like(
        ["reindexdb", "--table", "test1", "--tablespace", tbspace_name, "postgres"],
        r"statement: REINDEX \(TABLESPACE {}\) TABLE public\.test1;".format(
            tbspace_name
        ),
        "reindex specific table on tablespace",
    )
    node.issues_sql_like(
        ["reindexdb", "--index", "test1x", "postgres"],
        r"statement: REINDEX INDEX public\.test1x;",
        "reindex specific index",
    )
    node.issues_sql_like(
        ["reindexdb", "--schema", "pg_catalog", "postgres"],
        r"statement: REINDEX SCHEMA pg_catalog;",
        "reindex specific schema",
    )
    node.issues_sql_like(
        ["reindexdb", "--verbose", "--table", "test1", "postgres"],
        r"statement: REINDEX \(VERBOSE\) TABLE public\.test1;",
        "reindex with verbose output",
    )
    node.issues_sql_like(
        [
            "reindexdb",
            "--verbose",
            "--table",
            "test1",
            "--tablespace",
            tbspace_name,
            "postgres",
        ],
        r"statement: REINDEX \(VERBOSE, TABLESPACE {}\) TABLE public\.test1;".format(
            tbspace_name
        ),
        "reindex with verbose output and tablespace",
    )

    _test_concurrently(node, tbspace_name, toast_table, toast_index)
    _test_connstr_and_parallel(node)


def _test_concurrently(node, tbspace_name, toast_table, toast_index):
    node.safe_psql("TRUNCATE index_relfilenodes; " + _SAVE_RELFILENODES)
    node.issues_sql_like(
        ["reindexdb", "--concurrently", "postgres"],
        r"statement: REINDEX DATABASE CONCURRENTLY postgres;",
        "SQL REINDEX CONCURRENTLY run",
    )
    assert node.safe_psql(_COMPARE_RELFILENODES) == (
        "pg_constraint|pg_constraint_oid_index|OID is unchanged|"
        "relfilenode is unchanged\n"
        "pg_constraint|pg_toast.pg_toast_<oid>_index|OID is unchanged|"
        "relfilenode is unchanged\n"
        "test1|pg_toast.pg_toast_<oid>_index|OID has changed|"
        "relfilenode has changed\n"
        "test1|test1x|OID has changed|relfilenode has changed"
    ), "OID change after REINDEX DATABASE CONCURRENTLY"

    node.issues_sql_like(
        ["reindexdb", "--concurrently", "--table", "test1", "postgres"],
        r"statement: REINDEX TABLE CONCURRENTLY public\.test1;",
        "reindex specific table concurrently",
    )
    node.issues_sql_like(
        ["reindexdb", "--concurrently", "--index", "test1x", "postgres"],
        r"statement: REINDEX INDEX CONCURRENTLY public\.test1x;",
        "reindex specific index concurrently",
    )
    node.issues_sql_like(
        ["reindexdb", "--concurrently", "--schema", "public", "postgres"],
        r"statement: REINDEX SCHEMA CONCURRENTLY public;",
        "reindex specific schema concurrently",
    )
    node.command_fails(
        ["reindexdb", "--concurrently", "--system", "postgres"],
        "reindex system tables concurrently",
    )
    node.issues_sql_like(
        ["reindexdb", "--concurrently", "--verbose", "--table", "test1", "postgres"],
        r"statement: REINDEX \(VERBOSE\) TABLE CONCURRENTLY public\.test1;",
        "reindex with verbose output concurrently",
    )
    node.issues_sql_like(
        [
            "reindexdb",
            "--concurrently",
            "--verbose",
            "--table",
            "test1",
            "--tablespace",
            tbspace_name,
            "postgres",
        ],
        r"statement: REINDEX \(VERBOSE, TABLESPACE {}\) TABLE CONCURRENTLY "
        r"public\.test1;".format(tbspace_name),
        "reindex concurrently with verbose output and tablespace",
    )

    # REINDEX TABLESPACE on toast indexes and tables fails.
    for args, what in (
        (["--table", toast_table], "reindex toast table with tablespace"),
        (
            ["--concurrently", "--table", toast_table],
            "reindex toast table concurrently with tablespace",
        ),
        (["--index", toast_index], "reindex toast index with tablespace"),
        (
            ["--concurrently", "--index", toast_index],
            "reindex toast index concurrently with tablespace",
        ),
    ):
        node.command_checks_all(
            ["reindexdb"] + args + ["--tablespace", tbspace_name, "postgres"],
            1,
            [],
            [r"cannot move system relation"],
            what,
        )


def _test_connstr_and_parallel(node):
    node.command_ok(
        ["reindexdb", "--echo", "--table=pg_am", "dbname=template1"],
        "reindexdb table with connection string",
    )
    node.command_ok(
        ["reindexdb", "--echo", "dbname=template1"],
        "reindexdb database with connection string",
    )
    node.command_ok(
        ["reindexdb", "--echo", "--system", "dbname=template1"],
        "reindexdb system with connection string",
    )

    node.safe_psql(
        "CREATE SCHEMA s1;"
        " CREATE TABLE s1.t1(id integer);"
        " CREATE INDEX ON s1.t1(id);"
        " CREATE INDEX i1 ON s1.t1(id);"
        " CREATE SCHEMA s2;"
        " CREATE TABLE s2.t2(id integer);"
        " CREATE INDEX ON s2.t2(id);"
        " CREATE INDEX i2 ON s2.t2(id);"
        " CREATE SCHEMA s3;"
    )

    node.command_fails(
        ["reindexdb", "--jobs", "2", "--system", "postgres"],
        "parallel reindexdb cannot process system catalogs",
    )
    node.command_ok(
        [
            "reindexdb",
            "--jobs",
            "2",
            "--index",
            "s1.i1",
            "--index",
            "s2.i2",
            "--index",
            "s1.t1_id_idx",
            "--index",
            "s2.t2_id_idx",
            "postgres",
        ],
        "parallel reindexdb for indices",
    )
    node.issues_sql_like(
        ["reindexdb", "--jobs", "2", "--schema", "s1", "--schema", "s2", "postgres"],
        r"statement: REINDEX TABLE s1.t1;",
        "parallel reindexdb for schemas does a per-table REINDEX",
    )
    node.command_ok(
        ["reindexdb", "--jobs", "2", "--schema", "s3"],
        "parallel reindexdb with empty schema",
    )
    node.command_ok(
        ["reindexdb", "--jobs", "2", "--concurrently", "--dbname", "postgres"],
        "parallel reindexdb on database, concurrently",
    )

    # Combinations of objects.
    node.issues_sql_like(
        ["reindexdb", "--system", "--table", "test1", "postgres"],
        r"statement: REINDEX SYSTEM postgres;",
        "specify both --system and --table",
    )
    node.issues_sql_like(
        ["reindexdb", "--system", "--index", "test1x", "postgres"],
        r"statement: REINDEX INDEX public.test1x;",
        "specify both --system and --index",
    )
    node.issues_sql_like(
        ["reindexdb", "--system", "--schema", "pg_catalog", "postgres"],
        r"statement: REINDEX SCHEMA pg_catalog;",
        "specify both --system and --schema",
    )
