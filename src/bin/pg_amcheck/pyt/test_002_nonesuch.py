# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/bin/pg_amcheck/t/002_nonesuch.pl.

pg_amcheck error handling for nonexistent targets: missing databases, schemas, tables, indexes, and roles each produce the documented diagnostics and exit codes.
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_002_nonesuch(create_pg):
    """pg_amcheck diagnostics for nonexistent databases/schemas/tables/roles."""
    node = create_pg("test", auth_extra=["--create-role", "no_such_user"], start=False)
    node.start()
    node.safe_psql("CREATE EXTENSION amcheck")
    node.command_checks_all(
        ["pg_amcheck", "qqq"],
        1,
        [r"""^$"""],
        [r"""FATAL:  database "qqq" does not exist"""],
        "checking a non-existent database",
    )
    node.command_checks_all(
        ["pg_amcheck", "--database", "qqq", "--database", "postgres"],
        1,
        [r"""^$"""],
        [r'''pg_amcheck: error: no connectable databases to check matching "qqq"'''],
        "checking an unresolvable database pattern",
    )
    node.command_checks_all(
        [
            "pg_amcheck",
            "--no-strict-names",
            "--database",
            "qqq",
            "--database",
            "postgres",
        ],
        0,
        [r"""^$"""],
        [r'''pg_amcheck: warning: no connectable databases to check matching "qqq"'''],
        "checking an unresolvable database pattern under --no-strict-names",
    )
    node.command_checks_all(
        ["pg_amcheck", "--database", "post", "--database", "postgres"],
        1,
        [r"""^$"""],
        [r'''pg_amcheck: error: no connectable databases to check matching "post"'''],
        "checking an unresolvable database pattern (substring of existent database)",
    )
    node.command_checks_all(
        ["pg_amcheck", "--database", "postgresql", "--database", "postgres"],
        1,
        [r"""^$"""],
        [
            r'''pg_amcheck: error: no connectable databases to check matching "postgresql"'''
        ],
        "checking an unresolvable database pattern (superstring of existent database)",
    )
    node.command_checks_all(
        ["pg_amcheck", "--username", "no_such_user", "postgres"],
        1,
        [r"""^$"""],
        [r"""role "no_such_user" does not exist"""],
        "checking with a non-existent user",
    )
    node.command_checks_all(
        ["pg_amcheck", "template1"],
        1,
        [r"""^$"""],
        [
            r"""pg_amcheck: warning: skipping database "template1": amcheck is not installed""",
            r"""pg_amcheck: error: no relations to check""",
        ],
        "checking a database by name without amcheck installed, no other databases",
    )
    node.command_checks_all(
        ["pg_amcheck", "--database", "template1", "--database", "postgres"],
        0,
        [r"""^$"""],
        [
            r"""pg_amcheck: warning: skipping database "template1": amcheck is not installed"""
        ],
        "checking a database by name without amcheck installed, with other databases",
    )
    node.command_checks_all(
        ["pg_amcheck", "--all"],
        0,
        [r"""^$"""],
        [
            r"""pg_amcheck: warning: skipping database "template1": amcheck is not installed"""
        ],
        "checking a database by pattern without amcheck installed, with other databases",
    )
    node.command_checks_all(
        ["pg_amcheck", "--database", "postgres", "--table", ".."],
        1,
        [r"""^$"""],
        [r'''pg_amcheck: error: no connectable databases to check matching "\.\."'''],
        'checking table pattern ".."',
    )
    node.command_checks_all(
        ["pg_amcheck", "--database", "postgres", "--table", ".foo.bar"],
        1,
        [r"""^$"""],
        [
            r'''pg_amcheck: error: no connectable databases to check matching "\.foo\.bar"'''
        ],
        'checking table pattern ".foo.bar"',
    )
    node.command_checks_all(
        ["pg_amcheck", "--database", "postgres", "--table", "."],
        1,
        [r"""^$"""],
        [r'''pg_amcheck: error: no heap tables to check matching "\."'''],
        'checking table pattern "."',
    )
    node.command_checks_all(
        ["pg_amcheck", "--database", "localhost.postgres"],
        2,
        [r"""^$"""],
        [
            r"""pg_amcheck: error: improper qualified name \(too many dotted names\): localhost\.postgres"""
        ],
        "multipart database patterns are rejected",
    )
    node.command_checks_all(
        ["pg_amcheck", "--schema", "localhost.postgres.pg_catalog"],
        2,
        [r"""^$"""],
        [
            r"""pg_amcheck: error: improper qualified name \(too many dotted names\): localhost\.postgres\.pg_catalog"""
        ],
        "three part schema patterns are rejected",
    )
    node.command_checks_all(
        ["pg_amcheck", "--table", "localhost.postgres.pg_catalog.pg_class"],
        2,
        [r"""^$"""],
        [
            r"""pg_amcheck: error: improper relation name \(too many dotted names\): localhost\.postgres\.pg_catalog\.pg_class"""
        ],
        "four part table patterns are rejected",
    )
    node.command_checks_all(
        [
            "pg_amcheck",
            "--no-strict-names",
            "--table",
            "this.is.a.really.long.dotted.string",
        ],
        2,
        [r"""^$"""],
        [
            r"""pg_amcheck: error: improper relation name \(too many dotted names\): this\.is\.a\.really\.long\.dotted\.string"""
        ],
        "ungrammatical table names still draw errors under --no-strict-names",
    )
    node.command_checks_all(
        ["pg_amcheck", "--no-strict-names", "--schema", "postgres.long.dotted.string"],
        2,
        [r"""^$"""],
        [
            r"""pg_amcheck: error: improper qualified name \(too many dotted names\): postgres\.long\.dotted\.string"""
        ],
        "ungrammatical schema names still draw errors under --no-strict-names",
    )
    node.command_checks_all(
        [
            "pg_amcheck",
            "--no-strict-names",
            "--database",
            "postgres.long.dotted.string",
        ],
        2,
        [r"""^$"""],
        [
            r"""pg_amcheck: error: improper qualified name \(too many dotted names\): postgres\.long\.dotted\.string"""
        ],
        "ungrammatical database names still draw errors under --no-strict-names",
    )
    node.command_checks_all(
        ["pg_amcheck", "--no-strict-names", "--exclude-table", "a.b.c.d"],
        2,
        [r"""^$"""],
        [
            r"""pg_amcheck: error: improper relation name \(too many dotted names\): a\.b\.c\.d"""
        ],
        "ungrammatical table exclusions still draw errors under --no-strict-names",
    )
    node.command_checks_all(
        ["pg_amcheck", "--no-strict-names", "--exclude-schema", "a.b.c"],
        2,
        [r"""^$"""],
        [
            r"""pg_amcheck: error: improper qualified name \(too many dotted names\): a\.b\.c"""
        ],
        "ungrammatical schema exclusions still draw errors under --no-strict-names",
    )
    node.command_checks_all(
        ["pg_amcheck", "--no-strict-names", "--exclude-database", "a.b"],
        2,
        [r"""^$"""],
        [
            r"""pg_amcheck: error: improper qualified name \(too many dotted names\): a\.b"""
        ],
        "ungrammatical database exclusions still draw errors under --no-strict-names",
    )
    node.command_checks_all(
        [
            "pg_amcheck",
            "--no-strict-names",
            "--table",
            "no_such_table",
            "--table",
            "no*such*table",
            "--index",
            "no_such_index",
            "--index",
            "no*such*index",
            "--relation",
            "no_such_relation",
            "--relation",
            "no*such*relation",
            "--database",
            "no_such_database",
            "--database",
            "no*such*database",
            "--relation",
            "none.none",
            "--relation",
            "none.none.none",
            "--relation",
            "postgres.none.none",
            "--relation",
            "postgres.pg_catalog.none",
            "--relation",
            "postgres.none.pg_class",
            "--table",
            "postgres.pg_catalog.pg_class",
        ],
        0,
        [r"""^$"""],
        [
            r'''pg_amcheck: warning: no heap tables to check matching "no_such_table"''',
            r'''pg_amcheck: warning: no heap tables to check matching "no\*such\*table"''',
            r'''pg_amcheck: warning: no btree indexes to check matching "no_such_index"''',
            r'''pg_amcheck: warning: no btree indexes to check matching "no\*such\*index"''',
            r'''pg_amcheck: warning: no relations to check matching "no_such_relation"''',
            r'''pg_amcheck: warning: no relations to check matching "no\*such\*relation"''',
            r'''pg_amcheck: warning: no heap tables to check matching "no\*such\*table"''',
            r'''pg_amcheck: warning: no connectable databases to check matching "no_such_database"''',
            r'''pg_amcheck: warning: no connectable databases to check matching "no\*such\*database"''',
            r'''pg_amcheck: warning: no relations to check matching "none\.none"''',
            r'''pg_amcheck: warning: no connectable databases to check matching "none\.none\.none"''',
            r'''pg_amcheck: warning: no relations to check matching "postgres\.none\.none"''',
            r'''pg_amcheck: warning: no relations to check matching "postgres\.pg_catalog\.none"''',
            r'''pg_amcheck: warning: no relations to check matching "postgres\.none\.pg_class"''',
            r'''pg_amcheck: warning: no connectable databases to check matching "no_such_database"''',
            r'''pg_amcheck: warning: no connectable databases to check matching "no\*such\*database"''',
            r'''pg_amcheck: warning: no connectable databases to check matching "none\.none\.none"''',
        ],
        "many unmatched patterns and one matched pattern under --no-strict-names",
    )
    node.safe_psql(
        "CREATE DATABASE regression_invalid;\n\tUPDATE pg_database SET datconnlimit = -2 WHERE datname = 'regression_invalid';"
    )
    node.command_checks_all(
        ["pg_amcheck", "--database", "regression_invalid"],
        1,
        [r"""^$"""],
        [
            r'''pg_amcheck: error: no connectable databases to check matching "regression_invalid"'''
        ],
        "checking handling of invalid database",
    )
    node.command_checks_all(
        [
            "pg_amcheck",
            "--database",
            "postgres",
            "--table",
            "regression_invalid.public.foo",
        ],
        1,
        [r"""^$"""],
        [
            r'''pg_amcheck: error: no connectable databases to check matching "regression_invalid.public.foo"'''
        ],
        "checking handling of object in invalid database",
    )
    node.safe_psql(
        "CREATE TABLE public.foo (f integer);\n\tCREATE INDEX foo_idx ON foo(f);"
    )
    node.safe_psql("CREATE DATABASE another_db")
    node.command_checks_all(
        [
            "pg_amcheck",
            "--database",
            "postgres",
            "--no-strict-names",
            "--table",
            "template1.public.foo",
            "--table",
            "another_db.public.foo",
            "--table",
            "no_such_database.public.foo",
            "--index",
            "template1.public.foo_idx",
            "--index",
            "another_db.public.foo_idx",
            "--index",
            "no_such_database.public.foo_idx",
        ],
        1,
        [r"""^$"""],
        [
            r"""pg_amcheck: warning: skipping database "template1": amcheck is not installed""",
            r'''pg_amcheck: warning: no heap tables to check matching "template1\.public\.foo"''',
            r'''pg_amcheck: warning: no heap tables to check matching "another_db\.public\.foo"''',
            r'''pg_amcheck: warning: no connectable databases to check matching "no_such_database\.public\.foo"''',
            r'''pg_amcheck: warning: no btree indexes to check matching "template1\.public\.foo_idx"''',
            r'''pg_amcheck: warning: no btree indexes to check matching "another_db\.public\.foo_idx"''',
            r'''pg_amcheck: warning: no connectable databases to check matching "no_such_database\.public\.foo_idx"''',
            r"""pg_amcheck: error: no relations to check""",
        ],
        "checking otherwise existent objects in the wrong databases",
    )
    node.command_checks_all(
        [
            "pg_amcheck",
            "--all",
            "--no-strict-names",
            "--exclude-schema",
            "public",
            "--exclude-schema",
            "pg_catalog",
            "--exclude-schema",
            "pg_toast",
            "--exclude-schema",
            "information_schema",
        ],
        1,
        [r"""^$"""],
        [
            r"""pg_amcheck: warning: skipping database "template1": amcheck is not installed""",
            r"""pg_amcheck: error: no relations to check""",
        ],
        "schema exclusion patterns exclude all relations",
    )
    node.command_checks_all(
        [
            "pg_amcheck",
            "--all",
            "--no-strict-names",
            "--schema",
            "public",
            "--schema",
            "pg_catalog",
            "--schema",
            "pg_toast",
            "--schema",
            "information_schema",
            "--table",
            "pg_catalog.pg_class",
            "--exclude-schema",
            "*",
        ],
        1,
        [r"""^$"""],
        [
            r"""pg_amcheck: warning: skipping database "template1": amcheck is not installed""",
            r"""pg_amcheck: error: no relations to check""",
        ],
        "schema exclusion pattern overrides all inclusion patterns",
    )
