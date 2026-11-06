# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/scripts/t/020_createdb.pl."""

import os
import re


def test_createdb(pg_bin, create_pg):
    """createdb across encodings, locale providers, strategies, and templates."""
    pg_bin.program_help_ok("createdb")
    pg_bin.program_version_ok("createdb")
    pg_bin.program_options_handling_ok("createdb")

    node = create_pg("main")

    node.issues_sql_like(
        ["createdb", "foobar1"],
        r"statement: CREATE DATABASE foobar1",
        "SQL CREATE DATABASE run",
    )
    node.issues_sql_like(
        [
            "createdb",
            "--locale",
            "C",
            "--encoding",
            "LATIN1",
            "--template",
            "template0",
            "foobar2",
        ],
        r"statement: CREATE DATABASE foobar2 ENCODING 'LATIN1'",
        "create database with encoding",
    )

    if os.environ.get("with_icu") == "yes":
        _test_icu(pg_bin, node, create_pg)
    else:
        node.command_fails(
            [
                "createdb",
                "--template",
                "template0",
                "--locale-provider",
                "icu",
                "foobar4",
            ],
            "create database with ICU fails since no ICU support",
        )

    _test_builtin_provider(node)
    _test_misc_failures(node)
    _test_templates_and_strategies(node)


def _test_icu(pg_bin, node, create_pg):
    # Fails: template0 uses libc provider and has no ICU locale set.
    node.command_fails(
        [
            "createdb",
            "--template",
            "template0",
            "--encoding",
            "UTF8",
            "--locale-provider",
            "icu",
            "foobar4",
        ],
        "create database with ICU fails without ICU locale specified",
    )
    node.issues_sql_like(
        [
            "createdb",
            "--template",
            "template0",
            "--encoding",
            "UTF8",
            "--locale-provider",
            "icu",
            "--locale",
            "C",
            "--icu-locale",
            "en",
            "foobar5",
        ],
        r"statement: CREATE DATABASE foobar5 .* LOCALE_PROVIDER icu ICU_LOCALE 'en'",
        "create database with ICU locale specified",
    )
    node.command_fails(
        [
            "createdb",
            "--template",
            "template0",
            "--encoding",
            "UTF8",
            "--locale-provider",
            "icu",
            "--icu-locale",
            "@colNumeric=lower",
            "foobarX",
        ],
        "fails for invalid ICU locale",
    )
    node.command_fails_like(
        [
            "createdb",
            "--template",
            "template0",
            "--locale-provider",
            "icu",
            "--encoding",
            "SQL_ASCII",
            "foobarX",
        ],
        r'ERROR:  encoding "SQL_ASCII" is not supported with ICU provider',
        "fails for encoding not supported by ICU",
    )

    # Additional node which uses the icu provider.
    node2 = create_pg("icu", extra=["--locale-provider=icu", "--icu-locale=en"])
    node2.command_ok(
        [
            "createdb",
            "--template",
            "template0",
            "--locale-provider",
            "libc",
            "foobar55",
        ],
        "create database with libc provider from template database with icu provider",
    )
    node2.command_ok(
        ["createdb", "--template", "template0", "--icu-locale", "en-US", "foobar56"],
        "create database with icu locale from template database with icu provider",
    )
    node2.command_ok(
        [
            "createdb",
            "--template",
            "template0",
            "--locale-provider",
            "icu",
            "--locale",
            "en",
            "--lc-collate",
            "C",
            "--lc-ctype",
            "C",
            "foobar57",
        ],
        "create database with locale as ICU locale",
    )


def _test_builtin_provider(node):
    node.command_fails(
        [
            "createdb",
            "--template",
            "template0",
            "--locale-provider",
            "builtin",
            "tbuiltin1",
        ],
        'create database with provider "builtin" fails without --locale',
    )
    node.command_ok(
        [
            "createdb",
            "--template",
            "template0",
            "--locale-provider",
            "builtin",
            "--locale",
            "C",
            "tbuiltin2",
        ],
        'create database with provider "builtin" and locale "C"',
    )
    node.command_ok(
        [
            "createdb",
            "--template",
            "template0",
            "--locale-provider",
            "builtin",
            "--locale",
            "C",
            "--lc-collate",
            "C",
            "tbuiltin3",
        ],
        'create database with provider "builtin" and LC_COLLATE=C',
    )
    node.command_ok(
        [
            "createdb",
            "--template",
            "template0",
            "--locale-provider",
            "builtin",
            "--locale",
            "C",
            "--lc-ctype",
            "C",
            "tbuiltin4",
        ],
        'create database with provider "builtin" and LC_CTYPE=C',
    )
    node.command_ok(
        [
            "createdb",
            "--template",
            "template0",
            "--locale-provider",
            "builtin",
            "--lc-collate",
            "C",
            "--lc-ctype",
            "C",
            "--encoding",
            "UTF-8",
            "--builtin-locale",
            "C.UTF8",
            "tbuiltin5",
        ],
        "create database with --builtin-locale C.UTF-8 and -E UTF-8",
    )
    node.command_fails(
        [
            "createdb",
            "--template",
            "template0",
            "--locale-provider",
            "builtin",
            "--lc-collate",
            "C",
            "--lc-ctype",
            "C",
            "--encoding",
            "LATIN1",
            "--builtin-locale",
            "C.UTF-8",
            "tbuiltin6",
        ],
        "create database with --builtin-locale C.UTF-8 and -E LATIN1",
    )
    node.command_fails(
        [
            "createdb",
            "--template",
            "template0",
            "--locale-provider",
            "builtin",
            "--locale",
            "C",
            "--icu-locale",
            "en",
            "tbuiltin7",
        ],
        'create database with provider "builtin" and ICU_LOCALE="en"',
    )
    node.command_fails(
        [
            "createdb",
            "--template",
            "template0",
            "--locale-provider",
            "builtin",
            "--locale",
            "C",
            "--icu-rules",
            '""',
            "tbuiltin8",
        ],
        'create database with provider "builtin" and ICU_RULES=""',
    )
    node.command_fails(
        [
            "createdb",
            "--template",
            "template1",
            "--locale-provider",
            "builtin",
            "--locale",
            "C",
            "tbuiltin9",
        ],
        'create database with provider "builtin" not matching template',
    )


def _test_misc_failures(node):
    node.command_fails(["createdb", "foobar1"], "fails if database already exists")
    node.command_fails(
        ["createdb", "--template", "template0", "--locale-provider", "xyz", "foobarX"],
        "fails for invalid locale provider",
    )
    node.command_fails_like(
        ["createdb", "invalid \n dbname"],
        r"contains a newline or carriage return character",
        "fails if database name contains a newline character in name",
    )
    node.command_fails_like(
        ["createdb", "invalid \r dbname"],
        r"contains a newline or carriage return character",
        "fails if database name contains a carriage return character in name",
    )

    # Quote handling with incorrect option values.
    node.command_checks_all(
        ["createdb", "--encoding", "foo'; SELECT '1", "foobar2"],
        1,
        [r"^$"],
        [r"""(?s)^createdb: error: "foo'; SELECT '1" is not a valid encoding name"""],
        "createdb with incorrect --encoding",
    )
    node.command_checks_all(
        ["createdb", "--lc-collate", "foo'; SELECT '1", "foobar2"],
        1,
        [r"^$"],
        [
            r"(?s)^createdb: error: database creation failed: ERROR:  "
            r"invalid LC_COLLATE locale name"
            r"|^createdb: error: database creation failed: ERROR:  "
            r"new collation \(foo'; SELECT '1\) is incompatible with the "
            r"collation of the template database"
        ],
        "createdb with incorrect --lc-collate",
    )
    node.command_checks_all(
        ["createdb", "--lc-ctype", "foo'; SELECT '1", "foobar2"],
        1,
        [r"^$"],
        [
            r"(?s)^createdb: error: database creation failed: ERROR:  "
            r"invalid LC_CTYPE locale name"
            r"|^createdb: error: database creation failed: ERROR:  "
            r"new LC_CTYPE \(foo'; SELECT '1\) is incompatible with the "
            r"LC_CTYPE of the template database"
        ],
        "createdb with incorrect --lc-ctype",
    )
    node.command_checks_all(
        ["createdb", "--strategy", "foo", "foobar2"],
        1,
        [r"^$"],
        [
            r"(?s)^createdb: error: database creation failed: ERROR:  "
            r'invalid create database strategy "foo"'
        ],
        "createdb with incorrect --strategy",
    )


def _test_templates_and_strategies(node):
    # Use of templates with shared dependencies copied from the template.
    node.safe_psql(
        "CREATE ROLE role_foobar;"
        " CREATE TABLE tab_foobar (id int);"
        " ALTER TABLE tab_foobar owner to role_foobar;"
        " CREATE POLICY pol_foobar ON tab_foobar FOR ALL TO role_foobar;",
        dbname="foobar2",
    )
    node.issues_sql_like(
        ["createdb", "--locale", "C", "--template", "foobar2", "foobar3"],
        r"statement: CREATE DATABASE foobar3 TEMPLATE foobar2 LOCALE 'C'",
        "create database with template",
    )
    stdout = node.safe_psql(
        "SELECT pg_describe_object(classid, objid, objsubid) AS obj,"
        " pg_describe_object(refclassid, refobjid, 0) AS refobj"
        " FROM pg_shdepend s JOIN pg_database d ON (d.oid = s.dbid)"
        " WHERE d.datname = 'foobar3' ORDER BY obj;",
        dbname="foobar3",
    )
    assert re.search(
        r"^policy pol_foobar on table tab_foobar\|role role_foobar\n"
        r"table tab_foobar\|role role_foobar$",
        stdout,
    ), "shared dependencies copied over to target database"

    # Database creation strategy.
    node.issues_sql_like(
        ["createdb", "--template", "foobar2", "--strategy", "wal_log", "foobar6"],
        r"statement: CREATE DATABASE foobar6 STRATEGY wal_log TEMPLATE foobar2",
        "create database with WAL_LOG strategy",
    )
    node.issues_sql_like(
        ["createdb", "--template", "foobar2", "--strategy", "WAL_LOG", "foobar6s"],
        r'statement: CREATE DATABASE foobar6s STRATEGY "WAL_LOG" TEMPLATE foobar2',
        "create database with WAL_LOG strategy",
    )
    node.issues_sql_like(
        ["createdb", "--template", "foobar2", "--strategy", "file_copy", "foobar7"],
        r"statement: CREATE DATABASE foobar7 STRATEGY file_copy TEMPLATE foobar2",
        "create database with FILE_COPY strategy",
    )
    node.issues_sql_like(
        ["createdb", "--template", "foobar2", "--strategy", "FILE_COPY", "foobar7s"],
        r'statement: CREATE DATABASE foobar7s STRATEGY "FILE_COPY" TEMPLATE foobar2',
        "create database with FILE_COPY strategy",
    )

    # Database owned by role_foobar.
    node.issues_sql_like(
        ["createdb", "--template", "foobar2", "--owner", "role_foobar", "foobar8"],
        r"statement: CREATE DATABASE foobar8 OWNER role_foobar TEMPLATE foobar2",
        "create database with owner role_foobar",
    )
    node.safe_psql("DROP OWNED BY role_foobar;", dbname="foobar2")
    node.safe_psql("DROP DATABASE foobar8;", dbname="foobar2")
