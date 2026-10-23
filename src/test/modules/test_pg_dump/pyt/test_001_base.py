# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/modules/test_pg_dump/t/001_base.pl.

Data-driven pg_dump/pg_restore matrix for the test_pg_dump extension. A set of
named dump runs (full dumps, section/schema/data-only dumps, format+restore
round-trips, extension include/exclude variants, pg_dumpall --globals-only) is
executed against a single seeded server. Each named test owns a regexp plus
'like'/'unlike' membership keyed by run (or test_key); for every run the test's
regexp must match the dump output iff the run is a 'like' and not an 'unlike'.

Faithful transcription of the Perl original: %pgdump_runs, %full_runs, %tests
and the driver are reproduced below. Regexps preserve the Perl /xm (and /xms)
semantics via re.VERBOSE | re.MULTILINE (| re.DOTALL); the Perl \\Q...\\E
quotemeta blocks are expanded with re.escape (which also escapes spaces, so
they survive VERBOSE mode).
"""

import os
import re
import tempfile
from typing import Dict, List, Optional, Pattern, Tuple

import pypg

XM = re.VERBOSE | re.MULTILINE
XMS = re.VERBOSE | re.MULTILINE | re.DOTALL

# Each regexp is built from a sequence of segments. A ("lit", text) segment is
# a Perl \Q...\E quotemeta literal (re.escape, which also escapes spaces so it
# survives VERBOSE mode); an ("rx", raw) segment is verbatim regex syntax.
_Segment = Tuple[str, str]


def _qr(parts: List[_Segment], flags: int) -> Pattern[str]:
    """Compile a Perl-style qr/.../ from literal/regex segments."""
    pieces = []
    for kind, val in parts:
        pieces.append(re.escape(val) if kind == "lit" else val)
    return re.compile("".join(pieces), flags)


# ---------------------------------------------------------------------------
# Definition of the pg_dump runs to make. Mirrors %pgdump_runs.
#
# Each entry maps a run name to a dict with: 'dump_cmd' (argv, with $tempdir
# placeholders resolved at runtime), optional 'restore_cmd', optional
# 'test_key' (reuse another run's like/unlike set), optional 'compile_option'
# (skip the run when the build dependency is missing, e.g. 'gzip').
# ---------------------------------------------------------------------------


def _pgdump_runs(tempdir: str) -> Dict[str, dict]:
    """Build the run matrix with $tempdir paths resolved (mirrors %pgdump_runs)."""
    return {
        "binary_upgrade": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/binary_upgrade.sql",
                "--schema-only",
                "--sequence-data",
                "--binary-upgrade",
                "--dbname",
                "postgres",
            ],
        },
        "clean": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/clean.sql",
                "--clean",
                "--dbname",
                "postgres",
            ],
        },
        "clean_if_exists": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/clean_if_exists.sql",
                "--clean",
                "--if-exists",
                "--encoding",
                "UTF8",  # no-op, just tests that it is accepted
                "postgres",
            ],
        },
        "createdb": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/createdb.sql",
                "--create",
                "--no-reconnect",  # no-op, just for testing
                "postgres",
            ],
        },
        "data_only": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/data_only.sql",
                "--data-only",
                "--verbose",  # no-op, just make sure it works
                "postgres",
            ],
        },
        "defaults": {
            "dump_cmd": [
                "pg_dump",
                "--file",
                f"{tempdir}/defaults.sql",
                "postgres",
            ],
        },
        "defaults_custom_format": {
            "test_key": "defaults",
            "compile_option": "gzip",
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--format",
                "custom",
                "--compress",
                "6",
                "--file",
                f"{tempdir}/defaults_custom_format.dump",
                "postgres",
            ],
            "restore_cmd": [
                "pg_restore",
                "--file",
                f"{tempdir}/defaults_custom_format.sql",
                f"{tempdir}/defaults_custom_format.dump",
            ],
        },
        "defaults_dir_format": {
            "test_key": "defaults",
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--format",
                "directory",
                "--file",
                f"{tempdir}/defaults_dir_format",
                "postgres",
            ],
            "restore_cmd": [
                "pg_restore",
                "--file",
                f"{tempdir}/defaults_dir_format.sql",
                f"{tempdir}/defaults_dir_format",
            ],
        },
        "defaults_parallel": {
            "test_key": "defaults",
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--format",
                "directory",
                "--jobs",
                "2",
                "--file",
                f"{tempdir}/defaults_parallel",
                "postgres",
            ],
            "restore_cmd": [
                "pg_restore",
                "--file",
                f"{tempdir}/defaults_parallel.sql",
                f"{tempdir}/defaults_parallel",
            ],
        },
        "defaults_tar_format": {
            "test_key": "defaults",
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--format",
                "tar",
                "--file",
                f"{tempdir}/defaults_tar_format.tar",
                "postgres",
            ],
            "restore_cmd": [
                "pg_restore",
                "--file",
                f"{tempdir}/defaults_tar_format.sql",
                f"{tempdir}/defaults_tar_format.tar",
            ],
        },
        "exclude_table": {
            "dump_cmd": [
                "pg_dump",
                "--exclude-table",
                "regress_table_dumpable",
                "--file",
                f"{tempdir}/exclude_table.sql",
                "postgres",
            ],
        },
        "extension_schema": {
            "dump_cmd": [
                "pg_dump",
                "--schema",
                "public",
                "--file",
                f"{tempdir}/extension_schema.sql",
                "postgres",
            ],
        },
        "pg_dumpall_globals": {
            "dump_cmd": [
                "pg_dumpall",
                "--no-sync",
                "--file",
                f"{tempdir}/pg_dumpall_globals.sql",
                "--globals-only",
            ],
        },
        "no_privs": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/no_privs.sql",
                "--no-privileges",
                "postgres",
            ],
        },
        "no_owner": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/no_owner.sql",
                "--no-owner",
                "postgres",
            ],
        },
        # regress_dump_login_role shouldn't need SELECT rights on internal
        # (undumped) extension tables
        "privileged_internals": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/privileged_internals.sql",
                # these two tables are irrelevant to the test case
                "--exclude-table",
                "regress_pg_dump_schema.external_tab",
                "--exclude-table",
                "regress_pg_dump_schema.extdependtab",
                "--username",
                "regress_dump_login_role",
                "postgres",
            ],
        },
        "schema_only": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/schema_only.sql",
                "--schema-only",
                "postgres",
            ],
        },
        "section_pre_data": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/section_pre_data.sql",
                "--section",
                "pre-data",
                "postgres",
            ],
        },
        "section_data": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/section_data.sql",
                "--section",
                "data",
                "postgres",
            ],
        },
        "section_post_data": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/section_post_data.sql",
                "--section",
                "post-data",
                "postgres",
            ],
        },
        "with_extension": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/with_extension.sql",
                "--extension",
                "test_pg_dump",
                "postgres",
            ],
        },
        "exclude_extension": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/exclude_extension.sql",
                "--exclude-extension",
                "test_pg_dump",
                "postgres",
            ],
        },
        "exclude_extension_filter": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/exclude_extension_filter.sql",
                "--filter",
                f"{tempdir}/exclude_extension_filter.txt",
                "postgres",
            ],
        },
        # plpgsql in the list blocks the dump of extension test_pg_dump
        "without_extension": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/without_extension.sql",
                "--extension",
                "plpgsql",
                "postgres",
            ],
        },
        # plpgsql in the list of extensions blocks the dump of extension
        # test_pg_dump.  "public" is the schema used by the extension
        # test_pg_dump, but none of its objects should be dumped.
        "without_extension_explicit_schema": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/without_extension_explicit_schema.sql",
                "--extension",
                "plpgsql",
                "--schema",
                "public",
                "postgres",
            ],
        },
        # plpgsql in the list of extensions blocks the dump of extension
        # test_pg_dump, but not the dump of objects not dependent on the
        # extension located on a schema maintained by the extension.
        "without_extension_internal_schema": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/without_extension_internal_schema.sql",
                "--extension",
                "plpgsql",
                "--schema",
                "regress_pg_dump_schema",
                "postgres",
            ],
        },
    }


# Tests which are considered 'full' dumps by pg_dump, but there are flags used
# to exclude specific items (ACLs, LOs, etc).  Mirrors %full_runs.
FULL_RUNS: Dict[str, int] = {
    "binary_upgrade": 1,
    "clean": 1,
    "clean_if_exists": 1,
    "createdb": 1,
    "defaults": 1,
    "exclude_table": 1,
    "no_privs": 1,
    "no_owner": 1,
    "privileged_internals": 1,
    "with_extension": 1,
    "exclude_extension": 1,
    "exclude_extension_filter": 1,
    "without_extension": 1,
}


def _all_run_names() -> Dict[str, int]:
    """The set of all run names (mirrors {%pgdump_runs} as a like/unlike set)."""
    return {name: 1 for name in _pgdump_runs("")}


def _full() -> Dict[str, int]:
    """A fresh copy of FULL_RUNS for merging into a test's 'like'."""
    return dict(FULL_RUNS)


# ---------------------------------------------------------------------------
# Definition of the tests to run. Mirrors %tests.
#
# Each entry maps a test name (also the log message) to a dict with: 'regexp'
# (compiled), 'like'/'unlike' dicts keyed by run-name or test_key, and optional
# 'create_order' (int) + 'create_sql' (run during setup, ordered by it).
# ---------------------------------------------------------------------------


def _tests() -> Dict[str, dict]:
    """Build the test matrix (mirrors %tests)."""
    return {
        "ALTER EXTENSION test_pg_dump": {
            "create_order": 9,
            "create_sql": "ALTER EXTENSION test_pg_dump ADD TABLE regress_pg_dump_table_added;",
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    ("lit", "CREATE TABLE public.regress_pg_dump_table_added ("),
                    ("rx", r"\n\s+"),
                    ("lit", "col1 integer NOT NULL,"),
                    ("rx", r"\n\s+"),
                    ("lit", "col2 integer"),
                    ("rx", r"\n\);\n"),
                ],
                XM,
            ),
            "like": {"binary_upgrade": 1},
        },
        "CREATE EXTENSION test_pg_dump": {
            "create_order": 2,
            "create_sql": "CREATE EXTENSION test_pg_dump;",
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "CREATE EXTENSION IF NOT EXISTS test_pg_dump WITH SCHEMA public;",
                    ),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {
                **_full(),
                "schema_only": 1,
                "section_pre_data": 1,
            },
            "unlike": {
                "binary_upgrade": 1,
                "exclude_extension": 1,
                "exclude_extension_filter": 1,
                "without_extension": 1,
            },
        },
        "CREATE ROLE regress_dump_test_role": {
            "create_order": 1,
            "create_sql": "CREATE ROLE regress_dump_test_role;",
            "regexp": re.compile(
                r"^CREATE ROLE regress_dump_test_role;\n", re.MULTILINE
            ),
            "like": {"pg_dumpall_globals": 1},
        },
        "CREATE ROLE regress_dump_login_role": {
            "create_order": 1,
            "create_sql": "CREATE ROLE regress_dump_login_role LOGIN;",
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    ("lit", "CREATE ROLE regress_dump_login_role;"),
                    ("rx", r"\n"),
                    ("lit", "ALTER ROLE regress_dump_login_role WITH "),
                    ("rx", r".*"),
                    ("lit", " LOGIN "),
                    ("rx", r".*;"),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {"pg_dumpall_globals": 1},
        },
        "GRANT ALTER SYSTEM ON PARAMETER full_page_writes TO regress_dump_test_role": {
            "create_order": 2,
            "create_sql": "GRANT ALTER SYSTEM ON PARAMETER full_page_writes TO regress_dump_test_role;",
            "regexp": re.compile(
                r"^GRANT ALTER SYSTEM ON PARAMETER full_page_writes TO regress_dump_test_role;",
                re.MULTILINE,
            ),
            "like": {"pg_dumpall_globals": 1},
        },
        "GRANT ALL ON PARAMETER Custom.Knob TO regress_dump_test_role WITH GRANT OPTION": {
            "create_order": 2,
            "create_sql": "GRANT SET, ALTER SYSTEM ON PARAMETER Custom.Knob TO regress_dump_test_role WITH GRANT OPTION;",
            # "set" plus "alter system" is "all" privileges on parameters
            "regexp": re.compile(
                r'^GRANT ALL ON PARAMETER "custom.knob" TO regress_dump_test_role WITH GRANT OPTION;',
                re.MULTILINE,
            ),
            "like": {"pg_dumpall_globals": 1},
        },
        "GRANT ALL ON PARAMETER DateStyle TO regress_dump_test_role": {
            "create_order": 2,
            "create_sql": 'GRANT ALL ON PARAMETER "DateStyle" TO regress_dump_test_role WITH GRANT OPTION; REVOKE GRANT OPTION FOR ALL ON PARAMETER DateStyle FROM regress_dump_test_role;',
            # The revoke simplifies the ultimate grant so as to not include
            # "with grant option"
            "regexp": re.compile(
                r"^GRANT ALL ON PARAMETER datestyle TO regress_dump_test_role;",
                re.MULTILINE,
            ),
            "like": {"pg_dumpall_globals": 1},
        },
        "CREATE SCHEMA public": {
            "regexp": re.compile(r"^CREATE SCHEMA public;", re.MULTILINE),
            "like": {
                "extension_schema": 1,
                "without_extension_explicit_schema": 1,
            },
        },
        "CREATE SEQUENCE regress_pg_dump_table_col1_seq": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    ("lit", "CREATE SEQUENCE public.regress_pg_dump_table_col1_seq"),
                    ("rx", r"\n\s+"),
                    ("lit", "AS integer"),
                    ("rx", r"\n\s+"),
                    ("lit", "START WITH 1"),
                    ("rx", r"\n\s+"),
                    ("lit", "INCREMENT BY 1"),
                    ("rx", r"\n\s+"),
                    ("lit", "NO MINVALUE"),
                    ("rx", r"\n\s+"),
                    ("lit", "NO MAXVALUE"),
                    ("rx", r"\n\s+"),
                    ("lit", "CACHE 1;"),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {"binary_upgrade": 1},
        },
        "CREATE TABLE regress_pg_dump_table_added": {
            "create_order": 7,
            "create_sql": "CREATE TABLE regress_pg_dump_table_added (col1 int not null, col2 int);",
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    ("lit", "CREATE TABLE public.regress_pg_dump_table_added ("),
                    ("rx", r"\n\s+"),
                    ("lit", "col1 integer NOT NULL,"),
                    ("rx", r"\n\s+"),
                    ("lit", "col2 integer"),
                    ("rx", r"\n\);\n"),
                ],
                XM,
            ),
            "like": {"binary_upgrade": 1},
        },
        "CREATE SEQUENCE regress_pg_dump_seq": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    ("lit", "CREATE SEQUENCE public.regress_pg_dump_seq"),
                    ("rx", r"\n\s+"),
                    ("lit", "START WITH 1"),
                    ("rx", r"\n\s+"),
                    ("lit", "INCREMENT BY 1"),
                    ("rx", r"\n\s+"),
                    ("lit", "NO MINVALUE"),
                    ("rx", r"\n\s+"),
                    ("lit", "NO MAXVALUE"),
                    ("rx", r"\n\s+"),
                    ("lit", "CACHE 1;"),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {"binary_upgrade": 1},
        },
        "SETVAL SEQUENCE regress_seq_dumpable": {
            "create_order": 6,
            "create_sql": "SELECT nextval('regress_seq_dumpable');",
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "SELECT pg_catalog.setval('public.regress_seq_dumpable', 1, true);",
                    ),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {
                **_full(),
                "data_only": 1,
                "section_data": 1,
                "extension_schema": 1,
            },
            "unlike": {
                "exclude_extension": 1,
                "exclude_extension_filter": 1,
                "without_extension": 1,
            },
        },
        "CREATE TABLE regress_pg_dump_table": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    ("lit", "CREATE TABLE public.regress_pg_dump_table ("),
                    ("rx", r"\n\s+"),
                    ("lit", "col1 integer NOT NULL,"),
                    ("rx", r"\n\s+"),
                    ("lit", "col2 integer,"),
                    ("rx", r"\n\s+"),
                    (
                        "lit",
                        "CONSTRAINT regress_pg_dump_table_col2_check CHECK ((col2 > 0))",
                    ),
                    ("rx", r"\n\);\n"),
                ],
                XM,
            ),
            "like": {"binary_upgrade": 1},
        },
        "COPY public.regress_table_dumpable (col1)": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    ("lit", "COPY public.regress_table_dumpable (col1) FROM stdin;"),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {
                **_full(),
                "data_only": 1,
                "section_data": 1,
                "extension_schema": 1,
            },
            "unlike": {
                "binary_upgrade": 1,
                "exclude_table": 1,
                "exclude_extension": 1,
                "exclude_extension_filter": 1,
                "without_extension": 1,
            },
        },
        "REVOKE ALL ON FUNCTION wgo_then_no_access": {
            "create_order": 3,
            "create_sql": "\n\t\t\tDO $$BEGIN EXECUTE format(\n"
            "\t\t\t\t'REVOKE ALL ON FUNCTION wgo_then_no_access()\n"
            "\t\t\t\t FROM pg_signal_backend, public, %I',\n"
            "\t\t\t\t(SELECT usename\n"
            "\t\t\t\t FROM pg_user JOIN pg_proc ON proowner = usesysid\n"
            "\t\t\t\t WHERE proname = 'wgo_then_no_access')); END$$;",
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "REVOKE ALL ON FUNCTION public.wgo_then_no_access() FROM PUBLIC;",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "REVOKE ALL ON FUNCTION public.wgo_then_no_access() FROM ",
                    ),
                    ("rx", r".*;"),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "REVOKE ALL ON FUNCTION public.wgo_then_no_access() FROM pg_signal_backend;",
                    ),
                    ("rx", r""),
                ],
                XM,
            ),
            "like": {
                **_full(),
                "schema_only": 1,
                "section_pre_data": 1,
            },
            "unlike": {
                "no_privs": 1,
                "exclude_extension": 1,
                "exclude_extension_filter": 1,
                "without_extension": 1,
            },
        },
        "REVOKE GRANT OPTION FOR UPDATE ON SEQUENCE wgo_then_regular": {
            "create_order": 3,
            "create_sql": "REVOKE GRANT OPTION FOR UPDATE ON SEQUENCE\n"
            "\t\t\t\t\t\twgo_then_regular FROM pg_signal_backend;",
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "REVOKE ALL ON SEQUENCE public.wgo_then_regular FROM pg_signal_backend;",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "GRANT SELECT,UPDATE ON SEQUENCE public.wgo_then_regular TO pg_signal_backend;",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "GRANT USAGE ON SEQUENCE public.wgo_then_regular TO pg_signal_backend WITH GRANT OPTION;",
                    ),
                    ("rx", r""),
                ],
                XM,
            ),
            "like": {
                **_full(),
                "schema_only": 1,
                "section_pre_data": 1,
            },
            "unlike": {
                "no_privs": 1,
                "exclude_extension": 1,
                "exclude_extension_filter": 1,
                "without_extension": 1,
            },
        },
        "CREATE ACCESS METHOD regress_test_am": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "CREATE ACCESS METHOD regress_test_am TYPE INDEX HANDLER bthandler;",
                    ),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {"binary_upgrade": 1},
        },
        "COMMENT ON EXTENSION test_pg_dump": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    ("lit", "COMMENT ON EXTENSION test_pg_dump "),
                    ("lit", "IS 'Test pg_dump with an extension';"),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {
                **_full(),
                "schema_only": 1,
                "section_pre_data": 1,
            },
            "unlike": {
                "exclude_extension": 1,
                "exclude_extension_filter": 1,
                "without_extension": 1,
            },
        },
        "GRANT SELECT regress_pg_dump_table_added pre-ALTER EXTENSION": {
            "create_order": 8,
            "create_sql": "GRANT SELECT ON regress_pg_dump_table_added TO regress_dump_test_role;",
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "GRANT SELECT ON TABLE public.regress_pg_dump_table_added TO regress_dump_test_role;",
                    ),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {"binary_upgrade": 1},
        },
        "REVOKE SELECT regress_pg_dump_table_added post-ALTER EXTENSION": {
            "create_order": 10,
            "create_sql": "REVOKE SELECT ON regress_pg_dump_table_added FROM regress_dump_test_role;",
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "REVOKE SELECT ON TABLE public.regress_pg_dump_table_added FROM regress_dump_test_role;",
                    ),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {
                **_full(),
                "schema_only": 1,
                "section_pre_data": 1,
            },
            "unlike": {
                "no_privs": 1,
                "exclude_extension": 1,
                "exclude_extension_filter": 1,
                "without_extension": 1,
            },
        },
        "GRANT SELECT ON TABLE regress_pg_dump_table": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "SELECT pg_catalog.binary_upgrade_set_record_init_privs(true);",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "GRANT SELECT ON TABLE public.regress_pg_dump_table TO regress_dump_test_role;",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "SELECT pg_catalog.binary_upgrade_set_record_init_privs(false);",
                    ),
                    ("rx", r"\n"),
                ],
                XMS,
            ),
            "like": {"binary_upgrade": 1},
        },
        "GRANT SELECT(col1) ON regress_pg_dump_table": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "SELECT pg_catalog.binary_upgrade_set_record_init_privs(true);",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "GRANT SELECT(col1) ON TABLE public.regress_pg_dump_table TO PUBLIC;",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "SELECT pg_catalog.binary_upgrade_set_record_init_privs(false);",
                    ),
                    ("rx", r"\n"),
                ],
                XMS,
            ),
            "like": {"binary_upgrade": 1},
        },
        "GRANT SELECT(col2) ON regress_pg_dump_table TO regress_dump_test_role": {
            "create_order": 4,
            "create_sql": "GRANT SELECT(col2) ON regress_pg_dump_table\n"
            "\t\t\t\t\t\t   TO regress_dump_test_role;",
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "GRANT SELECT(col2) ON TABLE public.regress_pg_dump_table TO regress_dump_test_role;",
                    ),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {
                **_full(),
                "schema_only": 1,
                "section_pre_data": 1,
            },
            "unlike": {
                "no_privs": 1,
                "exclude_extension": 1,
                "exclude_extension_filter": 1,
                "without_extension": 1,
            },
        },
        "GRANT USAGE ON regress_pg_dump_table_col1_seq TO regress_dump_test_role": {
            "create_order": 5,
            "create_sql": "GRANT USAGE ON SEQUENCE regress_pg_dump_table_col1_seq\n"
            "\t\t                   TO regress_dump_test_role;",
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "GRANT USAGE ON SEQUENCE public.regress_pg_dump_table_col1_seq TO regress_dump_test_role;",
                    ),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {
                **_full(),
                "schema_only": 1,
                "section_pre_data": 1,
            },
            "unlike": {
                "no_privs": 1,
                "exclude_extension": 1,
                "exclude_extension_filter": 1,
                "without_extension": 1,
            },
        },
        "GRANT USAGE ON regress_pg_dump_seq TO regress_dump_test_role": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "GRANT USAGE ON SEQUENCE public.regress_pg_dump_seq TO regress_dump_test_role;",
                    ),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {"binary_upgrade": 1},
        },
        "REVOKE SELECT(col1) ON regress_pg_dump_table": {
            "create_order": 3,
            "create_sql": "REVOKE SELECT(col1) ON regress_pg_dump_table\n"
            "\t\t\t\t\t\t   FROM PUBLIC;",
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "REVOKE SELECT(col1) ON TABLE public.regress_pg_dump_table FROM PUBLIC;",
                    ),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {
                **_full(),
                "schema_only": 1,
                "section_pre_data": 1,
            },
            "unlike": {
                "no_privs": 1,
                "exclude_extension": 1,
                "exclude_extension_filter": 1,
                "without_extension": 1,
            },
        },
        # Objects included in extension part of a schema created by this extension
        "CREATE TABLE regress_pg_dump_schema.test_table": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    ("lit", "CREATE TABLE regress_pg_dump_schema.test_table ("),
                    ("rx", r"\n\s+"),
                    ("lit", "col1 integer,"),
                    ("rx", r"\n\s+"),
                    ("lit", "col2 integer,"),
                    ("rx", r"\n\s+"),
                    ("lit", "CONSTRAINT test_table_col2_check CHECK ((col2 > 0))"),
                    ("rx", r"\n\);\n"),
                ],
                XM,
            ),
            "like": {"binary_upgrade": 1},
        },
        "GRANT SELECT ON regress_pg_dump_schema.test_table": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "SELECT pg_catalog.binary_upgrade_set_record_init_privs(true);",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "GRANT SELECT ON TABLE regress_pg_dump_schema.test_table TO regress_dump_test_role;",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "SELECT pg_catalog.binary_upgrade_set_record_init_privs(false);",
                    ),
                    ("rx", r"\n"),
                ],
                XMS,
            ),
            "like": {"binary_upgrade": 1},
        },
        "CREATE SEQUENCE regress_pg_dump_schema.test_seq": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    ("lit", "CREATE SEQUENCE regress_pg_dump_schema.test_seq"),
                    ("rx", r"\n\s+"),
                    ("lit", "START WITH 1"),
                    ("rx", r"\n\s+"),
                    ("lit", "INCREMENT BY 1"),
                    ("rx", r"\n\s+"),
                    ("lit", "NO MINVALUE"),
                    ("rx", r"\n\s+"),
                    ("lit", "NO MAXVALUE"),
                    ("rx", r"\n\s+"),
                    ("lit", "CACHE 1;"),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {"binary_upgrade": 1},
        },
        "GRANT USAGE ON regress_pg_dump_schema.test_seq": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "SELECT pg_catalog.binary_upgrade_set_record_init_privs(true);",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "GRANT USAGE ON SEQUENCE regress_pg_dump_schema.test_seq TO regress_dump_test_role;",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "SELECT pg_catalog.binary_upgrade_set_record_init_privs(false);",
                    ),
                    ("rx", r"\n"),
                ],
                XMS,
            ),
            "like": {"binary_upgrade": 1},
        },
        "CREATE TYPE regress_pg_dump_schema.test_type": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    ("lit", "CREATE TYPE regress_pg_dump_schema.test_type AS ("),
                    ("rx", r"\n\s+"),
                    ("lit", "col1 integer"),
                    ("rx", r"\n\);\n"),
                ],
                XM,
            ),
            "like": {"binary_upgrade": 1},
        },
        "GRANT USAGE ON regress_pg_dump_schema.test_type": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "SELECT pg_catalog.binary_upgrade_set_record_init_privs(true);",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "GRANT ALL ON TYPE regress_pg_dump_schema.test_type TO regress_dump_test_role;",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "SELECT pg_catalog.binary_upgrade_set_record_init_privs(false);",
                    ),
                    ("rx", r"\n"),
                ],
                XMS,
            ),
            "like": {"binary_upgrade": 1},
        },
        "CREATE FUNCTION regress_pg_dump_schema.test_func": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "CREATE FUNCTION regress_pg_dump_schema.test_func() RETURNS integer",
                    ),
                    ("rx", r"\n\s+"),
                    ("lit", "LANGUAGE sql"),
                    ("rx", r"\n"),
                ],
                XM,
            ),
            "like": {"binary_upgrade": 1},
        },
        "GRANT ALL ON regress_pg_dump_schema.test_func": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "SELECT pg_catalog.binary_upgrade_set_record_init_privs(true);",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "GRANT ALL ON FUNCTION regress_pg_dump_schema.test_func() TO regress_dump_test_role;",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "SELECT pg_catalog.binary_upgrade_set_record_init_privs(false);",
                    ),
                    ("rx", r"\n"),
                ],
                XMS,
            ),
            "like": {"binary_upgrade": 1},
        },
        "CREATE AGGREGATE regress_pg_dump_schema.test_agg": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "CREATE AGGREGATE regress_pg_dump_schema.test_agg(smallint) (",
                    ),
                    ("rx", r"\n\s+"),
                    ("lit", "SFUNC = int2_sum,"),
                    ("rx", r"\n\s+"),
                    ("lit", "STYPE = bigint"),
                    ("rx", r"\n\);\n"),
                ],
                XM,
            ),
            "like": {"binary_upgrade": 1},
        },
        "GRANT ALL ON regress_pg_dump_schema.test_agg": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "SELECT pg_catalog.binary_upgrade_set_record_init_privs(true);",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "GRANT ALL ON FUNCTION regress_pg_dump_schema.test_agg(smallint) TO regress_dump_test_role;",
                    ),
                    ("rx", r"\n"),
                    (
                        "lit",
                        "SELECT pg_catalog.binary_upgrade_set_record_init_privs(false);",
                    ),
                    ("rx", r"\n"),
                ],
                XMS,
            ),
            "like": {"binary_upgrade": 1},
        },
        "ALTER INDEX pkey DEPENDS ON extension": {
            "create_order": 11,
            "create_sql": "CREATE TABLE regress_pg_dump_schema.extdependtab (col1 integer primary key, col2 int);\n"
            "\t\tCREATE INDEX ON regress_pg_dump_schema.extdependtab (col2);\n"
            "\t\tALTER INDEX regress_pg_dump_schema.extdependtab_col2_idx DEPENDS ON EXTENSION test_pg_dump;\n"
            "\t\tALTER INDEX regress_pg_dump_schema.extdependtab_pkey DEPENDS ON EXTENSION test_pg_dump;",
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "ALTER INDEX regress_pg_dump_schema.extdependtab_pkey DEPENDS ON EXTENSION test_pg_dump;",
                    ),
                    ("rx", r"\n"),
                ],
                XMS,
            ),
            "like": {**_all_run_names()},
            "unlike": {
                "data_only": 1,
                "extension_schema": 1,
                "pg_dumpall_globals": 1,
                "privileged_internals": 1,
                "section_data": 1,
                "section_pre_data": 1,
                # Excludes this schema as extension is not listed.
                "without_extension_explicit_schema": 1,
            },
        },
        "ALTER INDEX idx DEPENDS ON extension": {
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    (
                        "lit",
                        "ALTER INDEX regress_pg_dump_schema.extdependtab_col2_idx DEPENDS ON EXTENSION test_pg_dump;",
                    ),
                    ("rx", r"\n"),
                ],
                XMS,
            ),
            "like": {**_all_run_names()},
            "unlike": {
                "data_only": 1,
                "extension_schema": 1,
                "pg_dumpall_globals": 1,
                "privileged_internals": 1,
                "section_data": 1,
                "section_pre_data": 1,
                # Excludes this schema as extension is not listed.
                "without_extension_explicit_schema": 1,
            },
        },
        # Objects not included in extension, part of schema created by extension
        "CREATE TABLE regress_pg_dump_schema.external_tab": {
            "create_order": 4,
            "create_sql": "CREATE TABLE regress_pg_dump_schema.external_tab\n"
            "\t\t\t\t\t\t   (col1 int);",
            "regexp": _qr(
                [
                    ("rx", r"^"),
                    ("lit", "CREATE TABLE regress_pg_dump_schema.external_tab ("),
                    ("rx", r"\n\s+"),
                    ("lit", "col1 integer"),
                    ("rx", r"\n\);\n"),
                ],
                XM,
            ),
            "like": {
                **_full(),
                "schema_only": 1,
                "section_pre_data": 1,
                # Excludes the extension and keeps the schema's data.
                "without_extension_internal_schema": 1,
            },
            "unlike": {"privileged_internals": 1},
        },
    }


def _create_order_key(item: Tuple[str, dict]) -> Tuple[int, int]:
    """Sort key reproducing the Perl create_order comparator.

    Tests with create_order come first, ordered numerically; tests without it
    follow in their existing (insertion) order, matching the stable sort the
    Perl comparator yields for the no-order pairs.
    """
    order = item[1].get("create_order")
    if order is None:
        return (1, 0)
    return (0, order)


def _build_create_sql(tests: Dict[str, dict]) -> str:
    """Concatenate each test's create_sql in create_order (mirrors the driver)."""
    create_sql = ""
    for _name, test in sorted(tests.items(), key=_create_order_key):
        if test.get("create_sql"):
            create_sql += test["create_sql"]
    return create_sql


def _check_test_definitions(tests: Dict[str, dict], test_key: str) -> None:
    """Reproduce the Perl die() sanity checks for like/unlike completeness."""
    for name, test in tests.items():
        if test.get("like") is None:
            raise AssertionError(f'missing "like" in test "{name}"')
        unlike = test.get("unlike") or {}
        like = test["like"]
        if unlike.get(test_key) and like.get(test_key) is None:
            raise AssertionError(
                f'useless "unlike" entry "{test_key}" in test "{name}"'
            )


def _run_tests_for_output(
    run: str,
    test_key: str,
    output_file: str,
    tests: Dict[str, dict],
) -> None:
    """Apply every test's like/unlike rule to one run's dumped SQL."""
    _check_test_definitions(tests, test_key)
    for name in sorted(tests):
        test = tests[name]
        like = test["like"]
        unlike = test.get("unlike") or {}
        regexp: Pattern[str] = test["regexp"]
        if like.get(test_key) and unlike.get(test_key) is None:
            assert regexp.search(
                output_file
            ), "{run}: should dump {name}\nReview {run} results".format(
                run=run, name=name
            )
        else:
            assert not regexp.search(
                output_file
            ), "{run}: should not dump {name}\nReview {run} results".format(
                run=run, name=name
            )


def test_001_base(create_pg):
    """pg_dump/pg_restore matrix against the test_pg_dump extension."""
    with tempfile.TemporaryDirectory(prefix="pg_dump_001_base") as tempdir:
        _run_matrix(create_pg, tempdir)


def _run_matrix(create_pg, tempdir: str) -> None:
    """Seed a server, then execute every run x test pair (mirrors the driver)."""
    pgdump_runs = _pgdump_runs(tempdir)
    tests = _tests()

    # Create a PG instance to test actually dumping from.
    node = create_pg("main", auth_extra=["--create-role", "regress_dump_login_role"])

    supports_gzip = pypg.check_pg_config(r"#define HAVE_LIBZ 1")

    # Set up schemas, tables, etc, to be dumped: build and run the combined
    # create statements (ordered by create_order).
    node.safe_psql(_build_create_sql(tests))

    # Create filter file for the exclude_extension_filter run.
    with open(
        os.path.join(tempdir, "exclude_extension_filter.txt"), "w", encoding="utf-8"
    ) as filterfile:
        filterfile.write("exclude extension test_pg_dump\n")

    # Run all runs.
    for run in sorted(pgdump_runs):
        spec = pgdump_runs[run]

        # Skip command-level tests for gzip if there is no support for it.
        if spec.get("compile_option") == "gzip" and not supports_gzip:
            continue

        node.command_ok(spec["dump_cmd"], f"{run}: pg_dump runs")

        restore_cmd: Optional[list] = spec.get("restore_cmd")
        if restore_cmd:
            node.command_ok(restore_cmd, f"{run}: pg_restore runs")

        test_key = spec.get("test_key", run)

        output_file = pypg.slurp_file(os.path.join(tempdir, f"{run}.sql"))

        _run_tests_for_output(run, test_key, output_file, tests)
