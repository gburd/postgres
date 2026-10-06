# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-lines
"""Port of src/bin/pg_dump/t/002_pg_dump.pl.

Data-driven pg_dump/pg_restore matrix for the pg_dump binary. A large set of
named dump runs (full dumps, section/schema/table/data-only dumps, format +
restore round-trips, exclude/only variants, pg_dumpall globals/dbprivs,
statistics import, ...) is executed against a single seeded server. Each named
test owns a regexp plus 'like'/'unlike' membership keyed by run (or test_key);
for every run the test's regexp must match the dump output iff the run is a
'like' and not an 'unlike'.

Faithful transcription of the Perl original: %pgdump_runs, %dump_test_schema_runs,
%full_runs, %tests and the driver are reproduced below. Regexps preserve the
Perl /xm (and /xms, /m, /s) semantics via re.VERBOSE | re.MULTILINE (and
re.DOTALL where /s applies). The Perl \\Q...\\E quotemeta blocks are expanded
with re.escape (which also escapes spaces, so they survive VERBOSE mode).
"""

import glob
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
# 'test_key' (reuse another run's like/unlike set), optional 'database' (the
# database the run dumps from, default 'postgres'), optional 'command_like'
# (run a side command and assert its stdout matches), optional 'glob_patterns'
# (files that must exist after the dump).
# ---------------------------------------------------------------------------


def _pgdump_runs(tempdir: str, supports_gzip: bool) -> Dict[str, dict]:
    """Build the run matrix with $tempdir paths resolved (mirrors %pgdump_runs)."""
    return {
        "binary_upgrade": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--format",
                "custom",
                "--file",
                f"{tempdir}/binary_upgrade.dump",
                "--no-password",
                "--no-data",
                "--sequence-data",
                "--binary-upgrade",
                "--statistics",
                "--dbname",
                "postgres",
            ],
            "restore_cmd": [
                "pg_restore",
                "--format",
                "custom",
                "--verbose",
                "--file",
                f"{tempdir}/binary_upgrade.sql",
                "--statistics",
                f"{tempdir}/binary_upgrade.dump",
            ],
        },
        "clean": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/clean.sql",
                "--clean",
                "--statistics",
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
                "UTF8",
                "--statistics",
                "postgres",
            ],
        },
        "column_inserts": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/column_inserts.sql",
                "--data-only",
                "--column-inserts",
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
                "--no-reconnect",
                "--verbose",
                "--statistics",
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
                "--superuser",
                "test_superuser",
                "--disable-triggers",
                "--verbose",
                "postgres",
            ],
        },
        "defaults": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/defaults.sql",
                "--statistics",
                "postgres",
            ],
        },
        "defaults_no_public": {
            "database": "regress_pg_dump_test",
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/defaults_no_public.sql",
                "--statistics",
                "regress_pg_dump_test",
            ],
        },
        "defaults_no_public_clean": {
            "database": "regress_pg_dump_test",
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--clean",
                "--file",
                f"{tempdir}/defaults_no_public_clean.sql",
                "--statistics",
                "regress_pg_dump_test",
            ],
        },
        "defaults_public_owner": {
            "database": "regress_public_owner",
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/defaults_public_owner.sql",
                "--statistics",
                "regress_public_owner",
            ],
        },
        # Do not use --no-sync to give test coverage for data sync.
        # By default, the custom format compresses its data file
        # when the code is compiled with gzip support, and lets them
        # uncompressed when not compiled with it.
        "defaults_custom_format": {
            "test_key": "defaults",
            "dump_cmd": [
                "pg_dump",
                "--format",
                "custom",
                "--file",
                f"{tempdir}/defaults_custom_format.dump",
                "--statistics",
                "postgres",
            ],
            "restore_cmd": [
                "pg_restore",
                "--format",
                "custom",
                "--file",
                f"{tempdir}/defaults_custom_format.sql",
                "--statistics",
                f"{tempdir}/defaults_custom_format.dump",
            ],
            "command_like": {
                "command": [
                    "pg_restore",
                    "--list",
                    f"{tempdir}/defaults_custom_format.dump",
                ],
                "expected": (
                    re.compile(r"Compression:\ gzip", re.VERBOSE)
                    if supports_gzip
                    else re.compile(r"Compression:\ none", re.VERBOSE)
                ),
                "name": "data content is gzip-compressed by default if available",
            },
        },
        # Do not use --no-sync to give test coverage for data sync.
        # By default, the directory format compresses its data files
        # when the code is compiled with gzip support, and lets them
        # uncompressed when not compiled with it.
        "defaults_dir_format": {
            "test_key": "defaults",
            "dump_cmd": [
                "pg_dump",
                "--format",
                "directory",
                "--file",
                f"{tempdir}/defaults_dir_format",
                "--statistics",
                "postgres",
            ],
            "restore_cmd": [
                "pg_restore",
                "--format",
                "directory",
                "--file",
                f"{tempdir}/defaults_dir_format.sql",
                "--statistics",
                f"{tempdir}/defaults_dir_format",
            ],
            "command_like": {
                "command": [
                    "pg_restore",
                    "--list",
                    f"{tempdir}/defaults_dir_format",
                ],
                "expected": (
                    re.compile(r"Compression:\ gzip", re.VERBOSE)
                    if supports_gzip
                    else re.compile(r"Compression:\ none", re.VERBOSE)
                ),
                "name": "data content is gzip-compressed by default",
            },
            "glob_patterns": [
                f"{tempdir}/defaults_dir_format/toc.dat",
                f"{tempdir}/defaults_dir_format/blobs_*.toc",
                (
                    f"{tempdir}/defaults_dir_format/*.dat.gz"
                    if supports_gzip
                    else f"{tempdir}/defaults_dir_format/*.dat"
                ),
            ],
        },
        # Do not use --no-sync to give test coverage for data sync.
        "defaults_parallel": {
            "test_key": "defaults",
            "dump_cmd": [
                "pg_dump",
                "--format",
                "directory",
                "--jobs",
                "2",
                "--file",
                f"{tempdir}/defaults_parallel",
                "--statistics",
                "postgres",
            ],
            "restore_cmd": [
                "pg_restore",
                "--file",
                f"{tempdir}/defaults_parallel.sql",
                "--statistics",
                f"{tempdir}/defaults_parallel",
            ],
        },
        # Do not use --no-sync to give test coverage for data sync.
        "defaults_tar_format": {
            "test_key": "defaults",
            "dump_cmd": [
                "pg_dump",
                "--format",
                "tar",
                "--file",
                f"{tempdir}/defaults_tar_format.tar",
                "--statistics",
                "postgres",
            ],
            "restore_cmd": [
                "pg_restore",
                "--format",
                "tar",
                "--file",
                f"{tempdir}/defaults_tar_format.sql",
                "--statistics",
                f"{tempdir}/defaults_tar_format.tar",
            ],
        },
        "exclude_dump_test_schema": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/exclude_dump_test_schema.sql",
                "--exclude-schema",
                "dump_test",
                "--statistics",
                "postgres",
            ],
        },
        "exclude_test_table": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/exclude_test_table.sql",
                "--exclude-table",
                "dump_test.test_table",
                "--statistics",
                "postgres",
            ],
        },
        "exclude_measurement": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/exclude_measurement.sql",
                "--exclude-table-and-children",
                "dump_test.measurement",
                "--statistics",
                "postgres",
            ],
        },
        "exclude_measurement_data": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/exclude_measurement_data.sql",
                "--exclude-table-data-and-children",
                "dump_test.measurement",
                "--no-unlogged-table-data",
                "--statistics",
                "postgres",
            ],
        },
        "exclude_test_table_data": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/exclude_test_table_data.sql",
                "--exclude-table-data",
                "dump_test.test_table",
                "--no-unlogged-table-data",
                "--statistics",
                "postgres",
            ],
        },
        "inserts": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/inserts.sql",
                "--data-only",
                "--inserts",
                "postgres",
            ],
        },
        "pg_dumpall_globals": {
            "dump_cmd": [
                "pg_dumpall",
                "--verbose",
                "--file",
                f"{tempdir}/pg_dumpall_globals.sql",
                "--globals-only",
                "--no-sync",
            ],
        },
        "pg_dumpall_globals_clean": {
            "dump_cmd": [
                "pg_dumpall",
                "--file",
                f"{tempdir}/pg_dumpall_globals_clean.sql",
                "--globals-only",
                "--clean",
                "--no-sync",
            ],
        },
        "pg_dumpall_dbprivs": {
            "dump_cmd": [
                "pg_dumpall",
                "--no-sync",
                "--file",
                f"{tempdir}/pg_dumpall_dbprivs.sql",
                "--statistics",
            ],
        },
        "pg_dumpall_exclude": {
            "dump_cmd": [
                "pg_dumpall",
                "--verbose",
                "--file",
                f"{tempdir}/pg_dumpall_exclude.sql",
                "--exclude-database",
                "*dump_test*",
                "--no-sync",
                "--statistics",
            ],
        },
        "no_toast_compression": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/no_toast_compression.sql",
                "--no-toast-compression",
                "--statistics",
                "postgres",
            ],
        },
        "no_large_objects": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/no_large_objects.sql",
                "--no-large-objects",
                "--statistics",
                "postgres",
            ],
        },
        "no_policies": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/no_policies.sql",
                "--no-policies",
                "--statistics",
                "postgres",
            ],
        },
        "no_policies_restore": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--format",
                "custom",
                "--file",
                f"{tempdir}/no_policies_restore.dump",
                "--statistics",
                "postgres",
            ],
            "restore_cmd": [
                "pg_restore",
                "--format",
                "custom",
                "--file",
                f"{tempdir}/no_policies_restore.sql",
                "--no-policies",
                "--statistics",
                f"{tempdir}/no_policies_restore.dump",
            ],
        },
        "no_privs": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/no_privs.sql",
                "--no-privileges",
                "--statistics",
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
                "--statistics",
                "postgres",
            ],
        },
        "no_subscriptions": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/no_subscriptions.sql",
                "--no-subscriptions",
                "--statistics",
                "postgres",
            ],
        },
        "no_subscriptions_restore": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--format",
                "custom",
                "--file",
                f"{tempdir}/no_subscriptions_restore.dump",
                "--statistics",
                "postgres",
            ],
            "restore_cmd": [
                "pg_restore",
                "--format",
                "custom",
                "--file",
                f"{tempdir}/no_subscriptions_restore.sql",
                "--no-subscriptions",
                "--statistics",
                f"{tempdir}/no_subscriptions_restore.dump",
            ],
        },
        "no_table_access_method": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/no_table_access_method.sql",
                "--no-table-access-method",
                "--statistics",
                "postgres",
            ],
        },
        "only_dump_test_schema": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/only_dump_test_schema.sql",
                "--schema",
                "dump_test",
                "--statistics",
                "postgres",
            ],
        },
        "only_dump_test_table": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/only_dump_test_table.sql",
                "--table",
                "dump_test.test_table",
                "--lock-wait-timeout",
                str(1000 * pypg.test_timeout_default()),
                "--statistics",
                "postgres",
            ],
        },
        "only_dump_measurement": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/only_dump_measurement.sql",
                "--table-and-children",
                "dump_test.measurement",
                "--lock-wait-timeout",
                str(1000 * pypg.test_timeout_default()),
                "--statistics",
                "postgres",
            ],
        },
        "role": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/role.sql",
                "--role",
                "regress_dump_test_role",
                "--schema",
                "dump_test_second_schema",
                "--statistics",
                "postgres",
            ],
        },
        "role_parallel": {
            "test_key": "role",
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--format",
                "directory",
                "--jobs",
                "2",
                "--file",
                f"{tempdir}/role_parallel",
                "--role",
                "regress_dump_test_role",
                "--schema",
                "dump_test_second_schema",
                "--statistics",
                "postgres",
            ],
            "restore_cmd": [
                "pg_restore",
                "--file",
                f"{tempdir}/role_parallel.sql",
                "--statistics",
                f"{tempdir}/role_parallel",
            ],
        },
        "rows_per_insert": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/rows_per_insert.sql",
                "--data-only",
                "--rows-per-insert",
                "4",
                "--table",
                "dump_test.test_table",
                "--table",
                "dump_test.test_fourth_table",
                "postgres",
            ],
        },
        "schema_only": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--format",
                "plain",
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
                "--statistics",
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
                "--statistics",
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
                "--statistics",
                "postgres",
            ],
        },
        "test_schema_plus_large_objects": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--file",
                f"{tempdir}/test_schema_plus_large_objects.sql",
                "--schema",
                "dump_test",
                "--large-objects",
                "--no-large-objects",
                "--statistics",
                "postgres",
            ],
        },
        "no_statistics": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                f"--file={tempdir}/no_statistics.sql",
                "--no-statistics",
                "postgres",
            ],
        },
        "no_data_no_schema": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                f"--file={tempdir}/no_data_no_schema.sql",
                "--no-data",
                "--no-schema",
                "postgres",
                "--statistics",
            ],
        },
        "statistics_only": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                f"--file={tempdir}/statistics_only.sql",
                "--statistics-only",
                "postgres",
            ],
        },
        "no_schema": {
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                f"--file={tempdir}/no_schema.sql",
                "--no-schema",
                "--statistics",
                "postgres",
            ],
        },
    }


# Tests which target the 'dump_test' schema, specifically.
# Mirrors %dump_test_schema_runs.
DUMP_TEST_SCHEMA_RUNS: Dict[str, int] = {
    "only_dump_test_schema": 1,
    "only_dump_measurement": 1,
    "test_schema_plus_large_objects": 1,
}

# Tests which are considered 'full' dumps by pg_dump, but there are flags used
# to exclude specific items (ACLs, LOs, etc).  Mirrors %full_runs.
#
# Note: 'schema_only_with_statistics' is not an actual run; it appears here (and
# in many 'unlike' sets) only as a membership marker so the like/unlike
# bookkeeping matches the Perl original exactly.
FULL_RUNS: Dict[str, int] = {
    "binary_upgrade": 1,
    "clean": 1,
    "clean_if_exists": 1,
    "createdb": 1,
    "defaults": 1,
    "exclude_dump_test_schema": 1,
    "exclude_test_table": 1,
    "exclude_test_table_data": 1,
    "exclude_measurement": 1,
    "exclude_measurement_data": 1,
    "no_toast_compression": 1,
    "no_large_objects": 1,
    "no_owner": 1,
    "no_policies": 1,
    "no_policies_restore": 1,
    "no_privs": 1,
    "no_statistics": 1,
    "no_subscriptions": 1,
    "no_subscriptions_restore": 1,
    "no_table_access_method": 1,
    "pg_dumpall_dbprivs": 1,
    "pg_dumpall_exclude": 1,
    "schema_only": 1,
    "schema_only_with_statistics": 1,
}


def _full() -> Dict[str, int]:
    """A fresh copy of FULL_RUNS for merging into a test's 'like'."""
    return dict(FULL_RUNS)


def _dts() -> Dict[str, int]:
    """A fresh copy of DUMP_TEST_SCHEMA_RUNS for merging into a test's 'like'."""
    return dict(DUMP_TEST_SCHEMA_RUNS)


# ---------------------------------------------------------------------------
# Definition of the tests to run. Mirrors %tests.
#
# Each entry maps a test name (also the log message) to a dict with: 'regexp'
# (compiled), 'like'/'unlike' dicts keyed by run-name or test_key, optional
# 'all_runs', optional 'create_order' (int) + 'create_sql' (run during setup,
# ordered by it), optional 'database', and optional 'collation'/'icu' gating.
# ---------------------------------------------------------------------------


def _tests() -> Dict[str, dict]:  # pylint: disable=too-many-statements
    """Build the test matrix (mirrors %tests)."""
    tests: Dict[str, dict] = {}

    tests["restrict"] = {
        "all_runs": 1,
        "regexp": re.compile(r"^\\restrict [a-zA-Z0-9]+$", re.MULTILINE),
    }
    tests["unrestrict"] = {
        "all_runs": 1,
        "regexp": re.compile(r"^\\unrestrict [a-zA-Z0-9]+$", re.MULTILINE),
    }
    tests["ALTER DEFAULT PRIVILEGES FOR ROLE regress_dump_test_role GRANT"] = {
        "create_order": 14,
        "create_sql": "ALTER DEFAULT PRIVILEGES\n"
        "\t\t\t\t\t   FOR ROLE regress_dump_test_role IN SCHEMA dump_test\n"
        "\t\t\t\t\t   GRANT SELECT ON TABLES TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER DEFAULT PRIVILEGES "),
                ("lit", "FOR ROLE regress_dump_test_role IN SCHEMA dump_test "),
                ("lit", "GRANT SELECT ON TABLES TO regress_dump_test_role;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_privs": 1,
            "only_dump_measurement": 1,
        },
    }
    tests[
        "ALTER DEFAULT PRIVILEGES FOR ROLE regress_dump_test_role GRANT EXECUTE ON FUNCTIONS"
    ] = {
        "create_order": 15,
        "create_sql": "ALTER DEFAULT PRIVILEGES\n"
        "\t\t\t\t\t   FOR ROLE regress_dump_test_role IN SCHEMA dump_test\n"
        "\t\t\t\t\t   GRANT EXECUTE ON FUNCTIONS TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER DEFAULT PRIVILEGES "),
                ("lit", "FOR ROLE regress_dump_test_role IN SCHEMA dump_test "),
                ("lit", "GRANT ALL ON FUNCTIONS TO regress_dump_test_role;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_privs": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER DEFAULT PRIVILEGES FOR ROLE regress_dump_test_role REVOKE"] = {
        "create_order": 55,
        "create_sql": "ALTER DEFAULT PRIVILEGES\n"
        "\t\t\t\t\t   FOR ROLE regress_dump_test_role\n"
        "\t\t\t\t\t   REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER DEFAULT PRIVILEGES "),
                ("lit", "FOR ROLE regress_dump_test_role "),
                ("lit", "REVOKE ALL ON FUNCTIONS FROM PUBLIC;"),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
        "unlike": {"no_privs": 1},
    }
    tests["ALTER DEFAULT PRIVILEGES FOR ROLE regress_dump_test_role REVOKE SELECT"] = {
        "create_order": 56,
        "create_sql": "ALTER DEFAULT PRIVILEGES\n"
        "\t\t\t\t\t   FOR ROLE regress_dump_test_role\n"
        "\t\t\t\t\t   REVOKE SELECT ON TABLES FROM regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER DEFAULT PRIVILEGES "),
                ("lit", "FOR ROLE regress_dump_test_role "),
                ("lit", "REVOKE ALL ON TABLES FROM regress_dump_test_role;"),
                ("rx", r"\n"),
                ("lit", "ALTER DEFAULT PRIVILEGES "),
                ("lit", "FOR ROLE regress_dump_test_role "),
                (
                    "lit",
                    "GRANT INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,MAINTAIN,UPDATE ON TABLES TO regress_dump_test_role;",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
        "unlike": {"no_privs": 1},
    }
    tests["ALTER ROLE regress_dump_test_role"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER ROLE regress_dump_test_role WITH "),
                ("lit", "NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB NOLOGIN "),
                ("lit", "NOREPLICATION NOBYPASSRLS;"),
            ],
            XM,
        ),
        "like": {
            "pg_dumpall_dbprivs": 1,
            "pg_dumpall_globals": 1,
            "pg_dumpall_globals_clean": 1,
            "pg_dumpall_exclude": 1,
        },
    }
    tests["ALTER COLLATION test0 OWNER TO"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER COLLATION public.test0 OWNER TO "),
                ("rx", r".+;"),
            ],
            re.MULTILINE,
        ),
        "collation": 1,
        "like": {**_full(), "section_pre_data": 1},
        "unlike": {"no_owner": 1},
    }
    tests["ALTER FOREIGN DATA WRAPPER dummy OWNER TO"] = {
        "regexp": re.compile(
            r"^ALTER FOREIGN DATA WRAPPER dummy OWNER TO .+;", re.MULTILINE
        ),
        "like": {**_full(), "section_pre_data": 1},
        "unlike": {"no_owner": 1},
    }
    tests["ALTER SERVER s1 OWNER TO"] = {
        "regexp": re.compile(r"^ALTER SERVER s1 OWNER TO .+;", re.MULTILINE),
        "like": {**_full(), "section_pre_data": 1},
        "unlike": {"no_owner": 1},
    }
    tests["ALTER FUNCTION dump_test.pltestlang_call_handler() OWNER TO"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER FUNCTION dump_test.pltestlang_call_handler() "),
                ("lit", "OWNER TO "),
                ("rx", r".+;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_owner": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER OPERATOR FAMILY dump_test.op_family OWNER TO"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER OPERATOR FAMILY dump_test.op_family USING btree "),
                ("lit", "OWNER TO "),
                ("rx", r".+;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_owner": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER OPERATOR FAMILY dump_test.op_family USING btree"] = {
        "create_order": 75,
        "create_sql": "ALTER OPERATOR FAMILY dump_test.op_family USING btree ADD\n"
        "\t\t\t\t\t\t OPERATOR 1 <(bigint,int4),\n"
        "\t\t\t\t\t\t OPERATOR 2 <=(bigint,int4),\n"
        "\t\t\t\t\t\t OPERATOR 3 =(bigint,int4),\n"
        "\t\t\t\t\t\t OPERATOR 4 >=(bigint,int4),\n"
        "\t\t\t\t\t\t OPERATOR 5 >(bigint,int4),\n"
        "\t\t\t\t\t\t FUNCTION 1 (int4, int4) btint4cmp(int4,int4),\n"
        "\t\t\t\t\t\t FUNCTION 2 (int4, int4) btint4sortsupport(internal),\n"
        "\t\t\t\t\t\t FUNCTION 4 (int4, int4) btequalimage(oid);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER OPERATOR FAMILY dump_test.op_family USING btree ADD"),
                ("rx", r"\n\s+"),
                ("lit", "OPERATOR 1 <(bigint,integer) ,"),
                ("rx", r"\n\s+"),
                ("lit", "OPERATOR 2 <=(bigint,integer) ,"),
                ("rx", r"\n\s+"),
                ("lit", "OPERATOR 3 =(bigint,integer) ,"),
                ("rx", r"\n\s+"),
                ("lit", "OPERATOR 4 >=(bigint,integer) ,"),
                ("rx", r"\n\s+"),
                ("lit", "OPERATOR 5 >(bigint,integer) ,"),
                ("rx", r"\n\s+"),
                ("lit", "FUNCTION 1 (integer, integer) btint4cmp(integer,integer) ,"),
                ("rx", r"\n\s+"),
                ("lit", "FUNCTION 2 (bigint, bigint) btint8sortsupport(internal) ,"),
                ("rx", r"\n\s+"),
                ("lit", "FUNCTION 2 (integer, integer) btint4sortsupport(internal) ,"),
                ("rx", r"\n\s+"),
                ("lit", "FUNCTION 4 (bigint, bigint) btequalimage(oid) ,"),
                ("rx", r"\n\s+"),
                ("lit", "FUNCTION 4 (integer, integer) btequalimage(oid);"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER OPERATOR CLASS dump_test.op_class OWNER TO"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER OPERATOR CLASS dump_test.op_class USING btree "),
                ("lit", "OWNER TO "),
                ("rx", r".+;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_owner": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER PUBLICATION pub1 OWNER TO"] = {
        "regexp": re.compile(r"^ALTER PUBLICATION pub1 OWNER TO .+;", re.MULTILINE),
        "like": {**_full(), "section_post_data": 1},
        "unlike": {"no_owner": 1},
    }
    tests["ALTER LARGE OBJECT ... OWNER TO"] = {
        "regexp": re.compile(r"^ALTER LARGE OBJECT \d+ OWNER TO .+;", re.MULTILINE),
        "like": {
            **_full(),
            "column_inserts": 1,
            "data_only": 1,
            "inserts": 1,
            "no_schema": 1,
            "section_data": 1,
            "test_schema_plus_large_objects": 1,
        },
        "unlike": {
            "binary_upgrade": 1,
            "no_large_objects": 1,
            "no_owner": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
        },
    }
    tests["ALTER PROCEDURAL LANGUAGE pltestlang OWNER TO"] = {
        "regexp": re.compile(
            r"^ALTER PROCEDURAL LANGUAGE pltestlang OWNER TO .+;", re.MULTILINE
        ),
        "like": {**_full(), "section_pre_data": 1},
        "unlike": {"no_owner": 1},
    }
    tests["ALTER SCHEMA dump_test OWNER TO"] = {
        "regexp": re.compile(r"^ALTER SCHEMA dump_test OWNER TO .+;", re.MULTILINE),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_owner": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER SCHEMA dump_test_second_schema OWNER TO"] = {
        "regexp": re.compile(
            r"^ALTER SCHEMA dump_test_second_schema OWNER TO .+;", re.MULTILINE
        ),
        "like": {**_full(), "role": 1, "section_pre_data": 1},
        "unlike": {"no_owner": 1},
    }
    tests["ALTER SCHEMA public OWNER TO"] = {
        "create_order": 15,
        "create_sql": 'ALTER SCHEMA public OWNER TO "regress_quoted  \\"" role";',
        "regexp": re.compile(r"^ALTER SCHEMA public OWNER TO .+;", re.MULTILINE),
        "like": {**_full(), "section_pre_data": 1},
        "unlike": {"no_owner": 1},
    }
    tests["ALTER SCHEMA public OWNER TO (w/o ACL changes)"] = {
        "database": "regress_public_owner",
        "create_order": 100,
        "create_sql": 'ALTER SCHEMA public OWNER TO "regress_quoted  \\"" role";',
        "regexp": re.compile(r"^(GRANT|REVOKE)", re.MULTILINE),
        "like": {},
    }
    tests["ALTER SEQUENCE test_table_col1_seq"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER SEQUENCE dump_test.test_table_col1_seq OWNED BY dump_test.test_table.col1;",
                ),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_pre_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER TABLE ONLY test_table ADD CONSTRAINT ... PRIMARY KEY"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER TABLE ONLY dump_test.test_table"),
                ("rx", r" \n^\s+"),
                ("lit", "ADD CONSTRAINT test_table_pkey PRIMARY KEY (col1);"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_post_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "only_dump_measurement": 1,
        },
    }

    tests["CONSTRAINT NOT NULL / NOT VALID"] = {
        "create_sql": "CREATE TABLE dump_test.test_table_nn (\n"
        "\t\t\t\t\t\t\tcol1 int);\n"
        "\t\t\t\t\t\t\tCREATE TABLE dump_test.test_table_nn_2 (\n"
        "\t\t\t\t\t\t\tcol1 int NOT NULL);\n"
        "\t\t\t\t\t\t\tCREATE TABLE dump_test.test_table_nn_chld1 (\n"
        "\t\t\t\t\t\t\t) INHERITS (dump_test.test_table_nn);\n"
        "\t\t\t\t\t\t\tCREATE TABLE dump_test.test_table_nn_chld2 (\n"
        "\t\t\t\t\t\t\t\tcol1 int\n"
        "\t\t\t\t\t\t\t) INHERITS (dump_test.test_table_nn);\n"
        "\t\t\t\t\t\t\tCREATE TABLE dump_test.test_table_nn_chld3 (\n"
        "\t\t\t\t\t\t\t) INHERITS (dump_test.test_table_nn, dump_test.test_table_nn_2);\n"
        "\t\t\tALTER TABLE dump_test.test_table_nn ADD CONSTRAINT nn NOT NULL col1 NOT VALID;\n"
        "\t\t\tALTER TABLE dump_test.test_table_nn_chld1 VALIDATE CONSTRAINT nn;\n"
        "\t\t\tALTER TABLE dump_test.test_table_nn_chld2 VALIDATE CONSTRAINT nn;\n"
        "\t\t\tCOMMENT ON CONSTRAINT nn ON dump_test.test_table_nn IS 'nn comment is valid';\n"
        "\t\t\tCOMMENT ON CONSTRAINT nn ON dump_test.test_table_nn_chld2 IS 'nn_chld2 comment is valid';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER TABLE dump_test.test_table_nn"),
                ("rx", r" \n^\s+"),
                ("lit", "ADD CONSTRAINT nn NOT NULL col1 NOT VALID;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON CONSTRAINT ON test_table_nn"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "COMMENT ON CONSTRAINT nn ON dump_test.test_table_nn IS"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON CONSTRAINT ON test_table_chld2"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON CONSTRAINT nn ON dump_test.test_table_nn_chld2 IS",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CONSTRAINT NOT NULL / NOT VALID (child1)"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_table_nn_chld1 ("),
                ("rx", r"\n^\s+"),
                ("lit", "CONSTRAINT nn NOT NULL col1"),
                ("rx", r"$"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
            "binary_upgrade": 1,
        },
    }
    tests["CONSTRAINT NOT NULL / NOT VALID (child2)"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_table_nn_chld2 ("),
                ("rx", r"\n^\s+"),
                ("lit", "col1 integer CONSTRAINT nn NOT NULL"),
                ("rx", r"$"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CONSTRAINT NOT NULL / NOT VALID (child3)"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_table_nn_chld3 ("),
                ("rx", r"\n^"),
                ("lit", ")"),
                ("rx", r"$"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
            "binary_upgrade": 1,
        },
    }
    tests["CONSTRAINT NOT NULL / NO INHERIT"] = {
        "create_sql": "CREATE TABLE dump_test.test_table_nonn (\n"
        "\t\tcol1 int NOT NULL NO INHERIT,\n"
        "\t\tcol2 int);\n"
        "\t\tCREATE TABLE dump_test.test_table_nonn_chld1 (\n"
        "\t\t   CONSTRAINT nn NOT NULL col2 NO INHERIT)\n"
        "\t\tINHERITS (dump_test.test_table_nonn); ",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_table_nonn ("),
                ("rx", r" \n^\s+"),
                ("lit", "col1 integer NOT NULL NO INHERIT"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1, "binary_upgrade": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CONSTRAINT NOT NULL / NO INHERIT (child1)"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_table_nonn_chld1 ("),
                ("rx", r" \n^\s+"),
                ("lit", "CONSTRAINT nn NOT NULL col2 NO INHERIT"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
            "binary_upgrade": 1,
        },
    }
    tests["CONSTRAINT PRIMARY KEY / WITHOUT OVERLAPS"] = {
        "create_sql": "CREATE TABLE dump_test.test_table_tpk (\n"
        "\t\t\t\t\t\t\tcol1 int4range,\n"
        "\t\t\t\t\t\t\tcol2 tstzrange,\n"
        "\t\t\t\t\t\t\tCONSTRAINT test_table_tpk_pkey PRIMARY KEY (col1, col2 WITHOUT OVERLAPS));",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER TABLE ONLY dump_test.test_table_tpk"),
                ("rx", r" \n^\s+"),
                (
                    "lit",
                    "ADD CONSTRAINT test_table_tpk_pkey PRIMARY KEY (col1, col2 WITHOUT OVERLAPS);",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CONSTRAINT UNIQUE / WITHOUT OVERLAPS"] = {
        "create_sql": "CREATE TABLE dump_test.test_table_tuq (\n"
        "\t\t\t\t\t\t\tcol1 int4range,\n"
        "\t\t\t\t\t\t\tcol2 tstzrange,\n"
        "\t\t\t\t\t\t\tCONSTRAINT test_table_tuq_uq UNIQUE (col1, col2 WITHOUT OVERLAPS));",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER TABLE ONLY dump_test.test_table_tuq"),
                ("rx", r" \n^\s+"),
                (
                    "lit",
                    "ADD CONSTRAINT test_table_tuq_uq UNIQUE (col1, col2 WITHOUT OVERLAPS);",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER TABLE (partitioned) ADD CONSTRAINT ... FOREIGN KEY"] = {
        "create_order": 4,
        "create_sql": "CREATE TABLE dump_test.test_table_fk (\n"
        "\t\t\t\t\t\t\tcol1 int references dump_test.test_table)\n"
        "\t\t\t\t\t\t\tPARTITION BY RANGE (col1);\n"
        "\t\t\t\t\t\t\tCREATE TABLE dump_test.test_table_fk_1\n"
        "\t\t\t\t\t\t\tPARTITION OF dump_test.test_table_fk\n"
        "\t\t\t\t\t\t\tFOR VALUES FROM (0) TO (10);",
        "regexp": _qr(
            [
                (
                    "lit",
                    "ADD CONSTRAINT test_table_fk_col1_fkey FOREIGN KEY (col1) REFERENCES dump_test.test_table",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }

    tests["ALTER TABLE ONLY test_table ALTER COLUMN col1 SET STATISTICS 90"] = {
        "create_order": 93,
        "create_sql": "ALTER TABLE dump_test.test_table ALTER COLUMN col1 SET STATISTICS 90;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER TABLE ONLY dump_test.test_table ALTER COLUMN col1 SET STATISTICS 90;",
                ),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_pre_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER TABLE ONLY test_table ALTER COLUMN col2 SET STORAGE"] = {
        "create_order": 94,
        "create_sql": "ALTER TABLE dump_test.test_table ALTER COLUMN col2 SET STORAGE EXTERNAL;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER TABLE ONLY dump_test.test_table ALTER COLUMN col2 SET STORAGE EXTERNAL;",
                ),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_pre_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER TABLE ONLY test_table ALTER COLUMN col3 SET STORAGE"] = {
        "create_order": 95,
        "create_sql": "ALTER TABLE dump_test.test_table ALTER COLUMN col3 SET STORAGE MAIN;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER TABLE ONLY dump_test.test_table ALTER COLUMN col3 SET STORAGE MAIN;",
                ),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_pre_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER TABLE ONLY test_table ALTER COLUMN col4 SET n_distinct"] = {
        "create_order": 95,
        "create_sql": "ALTER TABLE dump_test.test_table ALTER COLUMN col4 SET (n_distinct = 10);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER TABLE ONLY dump_test.test_table ALTER COLUMN col4 SET (n_distinct=10);",
                ),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_pre_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "only_dump_measurement": 1,
        },
    }
    tests[
        "ALTER TABLE ONLY dump_test.measurement ATTACH PARTITION measurement_y2006m2"
    ] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER TABLE ONLY dump_test.measurement ATTACH PARTITION dump_test_second_schema.measurement_y2006m2 ",
                ),
                ("lit", "FOR VALUES FROM ('2006-02-01') TO ('2006-03-01');"),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            "role": 1,
            "section_pre_data": 1,
            "binary_upgrade": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {"exclude_measurement": 1},
    }
    tests["ALTER TABLE test_table CLUSTER ON test_table_pkey"] = {
        "create_order": 96,
        "create_sql": "ALTER TABLE dump_test.test_table CLUSTER ON test_table_pkey",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER TABLE dump_test.test_table CLUSTER ON test_table_pkey;"),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_post_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER TABLE test_table DISABLE TRIGGER ALL"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "SET SESSION AUTHORIZATION 'test_superuser';"),
                ("rx", r"\n\n"),
                ("lit", "ALTER TABLE dump_test.test_table DISABLE TRIGGER ALL;"),
                ("rx", r"\n\n"),
                (
                    "lit",
                    "COPY dump_test.test_table (col1, col2, col3, col4) FROM stdin;",
                ),
                ("rx", r"\n(?:\d\t\\N\t\\N\t\\N\n){9}\\\.\n\n\n"),
                ("lit", "ALTER TABLE dump_test.test_table ENABLE TRIGGER ALL;"),
            ],
            XM,
        ),
        "like": {"data_only": 1},
    }
    tests["ALTER FOREIGN TABLE foreign_table ALTER COLUMN c1 OPTIONS"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER FOREIGN TABLE ONLY dump_test.foreign_table ALTER COLUMN c1 OPTIONS (",
                ),
                ("rx", r"\n\s+"),
                ("lit", "column_name 'col1'"),
                ("rx", r"\n"),
                ("lit", ");"),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER TABLE test_table OWNER TO"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER TABLE dump_test.test_table OWNER TO "),
                ("rx", r".+;"),
            ],
            re.MULTILINE,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_pre_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "only_dump_measurement": 1,
            "no_owner": 1,
        },
    }
    tests["ALTER TABLE test_table ENABLE ROW LEVEL SECURITY"] = {
        "create_order": 23,
        "create_sql": "ALTER TABLE dump_test.test_table\n"
        "\t\t\t\t\t   ENABLE ROW LEVEL SECURITY;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER TABLE dump_test.test_table ENABLE ROW LEVEL SECURITY;"),
            ],
            re.MULTILINE,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_post_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "no_policies": 1,
            "no_policies_restore": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER TABLE test_second_table OWNER TO"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER TABLE dump_test.test_second_table OWNER TO "),
                ("rx", r".+;"),
            ],
            re.MULTILINE,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_owner": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER TABLE measurement OWNER TO"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER TABLE dump_test.measurement OWNER TO "),
                ("rx", r".+;"),
            ],
            re.MULTILINE,
        ),
        "like": {
            **_full(),
            **_dts(),
            "section_pre_data": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_owner": 1,
            "exclude_measurement": 1,
        },
    }
    tests["ALTER TABLE measurement_y2006m2 OWNER TO"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER TABLE dump_test_second_schema.measurement_y2006m2 OWNER TO ",
                ),
                ("rx", r".+;"),
            ],
            re.MULTILINE,
        ),
        "like": {
            **_full(),
            "role": 1,
            "section_pre_data": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {
            "no_owner": 1,
            "exclude_measurement": 1,
        },
    }
    tests["ALTER FOREIGN TABLE foreign_table OWNER TO"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER FOREIGN TABLE dump_test.foreign_table OWNER TO "),
                ("rx", r".+;"),
            ],
            re.MULTILINE,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_owner": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER TEXT SEARCH CONFIGURATION alt_ts_conf1 OWNER TO"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER TEXT SEARCH CONFIGURATION dump_test.alt_ts_conf1 OWNER TO ",
                ),
                ("rx", r".+;"),
            ],
            re.MULTILINE,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_owner": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER TEXT SEARCH DICTIONARY alt_ts_dict1 OWNER TO"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER TEXT SEARCH DICTIONARY dump_test.alt_ts_dict1 OWNER TO ",
                ),
                ("rx", r".+;"),
            ],
            re.MULTILINE,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_owner": 1,
            "only_dump_measurement": 1,
        },
    }

    tests["LO create (using lo_from_bytea)"] = {
        "create_order": 50,
        "create_sql": "SELECT pg_catalog.lo_from_bytea(0, '\\x310a320a330a340a350a360a370a380a390a');",
        "regexp": re.compile(r"^SELECT pg_catalog\.lo_create\('\d+'\);", re.MULTILINE),
        "like": {
            **_full(),
            "column_inserts": 1,
            "data_only": 1,
            "inserts": 1,
            "no_schema": 1,
            "section_data": 1,
            "test_schema_plus_large_objects": 1,
        },
        "unlike": {
            "binary_upgrade": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
            "no_large_objects": 1,
        },
    }
    tests["LO load (using lo_from_bytea)"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "SELECT pg_catalog.lo_open"),
                ("rx", r" \('\d+',\ \d+\);\n"),
                ("lit", "SELECT pg_catalog.lowrite(0, "),
                ("lit", "'\\x310a320a330a340a350a360a370a380a390a');"),
                ("rx", r"\n"),
                ("lit", "SELECT pg_catalog.lo_close(0);"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            "column_inserts": 1,
            "data_only": 1,
            "inserts": 1,
            "no_schema": 1,
            "section_data": 1,
            "test_schema_plus_large_objects": 1,
        },
        "unlike": {
            "binary_upgrade": 1,
            "no_large_objects": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
        },
    }
    tests["LO create (with no data)"] = {
        "create_sql": "SELECT pg_catalog.lo_create(0);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "SELECT pg_catalog.lo_open"),
                ("rx", r" \('\d+',\ \d+\);\n"),
                ("lit", "SELECT pg_catalog.lo_close(0);"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            "column_inserts": 1,
            "data_only": 1,
            "inserts": 1,
            "no_schema": 1,
            "section_data": 1,
            "test_schema_plus_large_objects": 1,
        },
        "unlike": {
            "binary_upgrade": 1,
            "no_large_objects": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
        },
    }
    tests["COMMENT ON DATABASE postgres"] = {
        "regexp": re.compile(r"^COMMENT ON DATABASE postgres IS .+;", re.MULTILINE),
        "like": {"createdb": 1},
    }
    tests["COMMENT ON EXTENSION plpgsql"] = {
        "regexp": re.compile(r"^COMMENT ON EXTENSION plpgsql IS .+;", re.MULTILINE),
        "like": {},
    }
    tests["COMMENT ON SCHEMA public"] = {
        "regexp": re.compile(r"^COMMENT ON SCHEMA public IS .+;", re.MULTILINE),
        "like": {
            "pg_dumpall_dbprivs": 1,
            "pg_dumpall_exclude": 1,
        },
    }
    tests["COMMENT ON SCHEMA public IS NULL"] = {
        "database": "regress_public_owner",
        "create_order": 100,
        "create_sql": "COMMENT ON SCHEMA public IS NULL;",
        "regexp": re.compile(r"^COMMENT ON SCHEMA public IS '';", re.MULTILINE),
        "like": {"defaults_public_owner": 1},
    }
    tests["COMMENT ON TABLE dump_test.test_table"] = {
        "create_order": 36,
        "create_sql": "COMMENT ON TABLE dump_test.test_table\n"
        "\t\t\t\t\t   IS 'comment on table';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "COMMENT ON TABLE dump_test.test_table IS 'comment on table';"),
            ],
            re.MULTILINE,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_pre_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON COLUMN dump_test.test_table.col1"] = {
        "create_order": 36,
        "create_sql": "COMMENT ON COLUMN dump_test.test_table.col1\n"
        "\t\t\t\t\t   IS 'comment on column';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON COLUMN dump_test.test_table.col1 IS 'comment on column';",
                ),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_pre_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON COLUMN dump_test.composite.f1"] = {
        "create_order": 44,
        "create_sql": "COMMENT ON COLUMN dump_test.composite.f1\n"
        "\t\t\t\t\t   IS 'comment on column of type';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON COLUMN dump_test.composite.f1 IS 'comment on column of type';",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON COLUMN dump_test.test_second_table.col1"] = {
        "create_order": 63,
        "create_sql": "COMMENT ON COLUMN dump_test.test_second_table.col1\n"
        "\t\t\t\t\t   IS 'comment on column col1';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON COLUMN dump_test.test_second_table.col1 IS 'comment on column col1';",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON COLUMN dump_test.test_second_table.col2"] = {
        "create_order": 64,
        "create_sql": "COMMENT ON COLUMN dump_test.test_second_table.col2\n"
        "\t\t\t\t\t   IS 'comment on column col2';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON COLUMN dump_test.test_second_table.col2 IS 'comment on column col2';",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON CONVERSION dump_test.test_conversion"] = {
        "create_order": 79,
        "create_sql": "COMMENT ON CONVERSION dump_test.test_conversion\n"
        "\t\t\t\t\t   IS 'comment on test conversion';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON CONVERSION dump_test.test_conversion IS 'comment on test conversion';",
                ),
            ],
            re.MULTILINE,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON COLLATION test0"] = {
        "create_order": 77,
        "create_sql": "COMMENT ON COLLATION test0\n"
        "\t\t\t\t\t   IS 'comment on test0 collation';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON COLLATION public.test0 IS 'comment on test0 collation';",
                ),
            ],
            re.MULTILINE,
        ),
        "collation": 1,
        "like": {**_full(), "section_pre_data": 1},
    }
    tests["COMMENT ON LARGE OBJECT ..."] = {
        "create_order": 65,
        "create_sql": "DO $$\n"
        "\t\t\t\t\t\t DECLARE myoid oid;\n"
        "\t\t\t\t\t\t BEGIN\n"
        "\t\t\t\t\t\t\tSELECT loid FROM pg_largeobject INTO myoid;\n"
        "\t\t\t\t\t\t\tEXECUTE 'COMMENT ON LARGE OBJECT ' || myoid || ' IS ''comment on large object'';';\n"
        "\t\t\t\t\t\t END;\n"
        "\t\t\t\t\t\t $$;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "COMMENT ON LARGE OBJECT "),
                ("rx", r"[0-9]+"),
                ("lit", " IS 'comment on large object';"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            "column_inserts": 1,
            "data_only": 1,
            "inserts": 1,
            "no_schema": 1,
            "section_data": 1,
            "test_schema_plus_large_objects": 1,
        },
        "unlike": {
            "no_large_objects": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
        },
    }
    tests["COMMENT ON POLICY p1"] = {
        "create_order": 55,
        "create_sql": "COMMENT ON POLICY p1 ON dump_test.test_table\n"
        "\t\t\t\t\t   IS 'comment on policy';",
        "regexp": re.compile(
            r"^COMMENT ON POLICY p1 ON dump_test.test_table IS 'comment on policy';",
            re.MULTILINE,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_post_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "no_policies": 1,
            "no_policies_restore": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON PUBLICATION pub1"] = {
        "create_order": 55,
        "create_sql": "COMMENT ON PUBLICATION pub1\n"
        "\t\t\t\t\t   IS 'comment on publication';",
        "regexp": re.compile(
            r"^COMMENT ON PUBLICATION pub1 IS 'comment on publication';", re.MULTILINE
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["COMMENT ON SUBSCRIPTION sub1"] = {
        "create_order": 55,
        "create_sql": "COMMENT ON SUBSCRIPTION sub1\n"
        "\t\t\t\t\t   IS 'comment on subscription';",
        "regexp": re.compile(
            r"^COMMENT ON SUBSCRIPTION sub1 IS 'comment on subscription';", re.MULTILINE
        ),
        "like": {**_full(), "section_post_data": 1},
        "unlike": {
            "no_subscriptions": 1,
            "no_subscriptions_restore": 1,
        },
    }

    tests["COMMENT ON TEXT SEARCH CONFIGURATION dump_test.alt_ts_conf1"] = {
        "create_order": 84,
        "create_sql": "COMMENT ON TEXT SEARCH CONFIGURATION dump_test.alt_ts_conf1\n"
        "\t\t\t\t\t   IS 'comment on text search configuration';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON TEXT SEARCH CONFIGURATION dump_test.alt_ts_conf1 IS 'comment on text search configuration';",
                ),
            ],
            re.MULTILINE,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON TEXT SEARCH DICTIONARY dump_test.alt_ts_dict1"] = {
        "create_order": 84,
        "create_sql": "COMMENT ON TEXT SEARCH DICTIONARY dump_test.alt_ts_dict1\n"
        "\t\t\t\t\t   IS 'comment on text search dictionary';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON TEXT SEARCH DICTIONARY dump_test.alt_ts_dict1 IS 'comment on text search dictionary';",
                ),
            ],
            re.MULTILINE,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON TEXT SEARCH PARSER dump_test.alt_ts_prs1"] = {
        "create_order": 84,
        "create_sql": "COMMENT ON TEXT SEARCH PARSER dump_test.alt_ts_prs1\n"
        "\t\t\t\t\t   IS 'comment on text search parser';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON TEXT SEARCH PARSER dump_test.alt_ts_prs1 IS 'comment on text search parser';",
                ),
            ],
            re.MULTILINE,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON TEXT SEARCH TEMPLATE dump_test.alt_ts_temp1"] = {
        "create_order": 84,
        "create_sql": "COMMENT ON TEXT SEARCH TEMPLATE dump_test.alt_ts_temp1\n"
        "\t\t\t\t\t   IS 'comment on text search template';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON TEXT SEARCH TEMPLATE dump_test.alt_ts_temp1 IS 'comment on text search template';",
                ),
            ],
            re.MULTILINE,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON TYPE dump_test.planets - ENUM"] = {
        "create_order": 68,
        "create_sql": "COMMENT ON TYPE dump_test.planets\n"
        "\t\t\t\t\t   IS 'comment on enum type';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "COMMENT ON TYPE dump_test.planets IS 'comment on enum type';"),
            ],
            re.MULTILINE,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON TYPE dump_test.textrange - RANGE"] = {
        "create_order": 69,
        "create_sql": "COMMENT ON TYPE dump_test.textrange\n"
        "\t\t\t\t\t   IS 'comment on range type';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON TYPE dump_test.textrange IS 'comment on range type';",
                ),
            ],
            re.MULTILINE,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON TYPE dump_test.int42 - Regular"] = {
        "create_order": 70,
        "create_sql": "COMMENT ON TYPE dump_test.int42\n"
        "\t\t\t\t\t   IS 'comment on regular type';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON TYPE dump_test.int42 IS 'comment on regular type';",
                ),
            ],
            re.MULTILINE,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON TYPE dump_test.undefined - Undefined"] = {
        "create_order": 71,
        "create_sql": "COMMENT ON TYPE dump_test.undefined\n"
        "\t\t\t\t\t   IS 'comment on undefined type';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON TYPE dump_test.undefined IS 'comment on undefined type';",
                ),
            ],
            re.MULTILINE,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COPY test_table"] = {
        "create_order": 4,
        "create_sql": "INSERT INTO dump_test.test_table (col1) "
        "SELECT generate_series FROM generate_series(1,9);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COPY dump_test.test_table (col1, col2, col3, col4) FROM stdin;",
                ),
                ("rx", r"\n(?:\d\t\\N\t\\N\t\\N\n){9}\\\.\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "data_only": 1,
            "no_schema": 1,
            "only_dump_test_table": 1,
            "section_data": 1,
        },
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "exclude_test_table_data": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COPY fk_reference_test_table"] = {
        "create_order": 22,
        "create_sql": "INSERT INTO dump_test.fk_reference_test_table (col1) "
        "SELECT generate_series FROM generate_series(1,5);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "COPY dump_test.fk_reference_test_table (col1) FROM stdin;"),
                ("rx", r"\n(?:\d\n){5}\\\.\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "data_only": 1,
            "exclude_test_table": 1,
            "exclude_test_table_data": 1,
            "no_schema": 1,
            "section_data": 1,
        },
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COPY fk_reference_test_table second"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COPY dump_test.test_table (col1, col2, col3, col4) FROM stdin;",
                ),
                ("rx", r"\n(?:\d\t\\N\t\\N\t\\N\n){9}\\\.\n.*"),
                ("lit", "COPY dump_test.fk_reference_test_table (col1) FROM stdin;"),
                ("rx", r"\n(?:\d\n){5}\\\.\n"),
            ],
            XMS,
        ),
        "like": {
            "data_only": 1,
            "no_schema": 1,
        },
    }

    tests["COPY test_second_table"] = {
        "create_order": 7,
        "create_sql": "INSERT INTO dump_test.test_second_table (col1, col2) "
        "SELECT generate_series, generate_series::text "
        "FROM generate_series(1,9);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "COPY dump_test.test_second_table (col1, col2) FROM stdin;"),
                ("rx", r"\n(?:\d\t\d\n){9}\\\.\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "data_only": 1,
            "no_schema": 1,
            "section_data": 1,
        },
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COPY test_third_table"] = {
        "create_order": 7,
        "create_sql": "INSERT INTO dump_test.test_third_table VALUES (123, DEFAULT, 456);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", 'COPY dump_test.test_third_table (f1, "F3") FROM stdin;'),
                ("rx", r"\n123\t456\n\\\.\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "data_only": 1,
            "no_schema": 1,
            "section_data": 1,
        },
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COPY test_fourth_table"] = {
        "create_order": 7,
        "create_sql": "INSERT INTO dump_test.test_fourth_table DEFAULT VALUES;"
        "INSERT INTO dump_test.test_fourth_table DEFAULT VALUES;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "COPY dump_test.test_fourth_table  FROM stdin;"),
                ("rx", r"\n\n\n\\\.\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "data_only": 1,
            "no_schema": 1,
            "section_data": 1,
        },
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COPY test_fifth_table"] = {
        "create_order": 54,
        "create_sql": "INSERT INTO dump_test.test_fifth_table VALUES (NULL, true, false, '11001'::bit(5), 'NaN');",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COPY dump_test.test_fifth_table (col1, col2, col3, col4, col5) FROM stdin;",
                ),
                ("rx", r"\n\\N\tt\tf\t11001\tNaN\n\\\.\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "data_only": 1,
            "no_schema": 1,
            "section_data": 1,
        },
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COPY test_table_identity"] = {
        "create_order": 54,
        "create_sql": "INSERT INTO dump_test.test_table_identity (col2) VALUES ('test');",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "COPY dump_test.test_table_identity (col1, col2) FROM stdin;"),
                ("rx", r"\n1\ttest\n\\\.\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "data_only": 1,
            "no_schema": 1,
            "section_data": 1,
        },
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["INSERT INTO test_table"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "rx",
                    r"(?:INSERT\ INTO\ dump_test\.test_table\ \(col1,\ col2,\ col3,\ col4\)\ VALUES\ \(\d,\ NULL,\ NULL,\ NULL\);\n){9}",
                ),
            ],
            XM,
        ),
        "like": {"column_inserts": 1},
    }
    tests["test_table with 4-row INSERTs"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "rx",
                    r"(?:"
                    r"INSERT\ INTO\ dump_test\.test_table\ VALUES\n"
                    r"(?:\t\(\d,\ NULL,\ NULL,\ NULL\),\n){3}"
                    r"\t\(\d,\ NULL,\ NULL,\ NULL\);\n"
                    r"){2}"
                    r"INSERT\ INTO\ dump_test\.test_table\ VALUES\n"
                    r"\t\(\d,\ NULL,\ NULL,\ NULL\);",
                ),
            ],
            XM,
        ),
        "like": {"rows_per_insert": 1},
    }
    tests["INSERT INTO test_second_table"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "rx",
                    r"(?:INSERT\ INTO\ dump_test\.test_second_table\ \(col1,\ col2\)\ VALUES\ \(\d,\ '\d'\);\n){9}",
                ),
            ],
            XM,
        ),
        "like": {"column_inserts": 1},
    }
    tests["INSERT INTO test_third_table (colnames)"] = {
        "regexp": re.compile(
            r'^INSERT INTO dump_test\.test_third_table \(f1, "F3"\) VALUES \(123, 456\);\n',
            re.MULTILINE,
        ),
        "like": {"column_inserts": 1},
    }
    tests["INSERT INTO test_third_table"] = {
        "regexp": re.compile(
            r"^INSERT INTO dump_test\.test_third_table VALUES \(123, DEFAULT, 456, DEFAULT\);\n",
            re.MULTILINE,
        ),
        "like": {"inserts": 1},
    }
    tests["INSERT INTO test_fourth_table"] = {
        "regexp": re.compile(
            r"^(?:INSERT INTO dump_test\.test_fourth_table DEFAULT VALUES;\n){2}",
            re.MULTILINE,
        ),
        "like": {"column_inserts": 1, "inserts": 1, "rows_per_insert": 1},
    }
    tests["INSERT INTO test_fifth_table"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "INSERT INTO dump_test.test_fifth_table (col1, col2, col3, col4, col5) VALUES (NULL, true, false, B'11001', 'NaN');",
                ),
            ],
            re.MULTILINE,
        ),
        "like": {"column_inserts": 1},
    }
    tests["INSERT INTO test_table_identity"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "INSERT INTO dump_test.test_table_identity (col1, col2) OVERRIDING SYSTEM VALUE VALUES (1, 'test');",
                ),
            ],
            re.MULTILINE,
        ),
        "like": {"column_inserts": 1},
    }

    tests["CREATE ROLE regress_dump_test_role"] = {
        "create_order": 1,
        "create_sql": "CREATE ROLE regress_dump_test_role;",
        "regexp": re.compile(r"^CREATE ROLE regress_dump_test_role;", re.MULTILINE),
        "like": {
            "pg_dumpall_dbprivs": 1,
            "pg_dumpall_exclude": 1,
            "pg_dumpall_globals": 1,
            "pg_dumpall_globals_clean": 1,
        },
    }
    tests["CREATE ROLE regress_quoted..."] = {
        "create_order": 1,
        "create_sql": 'CREATE ROLE "regress_quoted  \\"" role";',
        "regexp": re.compile(
            r'^CREATE ROLE "regress_quoted  \\"" role";', re.MULTILINE
        ),
        "like": {
            "pg_dumpall_dbprivs": 1,
            "pg_dumpall_exclude": 1,
            "pg_dumpall_globals": 1,
            "pg_dumpall_globals_clean": 1,
        },
    }
    tests["newline of table name in comment"] = {
        "create_sql": '-- meet getPartitioningInfo() "unsafe" condition\n'
        "\t\t\t\t\t\t CREATE TYPE pp_colors AS\n"
        "\t\t\t\t\t\t\tENUM ('green', 'blue', 'black');\n"
        "\t\t\t\t\t\t CREATE TABLE pp_enumpart (a pp_colors)\n"
        "\t\t\t\t\t\t\tPARTITION BY HASH (a);\n"
        "\t\t\t\t\t\t CREATE TABLE pp_enumpart1 PARTITION OF pp_enumpart\n"
        "\t\t\t\t\t\t\tFOR VALUES WITH (MODULUS 2, REMAINDER 0);\n"
        "\t\t\t\t\t\t CREATE TABLE pp_enumpart2 PARTITION OF pp_enumpart\n"
        "\t\t\t\t\t\t\tFOR VALUES WITH (MODULUS 2, REMAINDER 1);\n"
        "\t\t\t\t\t\t ALTER TABLE pp_enumpart\n"
        '\t\t\t\t\t\t\tRENAME TO "pp_enumpart\nattack";',
        "regexp": re.compile(r"\n--[^\n]*\nattack", re.DOTALL),
        "like": {},
    }
    tests["CREATE TABLESPACE regress_dump_tablespace"] = {
        "create_order": 2,
        "create_sql": "\n"
        "\t\t    SET allow_in_place_tablespaces = on;\n"
        "\t\t\tCREATE TABLESPACE regress_dump_tablespace\n"
        "\t\t\tOWNER regress_dump_test_role LOCATION ''",
        "regexp": re.compile(
            r"^CREATE TABLESPACE regress_dump_tablespace OWNER regress_dump_test_role LOCATION '';",
            re.MULTILINE,
        ),
        "like": {
            "pg_dumpall_dbprivs": 1,
            "pg_dumpall_exclude": 1,
            "pg_dumpall_globals": 1,
            "pg_dumpall_globals_clean": 1,
        },
    }
    tests["CREATE DATABASE regression_invalid..."] = {
        "create_order": 1,
        "create_sql": "\n"
        "\t\t    CREATE DATABASE regression_invalid;\n"
        "\t\t\tUPDATE pg_database SET datconnlimit = -2 WHERE datname = 'regression_invalid'",
        "regexp": re.compile(r"^CREATE DATABASE regression_invalid", re.MULTILINE),
        "like": {},
    }
    tests["CREATE ACCESS METHOD gist2"] = {
        "create_order": 52,
        "create_sql": "CREATE ACCESS METHOD gist2 TYPE INDEX HANDLER gisthandler;",
        "regexp": re.compile(
            r"CREATE ACCESS METHOD gist2 TYPE INDEX HANDLER gisthandler;", re.MULTILINE
        ),
        "like": {**_full(), "section_pre_data": 1},
    }
    tests['CREATE COLLATION test0 FROM "C"'] = {
        "create_order": 76,
        "create_sql": 'CREATE COLLATION test0 FROM "C";',
        "regexp": re.compile(
            r"CREATE COLLATION public.test0 \(provider = libc, locale = 'C'(, version = '[^']*')?\);",
            re.MULTILINE,
        ),
        "collation": 1,
        "like": {**_full(), "section_pre_data": 1},
    }
    tests["CREATE COLLATION icu_collation"] = {
        "create_order": 76,
        "create_sql": "CREATE COLLATION icu_collation (PROVIDER = icu, LOCALE = 'en-US-u-va-posix');",
        "regexp": re.compile(
            r"CREATE COLLATION public.icu_collation \(provider = icu, locale = 'en-US-u-va-posix'(, version = '[^']*')?\);",
            re.MULTILINE,
        ),
        "icu": 1,
        "like": {**_full(), "section_pre_data": 1},
    }
    tests["CREATE CAST FOR timestamptz"] = {
        "create_order": 51,
        "create_sql": "CREATE CAST (timestamptz AS interval) WITH FUNCTION age(timestamptz) AS ASSIGNMENT;",
        "regexp": re.compile(
            r"CREATE CAST \(timestamp with time zone AS interval\) WITH FUNCTION pg_catalog\.age\(timestamp with time zone\) AS ASSIGNMENT;",
            re.MULTILINE,
        ),
        "like": {**_full(), "section_pre_data": 1},
    }
    tests["CREATE DATABASE postgres"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE DATABASE postgres WITH TEMPLATE = template0 "),
                ("rx", r".+;"),
            ],
            XM,
        ),
        "like": {"createdb": 1},
    }
    tests["CREATE DATABASE dump_test"] = {
        "create_order": 47,
        "create_sql": "CREATE DATABASE dump_test;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE DATABASE dump_test WITH TEMPLATE = template0 "),
                ("rx", r".+;"),
            ],
            XM,
        ),
        "like": {"pg_dumpall_dbprivs": 1},
    }
    tests["CREATE DATABASE dump_test2 LOCALE = 'C'"] = {
        "create_order": 47,
        "create_sql": "CREATE DATABASE dump_test2 LOCALE = 'C' TEMPLATE = template0;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE DATABASE dump_test2 "),
                ("rx", r".*"),
                ("lit", "LOCALE = 'C';"),
            ],
            XM,
        ),
        "like": {"pg_dumpall_dbprivs": 1},
    }
    tests["CREATE EXTENSION ... plpgsql"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;",
                ),
            ],
            XM,
        ),
        "like": {},
    }

    tests["CREATE AGGREGATE dump_test.newavg"] = {
        "create_order": 25,
        "create_sql": "CREATE AGGREGATE dump_test.newavg (\n"
        "\t\t\t\t\t\t  sfunc = int4_avg_accum,\n"
        "\t\t\t\t\t\t  basetype = int4,\n"
        "\t\t\t\t\t\t  stype = _int8,\n"
        "\t\t\t\t\t\t  finalfunc = int8_avg,\n"
        "\t\t\t\t\t\t  finalfunc_modify = shareable,\n"
        "\t\t\t\t\t\t  initcond1 = '{0,0}'\n"
        "\t\t\t\t\t   );",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE AGGREGATE dump_test.newavg(integer) ("),
                ("rx", r"\n\s+"),
                ("lit", "SFUNC = int4_avg_accum,"),
                ("rx", r"\n\s+"),
                ("lit", "STYPE = bigint[],"),
                ("rx", r"\n\s+"),
                ("lit", "INITCOND = '{0,0}',"),
                ("rx", r"\n\s+"),
                ("lit", "FINALFUNC = int8_avg,"),
                ("rx", r"\n\s+"),
                ("lit", "FINALFUNC_MODIFY = SHAREABLE"),
                ("rx", r"\n\);"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "exclude_test_table": 1,
            "section_pre_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE CONVERSION dump_test.test_conversion"] = {
        "create_order": 78,
        "create_sql": "CREATE DEFAULT CONVERSION dump_test.test_conversion FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE DEFAULT CONVERSION dump_test.test_conversion FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE DOMAIN dump_test.us_postal_code"] = {
        "create_order": 29,
        "create_sql": "CREATE DOMAIN dump_test.us_postal_code AS TEXT\n"
        '\t\t               COLLATE "C"\n'
        "\t\t\t\t\t   DEFAULT '10014'\n"
        "\t\t\t\t\t   CONSTRAINT nn NOT NULL\n"
        "\t\t\t\t\t   CHECK(VALUE ~ '^\\d{5}$' OR\n"
        "\t\t\t\t\t\t\t VALUE ~ '^\\d{5}-\\d{4}$');\n"
        "\t\t\t\t\t   COMMENT ON CONSTRAINT nn\n"
        "\t\t\t\t\t\t ON DOMAIN dump_test.us_postal_code IS 'not null';\n"
        "\t\t\t\t\t   COMMENT ON CONSTRAINT us_postal_code_check\n"
        "\t\t\t\t\t\t ON DOMAIN dump_test.us_postal_code IS 'check it';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE DOMAIN dump_test.us_postal_code AS text COLLATE pg_catalog.\"C\" CONSTRAINT nn NOT NULL DEFAULT '10014'::text",
                ),
                ("rx", r"\n\s+"),
                ("lit", "CONSTRAINT us_postal_code_check CHECK "),
                ("lit", "(((VALUE ~ '^\\d{5}"),
                ("rx", r"\$"),
                ("lit", "'::text) OR (VALUE ~ '^\\d{5}-\\d{4}"),
                ("rx", r"\$"),
                ("lit", "'::text)));"),
                ("rx", r"(.|\n)*"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON CONSTRAINT ON DOMAIN (1)"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON CONSTRAINT nn ON DOMAIN dump_test.us_postal_code IS 'not null';",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["COMMENT ON CONSTRAINT ON DOMAIN (2)"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COMMENT ON CONSTRAINT us_postal_code_check ON DOMAIN dump_test.us_postal_code IS 'check it';",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE FUNCTION dump_test.pltestlang_call_handler"] = {
        "create_order": 17,
        "create_sql": "CREATE FUNCTION dump_test.pltestlang_call_handler()\n"
        "\t\t\t\t\t   RETURNS LANGUAGE_HANDLER AS '$libdir/plpgsql',\n"
        "\t\t\t\t\t   'plpgsql_call_handler' LANGUAGE C;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE FUNCTION dump_test.pltestlang_call_handler() "),
                ("lit", "RETURNS language_handler"),
                ("rx", r"\n\s+"),
                ("lit", "LANGUAGE c"),
                ("rx", r"\n\s+AS\ \'\$"),
                ("lit", "libdir/plpgsql', 'plpgsql_call_handler';"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE FUNCTION dump_test.trigger_func"] = {
        "create_order": 30,
        "create_sql": "CREATE FUNCTION dump_test.trigger_func()\n"
        "\t\t\t\t\t   RETURNS trigger LANGUAGE plpgsql\n"
        "\t\t\t\t\t   AS $$ BEGIN RETURN NULL; END;$$;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE FUNCTION dump_test.trigger_func() RETURNS trigger"),
                ("rx", r"\n\s+"),
                ("lit", "LANGUAGE plpgsql"),
                ("rx", r"\n\s+AS\ \$\$"),
                ("lit", " BEGIN RETURN NULL; END;"),
                ("rx", r"\$\$;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE FUNCTION dump_test.event_trigger_func"] = {
        "create_order": 32,
        "create_sql": "CREATE FUNCTION dump_test.event_trigger_func()\n"
        "\t\t\t\t\t   RETURNS event_trigger LANGUAGE plpgsql\n"
        "\t\t\t\t\t   AS $$ BEGIN RETURN; END;$$;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE FUNCTION dump_test.event_trigger_func() RETURNS event_trigger",
                ),
                ("rx", r"\n\s+"),
                ("lit", "LANGUAGE plpgsql"),
                ("rx", r"\n\s+AS\ \$\$"),
                ("lit", " BEGIN RETURN; END;"),
                ("rx", r"\$\$;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE OPERATOR FAMILY dump_test.op_family"] = {
        "create_order": 73,
        "create_sql": "CREATE OPERATOR FAMILY dump_test.op_family USING btree;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE OPERATOR FAMILY dump_test.op_family USING btree;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }

    tests["CREATE OPERATOR CLASS dump_test.op_class"] = {
        "create_order": 74,
        "create_sql": "CREATE OPERATOR CLASS dump_test.op_class\n"
        "\t\t                 FOR TYPE bigint USING btree FAMILY dump_test.op_family\n"
        "\t\t\t\t\t\t AS STORAGE bigint,\n"
        "\t\t\t\t\t\t OPERATOR 1 <(bigint,bigint),\n"
        "\t\t\t\t\t\t OPERATOR 2 <=(bigint,bigint),\n"
        "\t\t\t\t\t\t OPERATOR 3 =(bigint,bigint),\n"
        "\t\t\t\t\t\t OPERATOR 4 >=(bigint,bigint),\n"
        "\t\t\t\t\t\t OPERATOR 5 >(bigint,bigint),\n"
        "\t\t\t\t\t\t FUNCTION 1 btint8cmp(bigint,bigint),\n"
        "\t\t\t\t\t\t FUNCTION 2 btint8sortsupport(internal),\n"
        "\t\t\t\t\t\t FUNCTION 4 btequalimage(oid);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE OPERATOR CLASS dump_test.op_class"),
                ("rx", r"\n\s+"),
                ("lit", "FOR TYPE bigint USING btree FAMILY dump_test.op_family AS"),
                ("rx", r"\n\s+"),
                ("lit", "OPERATOR 1 <(bigint,bigint) ,"),
                ("rx", r"\n\s+"),
                ("lit", "OPERATOR 2 <=(bigint,bigint) ,"),
                ("rx", r"\n\s+"),
                ("lit", "OPERATOR 3 =(bigint,bigint) ,"),
                ("rx", r"\n\s+"),
                ("lit", "OPERATOR 4 >=(bigint,bigint) ,"),
                ("rx", r"\n\s+"),
                ("lit", "OPERATOR 5 >(bigint,bigint) ,"),
                ("rx", r"\n\s+"),
                ("lit", "FUNCTION 1 (bigint, bigint) btint8cmp(bigint,bigint);"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE OPERATOR CLASS dump_test.op_class_custom"] = {
        "create_order": 74,
        "create_sql": "CREATE OPERATOR dump_test.~~ (\n"
        "\t\t\t\t\t\t\t PROCEDURE = int4eq,\n"
        "\t\t\t\t\t\t\t LEFTARG = int,\n"
        "\t\t\t\t\t\t\t RIGHTARG = int);\n"
        "\t\t\t\t\t\t CREATE OPERATOR CLASS dump_test.op_class_custom\n"
        "\t\t\t\t\t\t\t FOR TYPE int USING btree AS\n"
        "\t\t\t\t\t\t\t OPERATOR 3 dump_test.~~;\n"
        "\t\t\t\t\t\t CREATE TYPE dump_test.range_type_custom AS RANGE (\n"
        "\t\t\t\t\t\t\t subtype = int,\n"
        "\t\t\t\t\t\t\t subtype_opclass = dump_test.op_class_custom);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE OPERATOR dump_test.~~ ("),
                ("rx", r"\n.+"),
                (
                    "lit",
                    "CREATE OPERATOR FAMILY dump_test.op_class_custom USING btree;",
                ),
                ("rx", r"\n.+"),
                ("lit", "CREATE OPERATOR CLASS dump_test.op_class_custom"),
                ("rx", r"\n\s+"),
                (
                    "lit",
                    "FOR TYPE integer USING btree FAMILY dump_test.op_class_custom AS",
                ),
                ("rx", r"\n\s+"),
                ("lit", "OPERATOR 3 dump_test.~~(integer,integer);"),
                ("rx", r"\n.+"),
                ("lit", "CREATE TYPE dump_test.range_type_custom AS RANGE ("),
                ("rx", r"\n\s+"),
                ("lit", "subtype = integer,"),
                ("rx", r"\n\s+"),
                ("lit", "multirange_type_name = dump_test.multirange_type_custom,"),
                ("rx", r"\n\s+"),
                ("lit", "subtype_opclass = dump_test.op_class_custom"),
                ("rx", r"\n"),
                ("lit", ");"),
            ],
            XMS,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE OPERATOR CLASS dump_test.op_class_empty"] = {
        "create_order": 89,
        "create_sql": "CREATE OPERATOR CLASS dump_test.op_class_empty\n"
        "\t\t                 FOR TYPE bigint USING btree FAMILY dump_test.op_family\n"
        "\t\t\t\t\t\t AS STORAGE bigint;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE OPERATOR CLASS dump_test.op_class_empty"),
                ("rx", r"\n\s+"),
                ("lit", "FOR TYPE bigint USING btree FAMILY dump_test.op_family AS"),
                ("rx", r"\n\s+"),
                ("lit", "STORAGE bigint;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE EVENT TRIGGER test_event_trigger"] = {
        "create_order": 33,
        "create_sql": "CREATE EVENT TRIGGER test_event_trigger\n"
        "\t\t\t\t\t   ON ddl_command_start\n"
        "\t\t\t\t\t   EXECUTE FUNCTION dump_test.event_trigger_func();",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE EVENT TRIGGER test_event_trigger "),
                ("lit", "ON ddl_command_start"),
                ("rx", r"\n\s+"),
                ("lit", "EXECUTE FUNCTION dump_test.event_trigger_func();"),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["CREATE TRIGGER test_trigger"] = {
        "create_order": 31,
        "create_sql": "CREATE TRIGGER test_trigger\n"
        "\t\t\t\t\t   BEFORE INSERT ON dump_test.test_table\n"
        "\t\t\t\t\t   FOR EACH ROW WHEN (NEW.col1 > 10)\n"
        "\t\t\t\t\t   EXECUTE FUNCTION dump_test.trigger_func();",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE TRIGGER test_trigger BEFORE INSERT ON dump_test.test_table ",
                ),
                ("lit", "FOR EACH ROW WHEN ((new.col1 > 10)) "),
                ("lit", "EXECUTE FUNCTION dump_test.trigger_func();"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_post_data": 1,
        },
        "unlike": {
            "exclude_test_table": 1,
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TYPE dump_test.planets AS ENUM"] = {
        "create_order": 37,
        "create_sql": "CREATE TYPE dump_test.planets\n"
        "\t\t\t\t\t   AS ENUM ( 'venus', 'earth', 'mars' );",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TYPE dump_test.planets AS ENUM ("),
                ("rx", r"\n\s+'venus',\n\s+'earth',\n\s+'mars'\n\);"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TYPE dump_test.planets AS ENUM pg_upgrade"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TYPE dump_test.planets AS ENUM ("),
                ("rx", r"\n\);.*^"),
                ("lit", "ALTER TYPE dump_test.planets ADD VALUE 'venus';"),
                ("rx", r"\n.*^"),
                ("lit", "ALTER TYPE dump_test.planets ADD VALUE 'earth';"),
                ("rx", r"\n.*^"),
                ("lit", "ALTER TYPE dump_test.planets ADD VALUE 'mars';"),
                ("rx", r"\n"),
            ],
            XMS,
        ),
        "like": {"binary_upgrade": 1},
    }
    tests["CREATE TYPE dump_test.textrange AS RANGE"] = {
        "create_order": 38,
        "create_sql": "CREATE TYPE dump_test.textrange\n"
        '\t\t\t\t\t   AS RANGE (subtype=text, collation="C");',
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TYPE dump_test.textrange AS RANGE ("),
                ("rx", r"\n\s+"),
                ("lit", "subtype = text,"),
                ("rx", r"\n\s+"),
                ("lit", "multirange_type_name = dump_test.textmultirange,"),
                ("rx", r"\n\s+"),
                ("lit", 'collation = pg_catalog."C"'),
                ("rx", r"\n\);"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TYPE dump_test.int42"] = {
        "create_order": 39,
        "create_sql": "CREATE TYPE dump_test.int42;",
        "regexp": _qr(
            [("rx", r"^"), ("lit", "CREATE TYPE dump_test.int42;")], re.MULTILINE
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }

    tests["CREATE TEXT SEARCH CONFIGURATION dump_test.alt_ts_conf1"] = {
        "create_order": 80,
        "create_sql": "CREATE TEXT SEARCH CONFIGURATION dump_test.alt_ts_conf1 (copy=english);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TEXT SEARCH CONFIGURATION dump_test.alt_ts_conf1 ("),
                ("rx", r"\n\s+"),
                ("lit", 'PARSER = pg_catalog."default" );'),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    _ts_conf_maps = [
        ("asciiword", "english_stem"),
        ("word", "english_stem"),
        ("numword", "simple"),
        ("email", "simple"),
        ("url", "simple"),
        ("host", "simple"),
        ("sfloat", "simple"),
        ("version", "simple"),
        ("hword_numpart", "simple"),
        ("hword_part", "english_stem"),
        ("hword_asciipart", "english_stem"),
        ("numhword", "simple"),
        ("asciihword", "english_stem"),
        ("hword", "english_stem"),
        ("url_path", "simple"),
        ("file", "simple"),
        ('"float"', "simple"),
        ('"int"', "simple"),
        ("uint", "simple"),
    ]
    _ts_conf_parts: List[_Segment] = [("rx", r"^")]
    for _tok, _dict in _ts_conf_maps:
        _ts_conf_parts += [
            ("lit", "ALTER TEXT SEARCH CONFIGURATION dump_test.alt_ts_conf1"),
            ("rx", r"\n\s+"),
            ("lit", f"ADD MAPPING FOR {_tok} WITH {_dict};"),
            ("rx", r"\n\n"),
        ]
    tests["ALTER TEXT SEARCH CONFIGURATION dump_test.alt_ts_conf1 ..."] = {
        "regexp": _qr(_ts_conf_parts, XM),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TEXT SEARCH TEMPLATE dump_test.alt_ts_temp1"] = {
        "create_order": 81,
        "create_sql": "CREATE TEXT SEARCH TEMPLATE dump_test.alt_ts_temp1 (lexize=dsimple_lexize);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TEXT SEARCH TEMPLATE dump_test.alt_ts_temp1 ("),
                ("rx", r"\n\s+"),
                ("lit", "LEXIZE = dsimple_lexize );"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TEXT SEARCH PARSER dump_test.alt_ts_prs1"] = {
        "create_order": 82,
        "create_sql": "CREATE TEXT SEARCH PARSER dump_test.alt_ts_prs1\n"
        "\t\t(start = prsd_start, gettoken = prsd_nexttoken, end = prsd_end, lextypes = prsd_lextype);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TEXT SEARCH PARSER dump_test.alt_ts_prs1 ("),
                ("rx", r"\n\s+"),
                ("lit", "START = prsd_start,"),
                ("rx", r"\n\s+"),
                ("lit", "GETTOKEN = prsd_nexttoken,"),
                ("rx", r"\n\s+"),
                ("lit", "END = prsd_end,"),
                ("rx", r"\n\s+"),
                ("lit", "LEXTYPES = prsd_lextype );"),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TEXT SEARCH DICTIONARY dump_test.alt_ts_dict1"] = {
        "create_order": 83,
        "create_sql": "CREATE TEXT SEARCH DICTIONARY dump_test.alt_ts_dict1 (template=simple);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TEXT SEARCH DICTIONARY dump_test.alt_ts_dict1 ("),
                ("rx", r"\n\s+"),
                ("lit", "TEMPLATE = pg_catalog.simple );"),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE FUNCTION dump_test.int42_in"] = {
        "create_order": 40,
        "create_sql": "CREATE FUNCTION dump_test.int42_in(cstring)\n"
        "\t\t\t\t\t   RETURNS dump_test.int42 AS 'int4in'\n"
        "\t\t\t\t\t   LANGUAGE internal STRICT IMMUTABLE;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE FUNCTION dump_test.int42_in(cstring) RETURNS dump_test.int42",
                ),
                ("rx", r"\n\s+"),
                ("lit", "LANGUAGE internal IMMUTABLE STRICT"),
                ("rx", r"\n\s+AS\ \$\$int4in\$\$;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE FUNCTION dump_test.int42_out"] = {
        "create_order": 41,
        "create_sql": "CREATE FUNCTION dump_test.int42_out(dump_test.int42)\n"
        "\t\t\t\t\t   RETURNS cstring AS 'int4out'\n"
        "\t\t\t\t\t   LANGUAGE internal STRICT IMMUTABLE;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE FUNCTION dump_test.int42_out(dump_test.int42) RETURNS cstring",
                ),
                ("rx", r"\n\s+"),
                ("lit", "LANGUAGE internal IMMUTABLE STRICT"),
                ("rx", r"\n\s+AS\ \$\$int4out\$\$;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE FUNCTION ... SUPPORT"] = {
        "create_order": 41,
        "create_sql": "CREATE FUNCTION dump_test.func_with_support() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$ SUPPORT varchar_support;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE FUNCTION dump_test.func_with_support() RETURNS integer",
                ),
                ("rx", r"\n\s+"),
                ("lit", "LANGUAGE sql SUPPORT varchar_support"),
                ("rx", r"\n\s+AS\ \$\$"),
                ("lit", " SELECT 1 "),
                ("rx", r"\$\$;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["Check ordering of a function that depends on a primary key"] = {
        "create_order": 41,
        "create_sql": "\n"
        "\t\t\tCREATE TABLE dump_test.ordering_table (id int primary key, data int);\n"
        "\t\t\tCREATE FUNCTION dump_test.ordering_func ()\n"
        "\t\t\tRETURNS SETOF dump_test.ordering_table\n"
        "\t\t\tLANGUAGE sql BEGIN ATOMIC\n"
        "\t\t\tSELECT * FROM dump_test.ordering_table GROUP BY id; END;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER TABLE ONLY dump_test.ordering_table"),
                ("rx", r"\n\s+"),
                ("lit", "ADD CONSTRAINT ordering_table_pkey PRIMARY KEY (id);"),
                ("rx", r".*^"),
                ("lit", "CREATE FUNCTION dump_test.ordering_func"),
            ],
            XMS,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE PROCEDURE dump_test.ptest1"] = {
        "create_order": 41,
        "create_sql": "CREATE PROCEDURE dump_test.ptest1(a int)\n"
        "\t\t\t\t\t   LANGUAGE SQL AS $$ INSERT INTO dump_test.test_table (col1) VALUES (a) $$;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE PROCEDURE dump_test.ptest1(IN a integer)"),
                ("rx", r"\n\s+"),
                ("lit", "LANGUAGE sql"),
                ("rx", r"\n\s+AS\ \$\$"),
                ("lit", " INSERT INTO dump_test.test_table (col1) VALUES (a) "),
                ("rx", r"\$\$;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }

    tests["CREATE TYPE dump_test.int42 populated"] = {
        "create_order": 42,
        "create_sql": "CREATE TYPE dump_test.int42 (\n"
        "\t\t\t\t\t\t   internallength = 4,\n"
        "\t\t\t\t\t\t   input = dump_test.int42_in,\n"
        "\t\t\t\t\t\t   output = dump_test.int42_out,\n"
        "\t\t\t\t\t\t   alignment = int4,\n"
        "\t\t\t\t\t\t   default = 42,\n"
        "\t\t\t\t\t\t   passedbyvalue);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TYPE dump_test.int42 ("),
                ("rx", r"\n\s+"),
                ("lit", "INTERNALLENGTH = 4,"),
                ("rx", r"\n\s+"),
                ("lit", "INPUT = dump_test.int42_in,"),
                ("rx", r"\n\s+"),
                ("lit", "OUTPUT = dump_test.int42_out,"),
                ("rx", r"\n\s+"),
                ("lit", "DEFAULT = '42',"),
                ("rx", r"\n\s+"),
                ("lit", "ALIGNMENT = int4,"),
                ("rx", r"\n\s+"),
                ("lit", "STORAGE = plain,"),
                ("rx", r"\n\s+PASSEDBYVALUE\n\);"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TYPE dump_test.composite"] = {
        "create_order": 43,
        "create_sql": "CREATE TYPE dump_test.composite AS (\n"
        "\t\t\t\t\t\t   f1 int,\n"
        "\t\t\t\t\t\t   f2 dump_test.int42\n"
        "\t\t\t\t\t   );",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TYPE dump_test.composite AS ("),
                ("rx", r"\n\s+"),
                ("lit", "f1 integer,"),
                ("rx", r"\n\s+"),
                ("lit", "f2 dump_test.int42"),
                ("rx", r"\n\);"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TYPE dump_test.undefined"] = {
        "create_order": 39,
        "create_sql": "CREATE TYPE dump_test.undefined;",
        "regexp": _qr(
            [("rx", r"^"), ("lit", "CREATE TYPE dump_test.undefined;")], re.MULTILINE
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE FOREIGN DATA WRAPPER dummy"] = {
        "create_order": 35,
        "create_sql": "CREATE FOREIGN DATA WRAPPER dummy;",
        "regexp": re.compile(r"CREATE FOREIGN DATA WRAPPER dummy;", re.MULTILINE),
        "like": {**_full(), "section_pre_data": 1},
    }
    tests["CREATE SERVER s1 FOREIGN DATA WRAPPER dummy"] = {
        "create_order": 36,
        "create_sql": "CREATE SERVER s1 FOREIGN DATA WRAPPER dummy;",
        "regexp": re.compile(
            r"CREATE SERVER s1 FOREIGN DATA WRAPPER dummy;", re.MULTILINE
        ),
        "like": {**_full(), "section_pre_data": 1},
    }
    tests["CREATE FOREIGN TABLE dump_test.foreign_table SERVER s1"] = {
        "create_order": 88,
        "create_sql": "CREATE FOREIGN TABLE dump_test.foreign_table (c1 int options (column_name 'col1'))\n"
        "\t\t\t\t\t\tSERVER s1 OPTIONS (schema_name 'x1');",
        "regexp": _qr(
            [
                ("lit", "CREATE FOREIGN TABLE dump_test.foreign_table ("),
                ("rx", r"\n\s+"),
                ("lit", "c1 integer"),
                ("rx", r"\n"),
                ("lit", ")"),
                ("rx", r"\n"),
                ("lit", "SERVER s1"),
                ("rx", r"\n"),
                ("lit", "OPTIONS ("),
                ("rx", r"\n\s+"),
                ("lit", "schema_name 'x1'"),
                ("rx", r"\n"),
                ("lit", ");"),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE USER MAPPING FOR regress_dump_test_role SERVER s1"] = {
        "create_order": 86,
        "create_sql": "CREATE USER MAPPING FOR regress_dump_test_role SERVER s1;",
        "regexp": re.compile(
            r"CREATE USER MAPPING FOR regress_dump_test_role SERVER s1;", re.MULTILINE
        ),
        "like": {**_full(), "section_pre_data": 1},
    }
    tests["CREATE TRANSFORM FOR int"] = {
        "create_order": 34,
        "create_sql": "CREATE TRANSFORM FOR int LANGUAGE SQL (FROM SQL WITH FUNCTION prsd_lextype(internal), TO SQL WITH FUNCTION int4recv(internal));",
        "regexp": re.compile(
            r"CREATE TRANSFORM FOR integer LANGUAGE sql \(FROM SQL WITH FUNCTION pg_catalog\.prsd_lextype\(internal\), TO SQL WITH FUNCTION pg_catalog\.int4recv\(internal\)\);",
            re.MULTILINE,
        ),
        "like": {**_full(), "section_pre_data": 1},
    }
    tests["CREATE LANGUAGE pltestlang"] = {
        "create_order": 18,
        "create_sql": "CREATE LANGUAGE pltestlang\n"
        "\t\t\t\t\t   HANDLER dump_test.pltestlang_call_handler;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE PROCEDURAL LANGUAGE pltestlang "),
                ("lit", "HANDLER dump_test.pltestlang_call_handler;"),
            ],
            XM,
        ),
        "like": {**_full(), "section_pre_data": 1},
        "unlike": {"exclude_dump_test_schema": 1},
    }
    tests["CREATE MATERIALIZED VIEW matview"] = {
        "create_order": 20,
        "create_sql": "CREATE MATERIALIZED VIEW dump_test.matview (col1) AS\n"
        "\t\t\t\t\t   SELECT col1 FROM dump_test.test_table;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE MATERIALIZED VIEW dump_test.matview AS"),
                ("rx", r"\n\s+"),
                ("lit", "SELECT col1"),
                ("rx", r"\n\s+"),
                ("lit", "FROM dump_test.test_table"),
                ("rx", r"\n\s+"),
                ("lit", "WITH NO DATA;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE MATERIALIZED VIEW matview_second"] = {
        "create_order": 21,
        "create_sql": "CREATE MATERIALIZED VIEW\n"
        "\t\t\t\t\t\t   dump_test.matview_second (col1) AS\n"
        "\t\t\t\t\t\t   SELECT * FROM dump_test.matview;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE MATERIALIZED VIEW dump_test.matview_second AS"),
                ("rx", r"\n\s+"),
                ("lit", "SELECT col1"),
                ("rx", r"\n\s+"),
                ("lit", "FROM dump_test.matview"),
                ("rx", r"\n\s+"),
                ("lit", "WITH NO DATA;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE MATERIALIZED VIEW matview_third"] = {
        "create_order": 58,
        "create_sql": "CREATE MATERIALIZED VIEW\n"
        "\t\t\t\t\t\t   dump_test.matview_third (col1) AS\n"
        "\t\t\t\t\t\t   SELECT * FROM dump_test.matview_second WITH NO DATA;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE MATERIALIZED VIEW dump_test.matview_third AS"),
                ("rx", r"\n\s+"),
                ("lit", "SELECT col1"),
                ("rx", r"\n\s+"),
                ("lit", "FROM dump_test.matview_second"),
                ("rx", r"\n\s+"),
                ("lit", "WITH NO DATA;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE MATERIALIZED VIEW matview_fourth"] = {
        "create_order": 59,
        "create_sql": "CREATE MATERIALIZED VIEW\n"
        "\t\t\t\t\t\t   dump_test.matview_fourth (col1) AS\n"
        "\t\t\t\t\t\t   SELECT * FROM dump_test.matview_third WITH NO DATA;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE MATERIALIZED VIEW dump_test.matview_fourth AS"),
                ("rx", r"\n\s+"),
                ("lit", "SELECT col1"),
                ("rx", r"\n\s+"),
                ("lit", "FROM dump_test.matview_third"),
                ("rx", r"\n\s+"),
                ("lit", "WITH NO DATA;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["Check ordering of a matview that depends on a primary key"] = {
        "create_order": 42,
        "create_sql": "\n"
        "\t\t\tCREATE MATERIALIZED VIEW dump_test.ordering_view AS\n"
        "\t\t\t\tSELECT * FROM dump_test.ordering_table GROUP BY id;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER TABLE ONLY dump_test.ordering_table"),
                ("rx", r"\n\s+"),
                ("lit", "ADD CONSTRAINT ordering_table_pkey PRIMARY KEY (id);"),
                ("rx", r".*^"),
                ("lit", "CREATE MATERIALIZED VIEW dump_test.ordering_view AS"),
                ("rx", r"\n\s+"),
                ("lit", "SELECT id,"),
            ],
            XMS,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }

    tests["CREATE POLICY p1 ON test_table"] = {
        "create_order": 22,
        "create_sql": "CREATE POLICY p1 ON dump_test.test_table\n"
        "\t\t\t\t\t\t   USING (true)\n"
        "\t\t\t\t\t\t   WITH CHECK (true);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE POLICY p1 ON dump_test.test_table "),
                ("lit", "USING (true) WITH CHECK (true);"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_post_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "no_policies": 1,
            "no_policies_restore": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE POLICY p2 ON test_table FOR SELECT"] = {
        "create_order": 24,
        "create_sql": "CREATE POLICY p2 ON dump_test.test_table\n"
        "\t\t\t\t\t\t   FOR SELECT TO regress_dump_test_role USING (true);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE POLICY p2 ON dump_test.test_table FOR SELECT TO regress_dump_test_role ",
                ),
                ("lit", "USING (true);"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_post_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "no_policies": 1,
            "no_policies_restore": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE POLICY p3 ON test_table FOR INSERT"] = {
        "create_order": 25,
        "create_sql": "CREATE POLICY p3 ON dump_test.test_table\n"
        "\t\t\t\t\t\t   FOR INSERT TO regress_dump_test_role WITH CHECK (true);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE POLICY p3 ON dump_test.test_table FOR INSERT "),
                ("lit", "TO regress_dump_test_role WITH CHECK (true);"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_post_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "no_policies": 1,
            "no_policies_restore": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE POLICY p4 ON test_table FOR UPDATE"] = {
        "create_order": 26,
        "create_sql": "CREATE POLICY p4 ON dump_test.test_table FOR UPDATE\n"
        "\t\t\t\t\t\t   TO regress_dump_test_role USING (true) WITH CHECK (true);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE POLICY p4 ON dump_test.test_table FOR UPDATE TO regress_dump_test_role ",
                ),
                ("lit", "USING (true) WITH CHECK (true);"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_post_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "no_policies": 1,
            "no_policies_restore": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE POLICY p5 ON test_table FOR DELETE"] = {
        "create_order": 27,
        "create_sql": "CREATE POLICY p5 ON dump_test.test_table\n"
        "\t\t\t\t\t\t   FOR DELETE TO regress_dump_test_role USING (true);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE POLICY p5 ON dump_test.test_table FOR DELETE "),
                ("lit", "TO regress_dump_test_role USING (true);"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_post_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "no_policies": 1,
            "no_policies_restore": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE POLICY p6 ON test_table AS RESTRICTIVE"] = {
        "create_order": 27,
        "create_sql": "CREATE POLICY p6 ON dump_test.test_table AS RESTRICTIVE\n"
        "\t\t\t\t\t\t   USING (false);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE POLICY p6 ON dump_test.test_table AS RESTRICTIVE "),
                ("lit", "USING (false);"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_post_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "no_policies": 1,
            "no_policies_restore": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE PROPERTY GRAPH propgraph"] = {
        "create_order": 20,
        "create_sql": "CREATE PROPERTY GRAPH dump_test.propgraph;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE PROPERTY GRAPH dump_test.propgraph"),
                ("rx", r";"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {"exclude_dump_test_schema": 1, "only_dump_measurement": 1},
    }

    tests["CREATE PUBLICATION pub1"] = {
        "create_order": 50,
        "create_sql": "CREATE PUBLICATION pub1;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE PUBLICATION pub1 WITH (publish = 'insert, update, delete, truncate');",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["CREATE PUBLICATION pub2"] = {
        "create_order": 50,
        "create_sql": "CREATE PUBLICATION pub2\n"
        "\t\t\t\t\t\t FOR ALL TABLES\n"
        "\t\t\t\t\t\t WITH (publish = '');",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE PUBLICATION pub2 FOR ALL TABLES WITH (publish = '');"),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["CREATE PUBLICATION pub3"] = {
        "create_order": 50,
        "create_sql": "CREATE PUBLICATION pub3;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE PUBLICATION pub3 WITH (publish = 'insert, update, delete, truncate');",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["CREATE PUBLICATION pub4"] = {
        "create_order": 50,
        "create_sql": "CREATE PUBLICATION pub4;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE PUBLICATION pub4 WITH (publish = 'insert, update, delete, truncate');",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["CREATE PUBLICATION pub5"] = {
        "create_order": 50,
        "create_sql": "CREATE PUBLICATION pub5 WITH (publish_generated_columns = stored);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE PUBLICATION pub5 WITH (publish = 'insert, update, delete, truncate', publish_generated_columns = stored);",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["CREATE PUBLICATION pub6"] = {
        "create_order": 50,
        "create_sql": "CREATE PUBLICATION pub6\n" "\t\t\t\t\t\t FOR ALL SEQUENCES;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE PUBLICATION pub6 FOR ALL SEQUENCES WITH (publish = 'insert, update, delete, truncate');",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["CREATE PUBLICATION pub7"] = {
        "create_order": 50,
        "create_sql": "CREATE PUBLICATION pub7\n"
        "\t\t\t\t\t\t FOR ALL SEQUENCES, ALL TABLES\n"
        "\t\t\t\t\t\t WITH (publish = '');",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE PUBLICATION pub7 FOR ALL TABLES, ALL SEQUENCES WITH (publish = '');",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["CREATE PUBLICATION pub8"] = {
        "create_order": 50,
        "create_sql": "CREATE PUBLICATION pub8 FOR ALL TABLES EXCEPT (TABLE dump_test.test_table);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE PUBLICATION pub8 FOR ALL TABLES EXCEPT (TABLE ONLY dump_test.test_table) WITH (publish = 'insert, update, delete, truncate');",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["CREATE PUBLICATION pub9"] = {
        "create_order": 50,
        "create_sql": "CREATE PUBLICATION pub9 FOR ALL TABLES EXCEPT (TABLE dump_test.test_table, dump_test.test_second_table);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE PUBLICATION pub9 FOR ALL TABLES EXCEPT (TABLE ONLY dump_test.test_table, TABLE ONLY dump_test.test_second_table) WITH (publish = 'insert, update, delete, truncate');",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["CREATE PUBLICATION pub10"] = {
        "create_order": 92,
        "create_sql": "CREATE PUBLICATION pub10 FOR ALL TABLES EXCEPT (TABLE dump_test.test_inheritance_parent);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE PUBLICATION pub10 FOR ALL TABLES EXCEPT (TABLE ONLY dump_test.test_inheritance_parent, TABLE ONLY dump_test.test_inheritance_child) WITH (publish = 'insert, update, delete, truncate');",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["CREATE SUBSCRIPTION sub1"] = {
        "create_order": 50,
        "create_sql": "CREATE SUBSCRIPTION sub1\n"
        "\t\t\t\t\t\t CONNECTION 'dbname=doesnotexist' PUBLICATION pub1\n"
        "\t\t\t\t\t\t WITH (connect = false);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE SUBSCRIPTION sub1 CONNECTION 'dbname=doesnotexist' PUBLICATION pub1 WITH (connect = false, slot_name = 'sub1', streaming = parallel);",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
        "unlike": {
            "no_subscriptions": 1,
            "no_subscriptions_restore": 1,
        },
    }
    tests["CREATE SUBSCRIPTION sub2"] = {
        "create_order": 50,
        "create_sql": "CREATE SUBSCRIPTION sub2\n"
        "\t\t\t\t\t\t CONNECTION 'dbname=doesnotexist' PUBLICATION pub1\n"
        "\t\t\t\t\t\t WITH (connect = false, origin = none, streaming = off);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE SUBSCRIPTION sub2 CONNECTION 'dbname=doesnotexist' PUBLICATION pub1 WITH (connect = false, slot_name = 'sub2', streaming = off, origin = none);",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
        "unlike": {
            "no_subscriptions": 1,
            "no_subscriptions_restore": 1,
        },
    }
    tests["CREATE SUBSCRIPTION sub3"] = {
        "create_order": 50,
        "create_sql": "CREATE SUBSCRIPTION sub3\n"
        "\t\t\t\t\t\t CONNECTION 'dbname=doesnotexist' PUBLICATION pub1\n"
        "\t\t\t\t\t\t WITH (connect = false, origin = any, streaming = on);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE SUBSCRIPTION sub3 CONNECTION 'dbname=doesnotexist' PUBLICATION pub1 WITH (connect = false, slot_name = 'sub3', streaming = on);",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
        "unlike": {
            "no_subscriptions": 1,
            "no_subscriptions_restore": 1,
        },
    }

    tests["ALTER PUBLICATION pub1 ADD TABLE test_table"] = {
        "create_order": 51,
        "create_sql": "ALTER PUBLICATION pub1 ADD TABLE dump_test.test_table;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER PUBLICATION pub1 ADD TABLE ONLY dump_test.test_table;"),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["ALTER PUBLICATION pub1 ADD TABLE test_second_table"] = {
        "create_order": 52,
        "create_sql": "ALTER PUBLICATION pub1 ADD TABLE dump_test.test_second_table;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER PUBLICATION pub1 ADD TABLE ONLY dump_test.test_second_table;",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["ALTER PUBLICATION pub1 ADD TABLE test_sixth_table (col3, col2)"] = {
        "create_order": 52,
        "create_sql": "ALTER PUBLICATION pub1 ADD TABLE dump_test.test_sixth_table (col3, col2);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER PUBLICATION pub1 ADD TABLE ONLY dump_test.test_sixth_table (col2, col3);",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests[
        "ALTER PUBLICATION pub1 ADD TABLE test_seventh_table (col3, col2) WHERE (col1 = 1)"
    ] = {
        "create_order": 52,
        "create_sql": "ALTER PUBLICATION pub1 ADD TABLE dump_test.test_seventh_table (col3, col2) WHERE (col1 = 1);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER PUBLICATION pub1 ADD TABLE ONLY dump_test.test_seventh_table (col2, col3) WHERE ((col1 = 1));",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["ALTER PUBLICATION pub3 ADD TABLES IN SCHEMA dump_test"] = {
        "create_order": 51,
        "create_sql": "ALTER PUBLICATION pub3 ADD TABLES IN SCHEMA dump_test;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER PUBLICATION pub3 ADD TABLES IN SCHEMA dump_test;"),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["ALTER PUBLICATION pub3 ADD TABLES IN SCHEMA public"] = {
        "create_order": 52,
        "create_sql": "ALTER PUBLICATION pub3 ADD TABLES IN SCHEMA public;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER PUBLICATION pub3 ADD TABLES IN SCHEMA public;"),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["ALTER PUBLICATION pub3 ADD TABLE test_table"] = {
        "create_order": 51,
        "create_sql": "ALTER PUBLICATION pub3 ADD TABLE dump_test.test_table;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER PUBLICATION pub3 ADD TABLE ONLY dump_test.test_table;"),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests["ALTER PUBLICATION pub4 ADD TABLE test_table WHERE (col1 > 0);"] = {
        "create_order": 51,
        "create_sql": "ALTER PUBLICATION pub4 ADD TABLE dump_test.test_table WHERE (col1 > 0);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER PUBLICATION pub4 ADD TABLE ONLY dump_test.test_table WHERE ((col1 > 0));",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }
    tests[
        "ALTER PUBLICATION pub4 ADD TABLE test_second_table WHERE (col2 = 'test');"
    ] = {
        "create_order": 52,
        "create_sql": "ALTER PUBLICATION pub4 ADD TABLE dump_test.test_second_table WHERE (col2 = 'test');",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER PUBLICATION pub4 ADD TABLE ONLY dump_test.test_second_table WHERE ((col2 = 'test'::text));",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_post_data": 1},
    }

    tests["CREATE SCHEMA public"] = {
        "regexp": re.compile(r"^CREATE SCHEMA public;", re.MULTILINE),
        "like": {},
    }
    tests["CREATE SCHEMA dump_test"] = {
        "create_order": 2,
        "create_sql": "CREATE SCHEMA dump_test;",
        "regexp": re.compile(r"^CREATE SCHEMA dump_test;", re.MULTILINE),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE SCHEMA dump_test_second_schema"] = {
        "create_order": 9,
        "create_sql": "CREATE SCHEMA dump_test_second_schema;",
        "regexp": re.compile(r"^CREATE SCHEMA dump_test_second_schema;", re.MULTILINE),
        "like": {**_full(), "role": 1, "section_pre_data": 1},
    }
    tests["CREATE TABLE test_table"] = {
        "create_order": 3,
        "create_sql": "CREATE TABLE dump_test.test_table (\n"
        "\t\t\t\t\t   col1 serial primary key,\n"
        "\t\t\t\t\t   col2 text COMPRESSION pglz,\n"
        "\t\t\t\t\t   col3 text,\n"
        "\t\t\t\t\t   col4 text,\n"
        "\t\t\t\t\t   CHECK (col1 <= 1000)\n"
        "\t\t\t\t   ) WITH (autovacuum_enabled = false, fillfactor=80);\n"
        "\t\t\t\t\t   COMMENT ON CONSTRAINT test_table_col1_check\n"
        "\t\t\t\t\t\t ON dump_test.test_table IS 'bounds check';",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_table ("),
                ("rx", r"\n\s+"),
                ("lit", "col1 integer NOT NULL,"),
                ("rx", r"\n\s+"),
                ("lit", "col2 text,"),
                ("rx", r"\n\s+"),
                ("lit", "col3 text,"),
                ("rx", r"\n\s+"),
                ("lit", "col4 text,"),
                ("rx", r"\n\s+"),
                ("lit", "CONSTRAINT test_table_col1_check CHECK ((col1 <= 1000))"),
                ("rx", r"\n"),
                ("lit", ")"),
                ("rx", r"\n"),
                ("lit", "WITH (autovacuum_enabled='false', fillfactor='80');"),
                ("rx", r"\n(.|\n)*"),
                (
                    "lit",
                    "COMMENT ON CONSTRAINT test_table_col1_check ON dump_test.test_table IS 'bounds check';",
                ),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_pre_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TABLE fk_reference_test_table"] = {
        "create_order": 21,
        "create_sql": "CREATE TABLE dump_test.fk_reference_test_table (\n"
        "\t\t\t\t\t   col1 int primary key references dump_test.test_table\n"
        "\t\t\t\t   );",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.fk_reference_test_table ("),
                ("rx", r"\n\s+"),
                ("lit", "col1 integer NOT NULL"),
                ("rx", r"\n\);"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TABLE test_second_table"] = {
        "create_order": 6,
        "create_sql": "CREATE TABLE dump_test.test_second_table (\n"
        "\t\t\t\t\t   col1 int,\n"
        "\t\t\t\t\t   col2 text\n"
        "\t\t\t\t   );",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_second_table ("),
                ("rx", r"\n\s+"),
                ("lit", "col1 integer,"),
                ("rx", r"\n\s+"),
                ("lit", "col2 text"),
                ("rx", r"\n\);"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TABLE measurement PARTITIONED BY"] = {
        "create_order": 90,
        "create_sql": "CREATE TABLE dump_test.measurement (\n"
        "\t\t\t\t\tcity_id serial not null,\n"
        "\t\t\t\t\tlogdate date not null,\n"
        "\t\t\t\t\tpeaktemp int CHECK (peaktemp >= -460),\n"
        "\t\t\t\t\tunitsales int\n"
        "\t\t\t\t   ) PARTITION BY RANGE (logdate);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "-- Name: measurement;"),
                ("rx", r".*\n"),
                ("lit", "--"),
                ("rx", r"\n\n"),
                ("lit", "CREATE TABLE dump_test.measurement ("),
                ("rx", r"\n\s+"),
                ("lit", "city_id integer NOT NULL,"),
                ("rx", r"\n\s+"),
                ("lit", "logdate date NOT NULL,"),
                ("rx", r"\n\s+"),
                ("lit", "peaktemp integer,"),
                ("rx", r"\n\s+"),
                ("lit", "unitsales integer,"),
                ("rx", r"\n\s+"),
                (
                    "lit",
                    "CONSTRAINT measurement_peaktemp_check CHECK ((peaktemp >= '-460'::integer))",
                ),
                ("rx", r"\n\)\n"),
                ("lit", "PARTITION BY RANGE (logdate);"),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "section_pre_data": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "exclude_measurement": 1,
        },
    }
    tests["Partition measurement_y2006m2 creation"] = {
        "create_order": 91,
        "create_sql": "CREATE TABLE dump_test_second_schema.measurement_y2006m2\n"
        "\t\t\t\t\t\tPARTITION OF dump_test.measurement (\n"
        "\t\t\t\t\t\t\tunitsales DEFAULT 0 CHECK (unitsales >= 0)\n"
        "\t\t\t\t\t\t)\n"
        "\t\t\t\t\t\tFOR VALUES FROM ('2006-02-01') TO ('2006-03-01');",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test_second_schema.measurement_y2006m2 ("),
                ("rx", r"\n\s+"),
                (
                    "lit",
                    "city_id integer DEFAULT nextval('dump_test.measurement_city_id_seq'::regclass) CONSTRAINT measurement_city_id_not_null NOT NULL,",
                ),
                ("rx", r"\n\s+"),
                (
                    "lit",
                    "logdate date CONSTRAINT measurement_logdate_not_null NOT NULL,",
                ),
                ("rx", r"\n\s+"),
                ("lit", "peaktemp integer,"),
                ("rx", r"\n\s+"),
                ("lit", "unitsales integer DEFAULT 0,"),
                ("rx", r"\n\s+"),
                (
                    "lit",
                    "CONSTRAINT measurement_peaktemp_check CHECK ((peaktemp >= '-460'::integer)),",
                ),
                ("rx", r"\n\s+"),
                (
                    "lit",
                    "CONSTRAINT measurement_y2006m2_unitsales_check CHECK ((unitsales >= 0))",
                ),
                ("rx", r"\n\);\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            "section_pre_data": 1,
            "role": 1,
            "binary_upgrade": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {"exclude_measurement": 1},
    }
    tests["Creation of row-level trigger in partitioned table"] = {
        "create_order": 92,
        "create_sql": "CREATE TRIGGER test_trigger\n"
        "\t\t   AFTER INSERT ON dump_test.measurement\n"
        "\t\t   FOR EACH ROW EXECUTE PROCEDURE dump_test.trigger_func()",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE TRIGGER test_trigger AFTER INSERT ON dump_test.measurement ",
                ),
                ("lit", "FOR EACH ROW "),
                ("lit", "EXECUTE FUNCTION dump_test.trigger_func();"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "section_post_data": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_measurement": 1,
        },
    }
    tests["COPY measurement"] = {
        "create_order": 93,
        "create_sql": "INSERT INTO dump_test.measurement (city_id, logdate, peaktemp, unitsales) "
        "VALUES (1, '2006-02-12', 35, 1);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "COPY dump_test_second_schema.measurement_y2006m2 (city_id, logdate, peaktemp, unitsales) FROM stdin;",
                ),
                ("rx", r"\n(?:1\t2006-02-12\t35\t1\n)\\\.\n"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "data_only": 1,
            "no_schema": 1,
            "only_dump_measurement": 1,
            "section_data": 1,
            "only_dump_test_schema": 1,
            "role_parallel": 1,
            "role": 1,
        },
        "unlike": {
            "binary_upgrade": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
            "exclude_measurement": 1,
            "only_dump_test_schema": 1,
            "test_schema_plus_large_objects": 1,
            "exclude_measurement_data": 1,
        },
    }
    tests["Disabled trigger on partition is altered"] = {
        "create_order": 93,
        "create_sql": "CREATE TABLE dump_test_second_schema.measurement_y2006m3\n"
        "\t\t\t\t\t\tPARTITION OF dump_test.measurement\n"
        "\t\t\t\t\t\tFOR VALUES FROM ('2006-03-01') TO ('2006-04-01');\n"
        "\t\t\t\t\t\tALTER TABLE dump_test_second_schema.measurement_y2006m3 DISABLE TRIGGER test_trigger;\n"
        "\t\t\t\t\t\tCREATE TABLE dump_test_second_schema.measurement_y2006m4\n"
        "\t\t\t\t\t\tPARTITION OF dump_test.measurement\n"
        "\t\t\t\t\t\tFOR VALUES FROM ('2006-04-01') TO ('2006-05-01');\n"
        "\t\t\t\t\t\tALTER TABLE dump_test_second_schema.measurement_y2006m4 ENABLE REPLICA TRIGGER test_trigger;\n"
        "\t\t\t\t\t\tCREATE TABLE dump_test_second_schema.measurement_y2006m5\n"
        "\t\t\t\t\t\tPARTITION OF dump_test.measurement\n"
        "\t\t\t\t\t\tFOR VALUES FROM ('2006-05-01') TO ('2006-06-01');\n"
        "\t\t\t\t\t\tALTER TABLE dump_test_second_schema.measurement_y2006m5 ENABLE ALWAYS TRIGGER test_trigger;\n"
        "\t\t\t\t\t\t",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER TABLE dump_test_second_schema.measurement_y2006m3 DISABLE TRIGGER test_trigger;",
                ),
            ],
            XM,
        ),
        "like": {
            **_full(),
            "section_post_data": 1,
            "role": 1,
            "binary_upgrade": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {"exclude_measurement": 1},
    }
    tests["Replica trigger on partition is altered"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER TABLE dump_test_second_schema.measurement_y2006m4 ENABLE REPLICA TRIGGER test_trigger;",
                ),
            ],
            XM,
        ),
        "like": {
            **_full(),
            "section_post_data": 1,
            "role": 1,
            "binary_upgrade": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {"exclude_measurement": 1},
    }
    tests["Always trigger on partition is altered"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER TABLE dump_test_second_schema.measurement_y2006m5 ENABLE ALWAYS TRIGGER test_trigger;",
                ),
            ],
            XM,
        ),
        "like": {
            **_full(),
            "section_post_data": 1,
            "role": 1,
            "binary_upgrade": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {"exclude_measurement": 1},
    }
    tests["Disabled trigger on partition is not created"] = {
        "regexp": re.compile(
            r"CREATE TRIGGER test_trigger.*ON dump_test_second_schema"
        ),
        "like": {},
    }
    tests["Triggers on partitions are not dropped"] = {
        "regexp": re.compile(r"DROP TRIGGER test_trigger.*ON dump_test_second_schema"),
        "like": {},
    }

    tests["CREATE TABLE test_third_table_generated_cols"] = {
        "create_order": 6,
        "create_sql": "CREATE TABLE dump_test.test_third_table (\n"
        "\t\t\t\t\t\tf1 int, junk int,\n"
        "\t\t\t\t\t\tg1 int generated always as (f1 * 2) stored,\n"
        '\t\t\t\t\t\t"F3" int,\n'
        '\t\t\t\t\t\tg2 int generated always as ("F3" * 3) stored\n'
        "\t\t\t\t\t);\n"
        "\t\t\t\t\tALTER TABLE dump_test.test_third_table DROP COLUMN junk;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_third_table ("),
                ("rx", r"\n\s+"),
                ("lit", "f1 integer,"),
                ("rx", r"\n\s+"),
                ("lit", "g1 integer GENERATED ALWAYS AS ((f1 * 2)) STORED,"),
                ("rx", r"\n\s+"),
                ("lit", '"F3" integer,'),
                ("rx", r"\n\s+"),
                ("lit", 'g2 integer GENERATED ALWAYS AS (("F3" * 3)) STORED'),
                ("rx", r"\n\);\n"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TABLE test_fourth_table_zero_col"] = {
        "create_order": 6,
        "create_sql": "CREATE TABLE dump_test.test_fourth_table (\n" "\t\t\t\t\t   );",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_fourth_table ("),
                ("rx", r"\n\);"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TABLE test_fifth_table"] = {
        "create_order": 53,
        "create_sql": "CREATE TABLE dump_test.test_fifth_table (\n"
        "\t\t\t\t\t\t\tcol1 integer,\n"
        "\t\t\t\t\t\t\tcol2 boolean,\n"
        "\t\t\t\t\t\t\tcol3 boolean,\n"
        "\t\t\t\t\t\t\tcol4 bit(5),\n"
        "\t\t\t\t\t\t\tcol5 float8\n"
        "\t\t\t\t\t   );",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_fifth_table ("),
                ("rx", r"\n\s+"),
                ("lit", "col1 integer,"),
                ("rx", r"\n\s+"),
                ("lit", "col2 boolean,"),
                ("rx", r"\n\s+"),
                ("lit", "col3 boolean,"),
                ("rx", r"\n\s+"),
                ("lit", "col4 bit(5),"),
                ("rx", r"\n\s+"),
                ("lit", "col5 double precision"),
                ("rx", r"\n\);"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TABLE test_sixth_table"] = {
        "create_order": 6,
        "create_sql": "CREATE TABLE dump_test.test_sixth_table (\n"
        "\t\t\t\t\t   col1 int,\n"
        "\t\t\t\t\t   col2 text,\n"
        "\t\t\t\t\t   col3 bytea\n"
        "\t\t\t\t   );",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_sixth_table ("),
                ("rx", r"\n\s+"),
                ("lit", "col1 integer,"),
                ("rx", r"\n\s+"),
                ("lit", "col2 text,"),
                ("rx", r"\n\s+"),
                ("lit", "col3 bytea"),
                ("rx", r"\n\);"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TABLE test_seventh_table"] = {
        "create_order": 6,
        "create_sql": "CREATE TABLE dump_test.test_seventh_table (\n"
        "\t\t\t\t\t   col1 int,\n"
        "\t\t\t\t\t   col2 text,\n"
        "\t\t\t\t\t   col3 bytea\n"
        "\t\t\t\t   );",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_seventh_table ("),
                ("rx", r"\n\s+"),
                ("lit", "col1 integer,"),
                ("rx", r"\n\s+"),
                ("lit", "col2 text,"),
                ("rx", r"\n\s+"),
                ("lit", "col3 bytea"),
                ("rx", r"\n\);"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TABLE test_table_identity"] = {
        "create_order": 3,
        "create_sql": "CREATE TABLE dump_test.test_table_identity (\n"
        "\t\t\t\t\t   col1 int generated always as identity primary key,\n"
        "\t\t\t\t\t   col2 text\n"
        "\t\t\t\t   );",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_table_identity ("),
                ("rx", r"\n\s+"),
                ("lit", "col1 integer NOT NULL,"),
                ("rx", r"\n\s+"),
                ("lit", "col2 text"),
                ("rx", r"\n\);.*"),
                (
                    "lit",
                    "ALTER TABLE dump_test.test_table_identity ALTER COLUMN col1 ADD GENERATED ALWAYS AS IDENTITY (",
                ),
                ("rx", r"\n\s+"),
                ("lit", "SEQUENCE NAME dump_test.test_table_identity_col1_seq"),
                ("rx", r"\n\s+"),
                ("lit", "START WITH 1"),
                ("rx", r"\n\s+"),
                ("lit", "INCREMENT BY 1"),
                ("rx", r"\n\s+"),
                ("lit", "NO MINVALUE"),
                ("rx", r"\n\s+"),
                ("lit", "NO MAXVALUE"),
                ("rx", r"\n\s+"),
                ("lit", "CACHE 1"),
                ("rx", r"\n\);"),
            ],
            XMS,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TABLE test_table_generated"] = {
        "create_order": 3,
        "create_sql": "CREATE TABLE dump_test.test_table_generated (\n"
        "\t\t\t\t\t   col1 int primary key,\n"
        "\t\t\t\t\t   col2 int generated always as (col1 * 2) stored,\n"
        "\t\t\t\t\t   col3 int generated always as (col1 * 3) virtual\n"
        "\t\t\t\t   );",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_table_generated ("),
                ("rx", r"\n\s+"),
                ("lit", "col1 integer NOT NULL,"),
                ("rx", r"\n\s+"),
                ("lit", "col2 integer GENERATED ALWAYS AS ((col1 * 2)) STORED,"),
                ("rx", r"\n\s+"),
                ("lit", "col3 integer GENERATED ALWAYS AS ((col1 * 3))"),
                ("rx", r"\n\);"),
            ],
            XMS,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TABLE test_table_generated_child1 (without local columns)"] = {
        "create_order": 4,
        "create_sql": "CREATE TABLE dump_test.test_table_generated_child1 ()\n"
        "\t\t\t\t\t\t INHERITS (dump_test.test_table_generated);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_table_generated_child1 ("),
                ("rx", r"\n\)\n"),
                ("lit", "INHERITS (dump_test.test_table_generated);"),
                ("rx", r"\n"),
            ],
            XMS,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER TABLE test_table_generated_child1"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER TABLE ONLY dump_test.test_table_generated_child1 ALTER COLUMN col2 ",
                ),
            ],
            re.MULTILINE,
        ),
        "like": {},
    }
    tests["CREATE TABLE test_table_generated_child2 (with local columns)"] = {
        "create_order": 4,
        "create_sql": "CREATE TABLE dump_test.test_table_generated_child2 (\n"
        "\t\t\t\t\t   col1 int,\n"
        "\t\t\t\t\t   col2 int\n"
        "\t\t\t\t\t ) INHERITS (dump_test.test_table_generated);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_table_generated_child2 ("),
                ("rx", r"\n\s+"),
                ("lit", "col1 integer,"),
                ("rx", r"\n\s+"),
                ("lit", "col2 integer"),
                ("rx", r"\n\)\n"),
                ("lit", "INHERITS (dump_test.test_table_generated);"),
                ("rx", r"\n"),
            ],
            XMS,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TABLE table_with_stats"] = {
        "create_order": 98,
        "create_sql": "CREATE TABLE dump_test.table_index_stats (\n"
        "\t\t\t\t\t   col1 int,\n"
        "\t\t\t\t\t   col2 int,\n"
        "\t\t\t\t\t   col3 int);\n"
        "\t\t\t\t\t CREATE INDEX index_with_stats\n"
        "\t\t\t\t\t  ON dump_test.table_index_stats\n"
        "\t\t\t\t\t  ((col1 + 1), col1, (col2 + 1), (col3 + 1));\n"
        "\t\t\t\t\t ALTER INDEX dump_test.index_with_stats\n"
        "\t\t\t\t\t   ALTER COLUMN 1 SET STATISTICS 400;\n"
        "\t\t\t\t\t ALTER INDEX dump_test.index_with_stats\n"
        "\t\t\t\t\t   ALTER COLUMN 3 SET STATISTICS 500;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER INDEX dump_test.index_with_stats ALTER COLUMN 1 SET STATISTICS 400;",
                ),
                ("rx", r"\n"),
                (
                    "lit",
                    "ALTER INDEX dump_test.index_with_stats ALTER COLUMN 3 SET STATISTICS 500;",
                ),
                ("rx", r"\n"),
            ],
            XMS,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TABLE test_inheritance_parent"] = {
        "create_order": 90,
        "create_sql": "CREATE TABLE dump_test.test_inheritance_parent (\n"
        "\t\t\t\t\t   col1 int NOT NULL,\n"
        "\t\t\t\t\t   col2 int CHECK (col2 >= 42)\n"
        "\t\t\t\t\t );",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_inheritance_parent ("),
                ("rx", r"\n\s+"),
                ("lit", "col1 integer NOT NULL,"),
                ("rx", r"\n\s+"),
                ("lit", "col2 integer,"),
                ("rx", r"\n\s+"),
                (
                    "lit",
                    "CONSTRAINT test_inheritance_parent_col2_check CHECK ((col2 >= 42))",
                ),
                ("rx", r"\n"),
                ("lit", ");"),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE TABLE test_inheritance_child"] = {
        "create_order": 91,
        "create_sql": "CREATE TABLE dump_test.test_inheritance_child (\n"
        "\t\t\t\t\t    col1 int NOT NULL,\n"
        "\t\t\t\t\t    CONSTRAINT test_inheritance_child CHECK (col2 >= 142857)\n"
        "\t\t\t\t\t) INHERITS (dump_test.test_inheritance_parent);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE TABLE dump_test.test_inheritance_child ("),
                ("rx", r"\n\s+"),
                ("lit", "col1 integer NOT NULL,"),
                ("rx", r"\n\s+"),
                ("lit", "CONSTRAINT test_inheritance_child CHECK ((col2 >= 142857))"),
                ("rx", r"\n\)\n"),
                ("lit", "INHERITS (dump_test.test_inheritance_parent);"),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }

    tests["CREATE STATISTICS extended_stats_no_options"] = {
        "create_order": 97,
        "create_sql": "CREATE STATISTICS dump_test.test_ext_stats_no_options\n"
        "\t\t\t\t\t\tON col1, col2 FROM dump_test.test_table",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE STATISTICS dump_test.test_ext_stats_no_options ON col1, col2 FROM dump_test.test_table;",
                ),
            ],
            XMS,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE STATISTICS extended_stats_options"] = {
        "create_order": 97,
        "create_sql": "CREATE STATISTICS dump_test.test_ext_stats_opts\n"
        "\t\t\t\t\t\t(ndistinct) ON col1, col2 FROM dump_test.test_fifth_table",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE STATISTICS dump_test.test_ext_stats_opts (ndistinct) ON col1, col2 FROM dump_test.test_fifth_table;",
                ),
            ],
            XMS,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER STATISTICS extended_stats_options"] = {
        "create_order": 98,
        "create_sql": "ALTER STATISTICS dump_test.test_ext_stats_opts SET STATISTICS 1000",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER STATISTICS dump_test.test_ext_stats_opts SET STATISTICS 1000;",
                ),
            ],
            XMS,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE STATISTICS extended_stats_expression"] = {
        "create_order": 99,
        "create_sql": "CREATE STATISTICS dump_test.test_ext_stats_expr\n"
        "\t\t\t\t\t\tON (2 * col1) FROM dump_test.test_fifth_table",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE STATISTICS dump_test.test_ext_stats_expr ON (2 * col1) FROM dump_test.test_fifth_table;",
                ),
            ],
            XMS,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE SEQUENCE test_table_col1_seq"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "CREATE SEQUENCE dump_test.test_table_col1_seq"),
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
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_pre_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE INDEX ON ONLY measurement"] = {
        "create_order": 92,
        "create_sql": "CREATE INDEX ON dump_test.measurement (city_id, logdate);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE INDEX measurement_city_id_logdate_idx ON ONLY dump_test.measurement USING",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_measurement": 1,
        },
    }
    tests["ALTER TABLE measurement PRIMARY KEY"] = {
        "catch_all": "CREATE ... commands",
        "create_order": 93,
        "create_sql": "ALTER TABLE dump_test.measurement ADD PRIMARY KEY (city_id, logdate);",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "ALTER TABLE ONLY dump_test.measurement"),
                ("rx", r" \n^\s+"),
                (
                    "lit",
                    "ADD CONSTRAINT measurement_pkey PRIMARY KEY (city_id, logdate);",
                ),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "section_post_data": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_measurement": 1,
        },
    }
    tests["CREATE INDEX ... ON measurement_y2006_m2"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE INDEX measurement_y2006m2_city_id_logdate_idx ON dump_test_second_schema.measurement_y2006m2 ",
                ),
            ],
            XM,
        ),
        "like": {
            **_full(),
            "role": 1,
            "section_post_data": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {"exclude_measurement": 1},
    }
    tests["ALTER INDEX ... ATTACH PARTITION"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER INDEX dump_test.measurement_city_id_logdate_idx ATTACH PARTITION dump_test_second_schema.measurement_y2006m2_city_id_logdate_idx",
                ),
            ],
            XM,
        ),
        "like": {
            **_full(),
            "role": 1,
            "section_post_data": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {"exclude_measurement": 1},
    }
    tests["ALTER INDEX ... ATTACH PARTITION (primary key)"] = {
        "catch_all": "CREATE ... commands",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER INDEX dump_test.measurement_pkey ATTACH PARTITION dump_test_second_schema.measurement_y2006m2_pkey",
                ),
            ],
            XM,
        ),
        "like": {
            **_full(),
            "role": 1,
            "section_post_data": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {"exclude_measurement": 1},
    }
    tests["CREATE VIEW test_view"] = {
        "create_order": 61,
        "create_sql": "CREATE VIEW dump_test.test_view\n"
        "\t\t                   WITH (check_option = 'local', security_barrier = true) AS\n"
        "\t\t                   SELECT col1 FROM dump_test.test_table;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE VIEW dump_test.test_view WITH (security_barrier='true') AS",
                ),
                ("rx", r"\n\s+"),
                ("lit", "SELECT col1"),
                ("rx", r"\n\s+"),
                ("lit", "FROM dump_test.test_table"),
                ("rx", r"\n\s+"),
                ("lit", "WITH LOCAL CHECK OPTION;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["ALTER VIEW test_view SET DEFAULT"] = {
        "create_order": 62,
        "create_sql": "ALTER VIEW dump_test.test_view ALTER COLUMN col1 SET DEFAULT 1;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "ALTER TABLE ONLY dump_test.test_view ALTER COLUMN col1 SET DEFAULT 1;",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "only_dump_measurement": 1,
        },
    }

    tests["DROP SCHEMA public (for testing without public schema)"] = {
        "database": "regress_pg_dump_test",
        "create_order": 100,
        "create_sql": "DROP SCHEMA public;",
        "regexp": re.compile(r"^DROP SCHEMA public;", re.MULTILINE),
        "like": {},
    }
    tests["DROP SCHEMA public"] = {
        "regexp": re.compile(r"^DROP SCHEMA public;", re.MULTILINE),
        "like": {},
    }
    tests["DROP SCHEMA IF EXISTS public"] = {
        "regexp": re.compile(r"^DROP SCHEMA IF EXISTS public;", re.MULTILINE),
        "like": {},
    }
    tests["DROP EXTENSION plpgsql"] = {
        "regexp": re.compile(r"^DROP EXTENSION plpgsql;", re.MULTILINE),
        "like": {},
    }
    tests["DROP FUNCTION dump_test.pltestlang_call_handler()"] = {
        "regexp": re.compile(
            r"^DROP FUNCTION dump_test\.pltestlang_call_handler\(\);", re.MULTILINE
        ),
        "like": {"clean": 1},
    }
    tests["DROP LANGUAGE pltestlang"] = {
        "regexp": re.compile(r"^DROP PROCEDURAL LANGUAGE pltestlang;", re.MULTILINE),
        "like": {"clean": 1},
    }
    tests["DROP SCHEMA dump_test"] = {
        "regexp": re.compile(r"^DROP SCHEMA dump_test;", re.MULTILINE),
        "like": {"clean": 1},
    }
    tests["DROP SCHEMA dump_test_second_schema"] = {
        "regexp": re.compile(r"^DROP SCHEMA dump_test_second_schema;", re.MULTILINE),
        "like": {"clean": 1},
    }
    tests["DROP TABLE test_table"] = {
        "regexp": re.compile(r"^DROP TABLE dump_test\.test_table;", re.MULTILINE),
        "like": {"clean": 1},
    }
    tests["DROP TABLE fk_reference_test_table"] = {
        "regexp": re.compile(
            r"^DROP TABLE dump_test\.fk_reference_test_table;", re.MULTILINE
        ),
        "like": {"clean": 1},
    }
    tests["DROP TABLE test_second_table"] = {
        "regexp": re.compile(
            r"^DROP TABLE dump_test\.test_second_table;", re.MULTILINE
        ),
        "like": {"clean": 1},
    }
    tests["DROP EXTENSION IF EXISTS plpgsql"] = {
        "regexp": re.compile(r"^DROP EXTENSION IF EXISTS plpgsql;", re.MULTILINE),
        "like": {},
    }
    tests["DROP FUNCTION IF EXISTS dump_test.pltestlang_call_handler()"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "DROP FUNCTION IF EXISTS dump_test.pltestlang_call_handler();"),
            ],
            XM,
        ),
        "like": {"clean_if_exists": 1},
    }
    tests["DROP LANGUAGE IF EXISTS pltestlang"] = {
        "regexp": re.compile(
            r"^DROP PROCEDURAL LANGUAGE IF EXISTS pltestlang;", re.MULTILINE
        ),
        "like": {"clean_if_exists": 1},
    }
    tests["DROP SCHEMA IF EXISTS dump_test"] = {
        "regexp": re.compile(r"^DROP SCHEMA IF EXISTS dump_test;", re.MULTILINE),
        "like": {"clean_if_exists": 1},
    }
    tests["DROP SCHEMA IF EXISTS dump_test_second_schema"] = {
        "regexp": re.compile(
            r"^DROP SCHEMA IF EXISTS dump_test_second_schema;", re.MULTILINE
        ),
        "like": {"clean_if_exists": 1},
    }
    tests["DROP TABLE IF EXISTS test_table"] = {
        "regexp": re.compile(
            r"^DROP TABLE IF EXISTS dump_test\.test_table;", re.MULTILINE
        ),
        "like": {"clean_if_exists": 1},
    }
    tests["DROP TABLE IF EXISTS test_second_table"] = {
        "regexp": re.compile(
            r"^DROP TABLE IF EXISTS dump_test\.test_second_table;", re.MULTILINE
        ),
        "like": {"clean_if_exists": 1},
    }
    tests["DROP ROLE regress_dump_test_role"] = {
        "regexp": _qr([("rx", r"^"), ("lit", "DROP ROLE regress_dump_test_role;")], XM),
        "like": {"pg_dumpall_globals_clean": 1},
    }
    tests["DROP ROLE pg_"] = {
        "regexp": _qr([("rx", r"^"), ("lit", "DROP ROLE pg_"), ("rx", r".+;")], XM),
        "like": {},
    }
    tests["GRANT USAGE ON SCHEMA dump_test_second_schema"] = {
        "create_order": 10,
        "create_sql": "GRANT USAGE ON SCHEMA dump_test_second_schema\n"
        "\t\t\t\t\t\t   TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "GRANT USAGE ON SCHEMA dump_test_second_schema TO regress_dump_test_role;",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "role": 1, "section_pre_data": 1},
        "unlike": {"no_privs": 1},
    }
    tests["GRANT USAGE ON FOREIGN DATA WRAPPER dummy"] = {
        "create_order": 85,
        "create_sql": "GRANT USAGE ON FOREIGN DATA WRAPPER dummy\n"
        "\t\t\t\t\t\t   TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "GRANT ALL ON FOREIGN DATA WRAPPER dummy TO regress_dump_test_role;",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_pre_data": 1},
        "unlike": {"no_privs": 1},
    }
    tests["GRANT USAGE ON FOREIGN SERVER s1"] = {
        "create_order": 85,
        "create_sql": "GRANT USAGE ON FOREIGN SERVER s1\n"
        "\t\t\t\t\t\t   TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "GRANT ALL ON FOREIGN SERVER s1 TO regress_dump_test_role;"),
            ],
            XM,
        ),
        "like": {**_full(), "section_pre_data": 1},
        "unlike": {"no_privs": 1},
    }
    tests["GRANT USAGE ON DOMAIN dump_test.us_postal_code"] = {
        "create_order": 72,
        "create_sql": "GRANT USAGE ON DOMAIN dump_test.us_postal_code TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "GRANT ALL ON TYPE dump_test.us_postal_code TO regress_dump_test_role;",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_privs": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["GRANT USAGE ON TYPE dump_test.int42"] = {
        "create_order": 87,
        "create_sql": "GRANT USAGE ON TYPE dump_test.int42 TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "GRANT ALL ON TYPE dump_test.int42 TO regress_dump_test_role;"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_privs": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["GRANT USAGE ON TYPE dump_test.planets - ENUM"] = {
        "create_order": 66,
        "create_sql": "GRANT USAGE ON TYPE dump_test.planets TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "GRANT ALL ON TYPE dump_test.planets TO regress_dump_test_role;",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_privs": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["GRANT USAGE ON TYPE dump_test.textrange - RANGE"] = {
        "create_order": 67,
        "create_sql": "GRANT USAGE ON TYPE dump_test.textrange TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "GRANT ALL ON TYPE dump_test.textrange TO regress_dump_test_role;",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_privs": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["GRANT CREATE ON DATABASE dump_test"] = {
        "create_order": 48,
        "create_sql": "GRANT CREATE ON DATABASE dump_test TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "GRANT CREATE ON DATABASE dump_test TO regress_dump_test_role;",
                ),
            ],
            XM,
        ),
        "like": {"pg_dumpall_dbprivs": 1},
    }
    tests["GRANT SELECT ON TABLE test_table"] = {
        "create_order": 5,
        "create_sql": "GRANT SELECT ON TABLE dump_test.test_table\n"
        "\t\t\t\t\t\t   TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "GRANT SELECT ON TABLE dump_test.test_table TO regress_dump_test_role;",
                ),
            ],
            re.MULTILINE,
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "section_pre_data": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "exclude_test_table": 1,
            "no_privs": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["GRANT SELECT ON TABLE measurement"] = {
        "create_order": 91,
        "create_sql": "GRANT SELECT ON TABLE dump_test.measurement\n"
        "\t\t\t\t\t\t   TO regress_dump_test_role;\n"
        "\t\t\t\t\t   GRANT SELECT(city_id) ON TABLE dump_test.measurement\n"
        '\t\t\t\t\t\t   TO "regress_quoted  \\"" role";',
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "GRANT SELECT ON TABLE dump_test.measurement TO regress_dump_test_role;",
                ),
                ("rx", r"\n.*^"),
                (
                    "lit",
                    'GRANT SELECT(city_id) ON TABLE dump_test.measurement TO "regress_quoted  \\"" role";',
                ),
            ],
            XMS,
        ),
        "like": {
            **_full(),
            **_dts(),
            "section_pre_data": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_privs": 1,
            "exclude_measurement": 1,
        },
    }
    tests["GRANT SELECT ON TABLE measurement_y2006m2"] = {
        "create_order": 94,
        "create_sql": "GRANT SELECT ON TABLE\n"
        "\t\t\t\t\t\t   dump_test_second_schema.measurement_y2006m2,\n"
        "\t\t\t\t\t\t   dump_test_second_schema.measurement_y2006m3,\n"
        "\t\t\t\t\t\t   dump_test_second_schema.measurement_y2006m4,\n"
        "\t\t\t\t\t\t   dump_test_second_schema.measurement_y2006m5\n"
        "\t\t\t\t\t\t   TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "GRANT SELECT ON TABLE dump_test_second_schema.measurement_y2006m2 TO regress_dump_test_role;",
                ),
            ],
            re.MULTILINE,
        ),
        "like": {
            **_full(),
            "role": 1,
            "section_pre_data": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {
            "no_privs": 1,
            "exclude_measurement": 1,
        },
    }

    tests["GRANT ALL ON LARGE OBJECT ..."] = {
        "create_order": 60,
        "create_sql": "DO $$\n"
        "\t\t\t\t\t\t DECLARE myoid oid;\n"
        "\t\t\t\t\t\t BEGIN\n"
        "\t\t\t\t\t\t\tSELECT loid FROM pg_largeobject INTO myoid;\n"
        "\t\t\t\t\t\t\tEXECUTE 'GRANT ALL ON LARGE OBJECT ' || myoid || ' TO regress_dump_test_role;';\n"
        "\t\t\t\t\t\t END;\n"
        "\t\t\t\t\t\t $$;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "GRANT ALL ON LARGE OBJECT "),
                ("rx", r"[0-9]+"),
                ("lit", " TO regress_dump_test_role;"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            "column_inserts": 1,
            "data_only": 1,
            "inserts": 1,
            "no_schema": 1,
            "section_data": 1,
            "test_schema_plus_large_objects": 1,
        },
        "unlike": {
            "binary_upgrade": 1,
            "no_large_objects": 1,
            "no_privs": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
        },
    }
    tests["GRANT INSERT(col1) ON TABLE test_second_table"] = {
        "create_order": 8,
        "create_sql": "GRANT INSERT (col1) ON TABLE dump_test.test_second_table\n"
        "\t\t\t\t\t\t   TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "GRANT INSERT(col1) ON TABLE dump_test.test_second_table TO regress_dump_test_role;",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_privs": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["GRANT SELECT ON PROPERTY GRAPH propgraph"] = {
        "create_order": 21,
        "create_sql": "GRANT SELECT ON PROPERTY GRAPH dump_test.propgraph TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "GRANT ALL ON PROPERTY GRAPH dump_test.propgraph TO regress_dump_test_role;",
                ),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_privs": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["GRANT EXECUTE ON FUNCTION pg_sleep() TO regress_dump_test_role"] = {
        "create_order": 16,
        "create_sql": "GRANT EXECUTE ON FUNCTION pg_sleep(float8)\n"
        "\t\t\t\t\t\t   TO regress_dump_test_role;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "GRANT ALL ON FUNCTION pg_catalog.pg_sleep(double precision) TO regress_dump_test_role;",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_pre_data": 1},
        "unlike": {"no_privs": 1},
    }
    _proc_cols = [
        "tableoid",
        "oid",
        "proname",
        "pronamespace",
        "proowner",
        "prolang",
        "procost",
        "prorows",
        "provariadic",
        "prosupport",
        "prokind",
        "prosecdef",
        "proleakproof",
        "proisstrict",
        "proretset",
        "provolatile",
        "proparallel",
        "pronargs",
        "pronargdefaults",
        "prorettype",
        "proargtypes",
        "proallargtypes",
        "proargmodes",
        "proargnames",
        "proargdefaults",
        "protrftypes",
        "prosrc",
        "probin",
        "proconfig",
        "proacl",
    ]
    _proc_create_cols = ",\n\t\t\t\t\t\t   ".join(_proc_cols)
    _proc_parts: List[_Segment] = []
    for _i, _col in enumerate(_proc_cols):
        if _i > 0:
            _proc_parts.append(("rx", r"\n.*"))
        _proc_parts.append(
            ("lit", f"GRANT SELECT({_col}) ON TABLE pg_catalog.pg_proc TO PUBLIC;")
        )
    tests["GRANT SELECT (proname ...) ON TABLE pg_proc TO public"] = {
        "create_order": 46,
        "create_sql": "GRANT SELECT (\n"
        "\t\t\t\t\t\t   " + _proc_create_cols + "\n"
        "\t\t\t\t\t\t) ON TABLE pg_proc TO public;",
        "regexp": _qr(_proc_parts, XMS),
        "like": {**_full(), "section_pre_data": 1},
        "unlike": {"no_privs": 1},
    }
    tests["GRANT USAGE ON SCHEMA public TO public"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "--"),
                ("rx", r"\n\n"),
                ("lit", "GRANT USAGE ON SCHEMA public TO PUBLIC;"),
            ],
            XM,
        ),
        "like": {},
    }
    tests["REFRESH MATERIALIZED VIEW matview"] = {
        "regexp": _qr(
            [("rx", r"^"), ("lit", "REFRESH MATERIALIZED VIEW dump_test.matview;")],
            re.MULTILINE,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["REFRESH MATERIALIZED VIEW matview_second"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "REFRESH MATERIALIZED VIEW dump_test.matview;"),
                ("rx", r"\n.*"),
                ("lit", "REFRESH MATERIALIZED VIEW dump_test.matview_second;"),
            ],
            XMS,
        ),
        "like": {**_full(), **_dts(), "section_post_data": 1},
        "unlike": {
            "binary_upgrade": 1,
            "exclude_dump_test_schema": 1,
            "schema_only": 1,
            "schema_only_with_statistics": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["REFRESH MATERIALIZED VIEW matview_third"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "REFRESH MATERIALIZED VIEW dump_test.matview_third;"),
            ],
            XMS,
        ),
        "like": {},
    }
    tests["REFRESH MATERIALIZED VIEW matview_fourth"] = {
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "REFRESH MATERIALIZED VIEW dump_test.matview_fourth;"),
            ],
            XMS,
        ),
        "like": {},
    }
    tests["REVOKE CONNECT ON DATABASE dump_test FROM public"] = {
        "create_order": 49,
        "create_sql": "REVOKE CONNECT ON DATABASE dump_test FROM public;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "REVOKE CONNECT,TEMPORARY ON DATABASE dump_test FROM PUBLIC;"),
                ("rx", r"\n"),
                ("lit", "GRANT TEMPORARY ON DATABASE dump_test TO PUBLIC;"),
                ("rx", r"\n"),
                (
                    "lit",
                    "GRANT CREATE ON DATABASE dump_test TO regress_dump_test_role;",
                ),
            ],
            XM,
        ),
        "like": {"pg_dumpall_dbprivs": 1},
    }
    tests["REVOKE EXECUTE ON FUNCTION pg_sleep() FROM public"] = {
        "create_order": 15,
        "create_sql": "REVOKE EXECUTE ON FUNCTION pg_sleep(float8)\n"
        "\t\t\t\t\t\t   FROM public;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "REVOKE ALL ON FUNCTION pg_catalog.pg_sleep(double precision) FROM PUBLIC;",
                ),
            ],
            XM,
        ),
        "like": {**_full(), "section_pre_data": 1},
        "unlike": {"no_privs": 1},
    }
    tests["REVOKE EXECUTE ON FUNCTION pg_stat_reset FROM regress_dump_test_role"] = {
        "create_order": 15,
        "create_sql": "\n"
        "\t\t\tALTER FUNCTION pg_stat_reset OWNER TO regress_dump_test_role;\n"
        "\t\t\tREVOKE EXECUTE ON FUNCTION pg_stat_reset\n"
        "\t\t\t  FROM regress_dump_test_role;",
        "regexp": re.compile(
            r"^[^-].*pg_stat_reset.* regress_dump_test_role", re.MULTILINE
        ),
        "like": {},
    }
    tests["REVOKE SELECT ON TABLE pg_proc FROM public"] = {
        "create_order": 45,
        "create_sql": "REVOKE SELECT ON TABLE pg_proc FROM public;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "REVOKE SELECT ON TABLE pg_catalog.pg_proc FROM PUBLIC;"),
            ],
            re.MULTILINE,
        ),
        "like": {**_full(), "section_pre_data": 1},
        "unlike": {"no_privs": 1},
    }
    tests["REVOKE ALL ON SCHEMA public"] = {
        "create_order": 16,
        "create_sql": 'REVOKE ALL ON SCHEMA public FROM "regress_quoted  \\"" role";',
        "regexp": re.compile(
            r'^REVOKE ALL ON SCHEMA public FROM "regress_quoted  \\"" role";',
            re.MULTILINE,
        ),
        "like": {**_full(), "section_pre_data": 1},
        "unlike": {"no_privs": 1},
    }
    tests["REVOKE USAGE ON LANGUAGE plpgsql FROM public"] = {
        "create_order": 16,
        "create_sql": "REVOKE USAGE ON LANGUAGE plpgsql FROM public;",
        "regexp": re.compile(
            r"^REVOKE ALL ON LANGUAGE plpgsql FROM PUBLIC;", re.MULTILINE
        ),
        "like": {
            **_full(),
            **_dts(),
            "only_dump_test_table": 1,
            "role": 1,
            "section_pre_data": 1,
            "only_dump_measurement": 1,
        },
        "unlike": {"no_privs": 1},
    }

    tests["CREATE ACCESS METHOD regress_test_table_am"] = {
        "create_order": 11,
        "create_sql": "CREATE ACCESS METHOD regress_table_am TYPE TABLE HANDLER heap_tableam_handler;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                (
                    "lit",
                    "CREATE ACCESS METHOD regress_table_am TYPE TABLE HANDLER heap_tableam_handler;",
                ),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {**_full(), "section_pre_data": 1},
    }
    tests["CREATE TABLE regress_pg_dump_table_am"] = {
        "create_order": 12,
        "create_sql": "\n"
        "\t\t\tCREATE TABLE dump_test.regress_pg_dump_table_am_0() USING heap;\n"
        "\t\t\tCREATE TABLE dump_test.regress_pg_dump_table_am_1 (col1 int) USING regress_table_am;\n"
        "\t\t\tCREATE TABLE dump_test.regress_pg_dump_table_am_2() USING heap;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "SET default_table_access_method = regress_table_am;"),
                ("rx", r"(\n(?!SET[^;]+;)[^\n]*)*"),
                ("rx", r"\n"),
                ("lit", "CREATE TABLE dump_test.regress_pg_dump_table_am_1 ("),
                ("rx", r"\n\s+"),
                ("lit", "col1 integer"),
                ("rx", r"\n\);"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_table_access_method": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["CREATE MATERIALIZED VIEW regress_pg_dump_matview_am"] = {
        "create_order": 13,
        "create_sql": "\n"
        "\t\t\tCREATE MATERIALIZED VIEW dump_test.regress_pg_dump_matview_am_0 USING heap AS SELECT 1;\n"
        "\t\t\tCREATE MATERIALIZED VIEW dump_test.regress_pg_dump_matview_am_1\n"
        "\t\t\t\tUSING regress_table_am AS SELECT count(*) FROM pg_class;\n"
        "\t\t\tCREATE MATERIALIZED VIEW dump_test.regress_pg_dump_matview_am_2 USING heap AS SELECT 1;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "SET default_table_access_method = regress_table_am;"),
                ("rx", r"(\n(?!SET[^;]+;)[^\n]*)*"),
                (
                    "lit",
                    "CREATE MATERIALIZED VIEW dump_test.regress_pg_dump_matview_am_1 AS",
                ),
                ("rx", r"\n\s+"),
                ("lit", "SELECT count(*) AS count"),
                ("rx", r"\n\s+"),
                ("lit", "FROM pg_class"),
                ("rx", r"\n\s+"),
                ("lit", "WITH NO DATA;"),
                ("rx", r"\n"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_table_access_method": 1,
            "only_dump_measurement": 1,
        },
    }
    tests["statistics_import"] = {
        "create_sql": "\n"
        "\t\t\tCREATE TABLE dump_test.has_stats\n"
        "\t\t\tAS SELECT g.g AS x, g.g / 2 AS y FROM generate_series(1,100) AS g(g);\n"
        "\t\t\tCREATE MATERIALIZED VIEW dump_test.has_stats_mv AS SELECT * FROM dump_test.has_stats;\n"
        '\t\t\tCREATE INDEX """dump_test""\'s post-data index" ON dump_test.has_stats(x, (x - 1));\n'
        "\t\t\tANALYZE dump_test.has_stats, dump_test.has_stats_mv;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "SELECT * FROM pg_catalog.pg_restore_relation_stats("),
                ("rx", r"\s+"),
                ("rx", r"'version',\s'\d+'::integer,\s+"),
                ("rx", r"'schemaname',\s'dump_test',\s+"),
                ("rx", r"'relname',\s'\"dump_test\"''s\ post-data\ index',\s+"),
                ("rx", r"'relpages',\s'\d+'::integer,\s+"),
                ("rx", r"'reltuples',\s'\d+'::real,\s+"),
                ("rx", r"'relallvisible',\s'\d+'::integer,\s+"),
                ("rx", r"'relallfrozen',\s'\d+'::integer\s+"),
                ("rx", r"\);\s+"),
                ("lit", "SELECT * FROM pg_catalog.pg_restore_attribute_stats("),
                ("rx", r"\s+"),
                ("rx", r"'version',\s'\d+'::integer,\s+"),
                ("rx", r"'schemaname',\s'dump_test',\s+"),
                ("rx", r"'relname',\s'\"dump_test\"''s\ post-data\ index',\s+"),
                ("rx", r"'attnum',\s'2'::smallint,\s+"),
                ("rx", r"'inherited',\s'f'::boolean,\s+"),
                ("rx", r"'null_frac',\s'0'::real,\s+"),
                ("rx", r"'avg_width',\s'4'::integer,\s+"),
                ("rx", r"'n_distinct',\s'-1'::real,\s+"),
                ("rx", r"'histogram_bounds',\s'\{[0-9,]+\}'::text,\s+"),
                ("rx", r"'correlation',\s'1'::real\s+"),
                ("rx", r"\);"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "no_data_no_schema": 1,
            "no_schema": 1,
            "section_post_data": 1,
            "statistics_only": 1,
            "schema_only_with_statistics": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_statistics": 1,
            "only_dump_measurement": 1,
            "schema_only": 1,
        },
    }
    tests["extended_statistics_import"] = {
        "create_sql": "\n"
        "\t\t\tCREATE TABLE dump_test.has_ext_stats\n"
        "\t\t\tAS SELECT g.g AS x, g.g / 2 AS y FROM generate_series(1,100) AS g(g);\n"
        "\t\t\tCREATE STATISTICS dump_test.es1 ON x, (y % 2) FROM dump_test.has_ext_stats;\n"
        "\t\t\tANALYZE dump_test.has_ext_stats;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("lit", "SELECT * FROM pg_catalog.pg_restore_extended_stats("),
                ("rx", r"\s+"),
            ],
            XM,
        ),
        "like": {
            **_full(),
            **_dts(),
            "no_data_no_schema": 1,
            "no_schema": 1,
            "section_post_data": 1,
            "statistics_only": 1,
            "schema_only_with_statistics": 1,
        },
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_statistics": 1,
            "only_dump_measurement": 1,
            "schema_only": 1,
        },
    }
    tests["relstats_on_unanalyzed_tables"] = {
        "regexp": re.compile(r"pg_catalog.pg_restore_relation_stats"),
        "like": {
            **_full(),
            **_dts(),
            "no_data_no_schema": 1,
            "no_schema": 1,
            "only_dump_test_table": 1,
            "role": 1,
            "role_parallel": 1,
            "section_data": 1,
            "section_post_data": 1,
            "statistics_only": 1,
            "schema_only_with_statistics": 1,
        },
        "unlike": {
            "no_statistics": 1,
            "schema_only": 1,
        },
    }
    tests["CREATE TABLE regress_pg_dump_table_part"] = {
        "create_order": 19,
        "create_sql": "\n"
        "\t\t\tCREATE TABLE dump_test.regress_pg_dump_table_am_parent (id int) PARTITION BY LIST (id);\n"
        "\t\t\tALTER TABLE dump_test.regress_pg_dump_table_am_parent SET ACCESS METHOD regress_table_am;\n"
        "\t\t\tCREATE TABLE dump_test.regress_pg_dump_table_am_child_1\n"
        "\t\t\t  PARTITION OF dump_test.regress_pg_dump_table_am_parent FOR VALUES IN (1);\n"
        "\t\t\tCREATE TABLE dump_test.regress_pg_dump_table_am_child_2\n"
        "\t\t\t  PARTITION OF dump_test.regress_pg_dump_table_am_parent FOR VALUES IN (2) USING heap;",
        "regexp": _qr(
            [
                ("rx", r"^"),
                ("rx", r"\n"),
                ("lit", "CREATE TABLE dump_test.regress_pg_dump_table_am_parent ("),
                ("rx", r"(\n(?!SET[^;]+;)[^\n]*)*"),
                (
                    "lit",
                    "ALTER TABLE dump_test.regress_pg_dump_table_am_parent SET ACCESS METHOD regress_table_am;",
                ),
                ("rx", r"(.*\n)*"),
                ("lit", "SET default_table_access_method = regress_table_am;"),
                ("rx", r"(\n(?!SET[^;]+;)[^\n]*)*"),
                ("rx", r"\n"),
                ("lit", "CREATE TABLE dump_test.regress_pg_dump_table_am_child_1 ("),
                ("rx", r"(.*\n)*"),
                ("lit", "SET default_table_access_method = heap;"),
                ("rx", r"(\n(?!SET[^;]+;)[^\n]*)*"),
                ("rx", r"\n"),
                ("lit", "CREATE TABLE dump_test.regress_pg_dump_table_am_child_2 ("),
                ("rx", r"(.*\n)*"),
            ],
            XM,
        ),
        "like": {**_full(), **_dts(), "section_pre_data": 1},
        "unlike": {
            "exclude_dump_test_schema": 1,
            "no_table_access_method": 1,
            "only_dump_measurement": 1,
        },
    }

    return tests


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


def _build_create_sql(
    tests: Dict[str, dict], collation_support: bool, supports_icu: bool
) -> Dict[str, str]:
    """Collect each test's create_sql per database in create_order.

    Mirrors the Perl seeding loop: tests are walked in create_order, an 'icu'
    test implies 'collation', collation/icu tests are skipped when unsupported,
    and each create_sql is normalized (stripped, given a trailing ';' if
    missing, then two newlines) and appended to its target database's buffer.
    """
    create_sql: Dict[str, str] = {}
    for _name, test in sorted(tests.items(), key=_create_order_key):
        test_db = test.get("database", "postgres")
        if test.get("icu"):
            test["collation"] = 1
        if not test.get("create_sql"):
            continue
        if not collation_support and test.get("collation"):
            continue
        if not supports_icu and test.get("icu"):
            continue
        sql = test["create_sql"]
        sql = sql.rstrip("\n")
        if not sql.endswith(";"):
            sql += ";"
        create_sql[test_db] = create_sql.get(test_db, "") + sql + "\n\n"
    return create_sql


def _check_test_definitions(tests: Dict[str, dict], test_key: str) -> None:
    """Reproduce the Perl die() sanity checks for like/unlike completeness."""
    for name, test in tests.items():
        if test.get("all_runs") is None and test.get("like") is None:
            raise AssertionError(f'missing "like" in test "{name}"')
        unlike = test.get("unlike") or {}
        like = test.get("like") or {}
        if unlike.get(test_key) and like.get(test_key) is None:
            raise AssertionError(
                f'useless "unlike" entry "{test_key}" in test "{name}"'
            )


def _run_tests_for_output(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    run: str,
    test_key: str,
    run_db: str,
    output_file: str,
    tests: Dict[str, dict],
    collation_support: bool,
    supports_icu: bool,
) -> None:
    """Apply every test's like/unlike rule to one run's dumped SQL."""
    _check_test_definitions(tests, test_key)
    for name in sorted(tests):
        test = tests[name]
        like = test.get("like") or {}
        unlike = test.get("unlike") or {}
        regexp: Pattern[str] = test["regexp"]
        test_db = test.get("database", "postgres")

        if not collation_support and test.get("collation"):
            continue
        if not supports_icu and test.get("icu"):
            continue
        if run_db != test_db:
            continue

        if (like.get(test_key) or test.get("all_runs")) and unlike.get(
            test_key
        ) is None:
            assert regexp.search(
                output_file
            ), f"{run}: should dump {name}\nReview {run} results"
        else:
            assert not regexp.search(
                output_file
            ), f"{run}: should not dump {name}\nReview {run} results"


def test_002_pg_dump(create_pg):
    """pg_dump/pg_restore matrix against a single seeded server."""
    with tempfile.TemporaryDirectory(prefix="pg_dump_002") as tempdir:
        _run_matrix(create_pg, tempdir)


def _seed_server(node, tempdir, tests, collation_support, supports_icu):
    """Create extra databases and run the ordered create_sql per database."""
    node.safe_psql("create database regress_pg_dump_test;")
    node.safe_psql("create database regress_public_owner;")
    create_sql = _build_create_sql(tests, collation_support, supports_icu)
    for db in sorted(create_sql):
        node.safe_psql(create_sql[db], dbname=db)


def _negative_tests(node, port, tempdir):
    """Run pg_dump/pg_dumpall error-path checks (mirrors the command_fails_like
    and command_ok preamble of the Perl driver)."""
    node.command_fails_like(
        ["pg_dump", "--port", str(port), "qqq"],
        r'pg_dump: error: connection to server .* failed: FATAL:  database "qqq" does not exist',
        "connecting to a non-existent database",
    )
    node.command_fails_like(
        ["pg_dump", "--dbname", "regression_invalid"],
        r'pg_dump: error: connection to server .* failed: FATAL:  cannot connect to invalid database "regression_invalid"',
        "connecting to an invalid database",
    )
    node.command_fails_like(
        ["pg_dump", "--port", str(port), "--role", "regress_dump_test_role"],
        r"pg_dump: error: query failed: ERROR:  permission denied for",
        "connecting with an unprivileged user",
    )
    node.command_fails_like(
        ["pg_dump", "--port", str(port), "--schema", "nonexistent"],
        r"pg_dump: error: no matching schemas were found",
        "dumping a non-existent schema",
    )
    node.command_fails_like(
        ["pg_dump", "--port", str(port), "--table", "nonexistent"],
        r"pg_dump: error: no matching tables were found",
        "dumping a non-existent table",
    )
    node.command_fails_like(
        ["pg_dump", "--port", str(port), "--strict-names", "--schema", "nonexistent*"],
        r"pg_dump: error: no matching schemas were found for pattern",
        "no matching schemas",
    )
    node.command_fails_like(
        [
            "pg_dump",
            "--port",
            str(port),
            "--strict-names",
            "--schema-only",
            "--statistics",
        ],
        r"pg_dump: error: options --statistics and -s/--schema-only cannot be used together",
        "cannot use --statistics and --schema-only together",
    )
    node.command_fails_like(
        ["pg_dump", "--port", str(port), "--strict-names", "--table", "nonexistent*"],
        r"pg_dump: error: no matching tables were found for pattern",
        "no matching tables",
    )
    node.command_fails_like(
        ["pg_dumpall", "--exclude-database", "."],
        r"pg_dumpall: error: improper qualified name \(too many dotted names\): \.",
        'pg_dumpall: option --exclude-database rejects multipart pattern "."',
    )
    node.command_fails_like(
        ["pg_dumpall", "--exclude-database", "myhost.mydb"],
        r"pg_dumpall: error: improper qualified name \(too many dotted names\): myhost\.mydb",
        "pg_dumpall: option --exclude-database rejects multipart database names",
    )
    node.command_ok(
        [
            "pg_dump",
            "--port",
            str(port),
            "--schema",
            "pg_catalog",
            "--file",
            f"{tempdir}/pgdump_pgcatalog.dmp",
        ],
        "pg_dump: option -n pg_catalog",
    )
    node.command_ok(
        [
            "pg_dumpall",
            "--port",
            str(port),
            "--exclude-database",
            '"myhost.mydb"',
            "--file",
            f"{tempdir}/pgdumpall.dmp",
        ],
        "pg_dumpall: option --exclude-database handles database names with embedded dots",
    )
    node.command_fails_like(
        ["pg_dump", "--schema", "myhost.mydb.myschema"],
        r"pg_dump: error: improper qualified name \(too many dotted names\): myhost\.mydb\.myschema",
        "pg_dump: option --schema rejects three-part schema names",
    )
    node.command_fails_like(
        ["pg_dump", "--schema", "otherdb.myschema"],
        r"pg_dump: error: cross-database references are not implemented: otherdb\.myschema",
        "pg_dump: option --schema rejects cross-database multipart schema names",
    )
    node.command_fails_like(
        ["pg_dump", "--schema", "."],
        r"pg_dump: error: cross-database references are not implemented: \.",
        'pg_dump: option --schema rejects degenerate two-part schema name: "."',
    )
    node.command_fails_like(
        ["pg_dump", "--schema", '"some.other.db".myschema'],
        r'pg_dump: error: cross-database references are not implemented: "some\.other\.db"\.myschema',
        "pg_dump: option --schema rejects cross-database multipart schema names with embedded dots",
    )
    node.command_fails_like(
        ["pg_dump", "--schema", ".."],
        r"pg_dump: error: improper qualified name \(too many dotted names\): \.\.",
        'pg_dump: option --schema rejects degenerate three-part schema name: ".."',
    )
    node.command_fails_like(
        ["pg_dump", "--table", "myhost.mydb.myschema.mytable"],
        r"pg_dump: error: improper relation name \(too many dotted names\): myhost\.mydb\.myschema\.mytable",
        "pg_dump: option --table rejects four-part table names",
    )
    node.command_fails_like(
        ["pg_dump", "--table", "otherdb.pg_catalog.pg_class"],
        r"pg_dump: error: cross-database references are not implemented: otherdb\.pg_catalog\.pg_class",
        "pg_dump: option --table rejects cross-database three part table names",
    )
    node.command_fails_like(
        [
            "pg_dump",
            "--port",
            str(port),
            "--table",
            '"some.other.db".pg_catalog.pg_class',
        ],
        r'pg_dump: error: cross-database references are not implemented: "some\.other\.db"\.pg_catalog\.pg_class',
        "pg_dump: option --table rejects cross-database three part table names with embedded dots",
    )


def _run_matrix(create_pg, tempdir: str) -> None:
    """Seed a server, then execute every run x test pair (mirrors the driver)."""
    supports_icu = os.environ.get("with_icu") == "yes"
    supports_gzip = pypg.check_pg_config(r"#define HAVE_LIBZ 1")

    pgdump_runs = _pgdump_runs(tempdir, supports_gzip)
    tests = _tests()

    node = create_pg("main")
    port = node.port

    # Determine whether this build supports CREATE COLLATION (libc provider).
    collation_check = node.psql_capture(
        'CREATE COLLATION testing FROM "C"; DROP COLLATION testing;',
        on_error_stop=False,
    )
    collation_support = "ERROR: " not in collation_check.stderr

    # ICU doesn't work with some encodings.
    encoding = node.safe_psql("show server_encoding")
    if encoding == "SQL_ASCII":
        supports_icu = False

    _seed_server(node, tempdir, tests, collation_support, supports_icu)

    _negative_tests(node, port, tempdir)

    for run in sorted(pgdump_runs):
        spec = pgdump_runs[run]

        node.command_ok(spec["dump_cmd"], f"{run}: pg_dump runs")

        for glob_pattern in spec.get("glob_patterns") or []:
            matches = glob.glob(glob_pattern)
            ok = len(matches) > 1 or (len(matches) == 1 and os.path.isfile(matches[0]))
            assert ok, f"{run}: glob check for {glob_pattern}"

        cmd_like = spec.get("command_like")
        if cmd_like:
            node.command_like(
                cmd_like["command"],
                cmd_like["expected"],
                f"{run}: {cmd_like['name']}",
            )

        restore_cmd: Optional[list] = spec.get("restore_cmd")
        if restore_cmd:
            node.command_ok(restore_cmd, f"{run}: pg_restore runs")

        test_key = spec.get("test_key", run)
        run_db = spec.get("database", "postgres")

        output_file = pypg.slurp_file(os.path.join(tempdir, f"{run}.sql"))

        _run_tests_for_output(
            run, test_key, run_db, output_file, tests, collation_support, supports_icu
        )

    node.stop("fast")
