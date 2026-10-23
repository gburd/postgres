# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/interfaces/libpq/t/002_api.pl.

libpq C API smoke test via the libpq_testclient helper: PQsslAttribute(NULL, "library") returns the SSL library name when built with OpenSSL, otherwise reports SSL is not enabled.
Generated from the Perl original via .agent/gen_golden.py.
"""

import os


def test_002_api(pg_bin):
    """libpq PQsslAttribute(NULL, library) behavior with/without OpenSSL."""
    result = pg_bin.run_command(["libpq_testclient", "--ssl"])
    if os.environ.get("with_ssl") == "openssl":
        assert (
            result.stdout == "OpenSSL"
        ), 'PQsslAttribute(NULL, "library") returns "OpenSSL"'
    else:
        assert (
            result.stderr == "SSL is not enabled"
        ), 'PQsslAttribute(NULL, "library") returns NULL'
