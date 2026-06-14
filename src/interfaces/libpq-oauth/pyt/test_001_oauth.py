# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Port of src/interfaces/libpq-oauth/t/001_oauth.pl.

Defers entirely to the oauth_tests C executable (a unit-test program for the
libpq-oauth client flow). The wrapper runs it and requires a successful exit;
its stdout/stderr are captured and surfaced on failure for debugging.
"""

import sys


def test_001_oauth(pg_bin):
    """The oauth_tests executable must run to a successful exit."""
    result = pg_bin.result(["oauth_tests"])
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    assert result.rc == 0, "oauth_tests returned {}".format(result.rc)
