# Copyright (c) 2025, PostgreSQL Global Development Group

"""Exception taxonomy for the pypg test framework.

The framework signals failure with exceptions rather than status returns, so a
test that does not explicitly tolerate a failure fails loudly at the point it
occurs. The hierarchy is:

    RuntimeError
      PgServerError             -- server lifecycle (start/stop/restart/backup)
      PgSqlError (= LibpqError) -- a SQL statement failed on the server

``PgSqlError`` is an alias for ``libpq.LibpqError`` (which lives in the lower
libpq layer to avoid a circular import); it carries the PostgreSQL diagnostic
fields (SQLSTATE, severity, detail, hint, ...) when they are available, whether
the statement ran in-process through libpq or was shelled out to psql. Tests
inspect ``sqlstate`` / ``sqlstate_class`` instead of regex-matching messages.
"""

from __future__ import annotations

from libpq.errors import LibpqError

# A failed SQL statement, regardless of execution channel (libpq or psql).
PgSqlError = LibpqError


class PgError(RuntimeError):
    """Base class for non-SQL errors raised by the pypg framework."""


class PgServerError(PgError):
    """A PostgreSQL server lifecycle operation failed.

    Raised by start/stop/restart/promote/backup and similar cluster operations.
    A test that expects such a failure should assert on it with
    ``pytest.raises(PgServerError)`` or tolerate it with
    ``contextlib.suppress(PgServerError)`` rather than passing a status flag.
    """


__all__ = ["PgError", "PgServerError", "PgSqlError", "LibpqError"]
