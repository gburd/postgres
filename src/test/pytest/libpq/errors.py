# Copyright (c) 2025, PostgreSQL Global Development Group

"""Exception classes for libpq errors.

``LibpqError`` carries the PostgreSQL diagnostic fields (SQLSTATE, severity,
detail, hint, ...) when libpq reports them. It is the lowest layer of the
framework's SQL-error hierarchy; ``pypg.PgSqlError`` is an alias for it, so
catching either name works and the layering (libpq below pypg) is preserved
without a circular import.
"""

from __future__ import annotations

from typing import Optional


class LibpqError(RuntimeError):
    """A SQL/libpq operation failed, carrying PostgreSQL diagnostic fields.

    ``sqlstate`` and the convenience ``sqlstate_class`` (its first two
    characters) are the stable, locale-independent way to assert on a specific
    error condition.
    """

    def __init__(
        self,
        message: str,
        *,
        sqlstate: Optional[str] = None,
        severity: Optional[str] = None,
        primary: Optional[str] = None,
        detail: Optional[str] = None,
        hint: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
        datatype_name: Optional[str] = None,
        constraint_name: Optional[str] = None,
        position: Optional[int] = None,
        context: Optional[str] = None,
    ):
        super().__init__(message)
        self.sqlstate = sqlstate
        self.severity = severity
        self.primary = primary
        self.detail = detail
        self.hint = hint
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_name = column_name
        self.datatype_name = datatype_name
        self.constraint_name = constraint_name
        self.position = position
        self.context = context

    @property
    def sqlstate_class(self) -> Optional[str]:
        """The two-character SQLSTATE class, or None if no SQLSTATE is set."""
        if self.sqlstate and len(self.sqlstate) >= 2:
            return self.sqlstate[:2]
        return None
