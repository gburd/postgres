# Copyright (c) 2025, PostgreSQL Global Development Group

"""The libpq result wrapper and its structured form.

``PGresult`` wraps a raw PGresult pointer (status, error fields, row fetch).
``ResultData`` is the structured snapshot a query returns -- status plus the
column ``names`` and ``types`` (OIDs) and the converted ``rows`` -- so callers
can introspect a result's shape, not just its values. The column metadata is
adopted from Andrew Dunstan's ResultData.
"""

import contextlib
import ctypes
from dataclasses import dataclass, field
from typing import List, NoReturn, Optional, Tuple

from .constants import DiagField, ExecStatus, _PGresult_p
from .errors import LibpqError, for_sqlstate
from .oids import _convert_pg_value


@dataclass
class ResultData:
    """Structured form of a successful query result.

    Carries the column ``names`` and ``types`` (PostgreSQL type OIDs) alongside
    the converted ``rows``, so a test can assert on a result's shape and column
    typing rather than only its values.

    Co-authored-by: Andrew Dunstan <andrew@dunslane.net>
    """

    status: int
    names: List[str] = field(default_factory=list)
    types: List[int] = field(default_factory=list)
    rows: List[Tuple] = field(default_factory=list)


class PGresult(contextlib.AbstractContextManager):
    """Wraps a raw _PGresult_p with a more friendly interface."""

    def __init__(self, lib: ctypes.CDLL, res: _PGresult_p):  # type: ignore[valid-type]
        self._lib = lib
        # Cleared to None on __exit__ once the result has been freed.
        self._res: Optional[_PGresult_p] = res  # type: ignore[valid-type]

    def __exit__(self, *exc):
        self.close()

    def close(self):
        """Free the underlying PGresult; idempotent."""
        if self._res is not None:
            self._lib.PQclear(self._res)
            self._res = None

    def status(self) -> ExecStatus:
        # A NULL PGresult (PQexec on OOM / lost connection) has no status; report
        # FATAL_ERROR so callers route it to raise_error() instead of mistaking
        # PQresultStatus(NULL)==0 for an empty query and silently returning None.
        if not self._res:
            return ExecStatus.PGRES_FATAL_ERROR
        return ExecStatus(self._lib.PQresultStatus(self._res))

    def error_message(self):
        """Returns the error message associated with this result."""
        msg = self._lib.PQresultErrorMessage(self._res)
        return msg.decode() if msg else ""

    def _get_error_field(self, field_: DiagField) -> Optional[str]:
        """Get an error field from the result using PQresultErrorField."""
        val = self._lib.PQresultErrorField(self._res, int(field_))
        return val.decode() if val else None

    def raise_error(self) -> NoReturn:
        """
        Raises LibpqError (or its SQLSTATE-specific subclass) with diagnostic
        information from the result.
        """
        if not self._res:
            raise LibpqError("query failed: out of memory or connection lost")

        sqlstate = self._get_error_field(DiagField.SQLSTATE)
        primary = self._get_error_field(DiagField.MESSAGE_PRIMARY)
        detail = self._get_error_field(DiagField.MESSAGE_DETAIL)
        hint = self._get_error_field(DiagField.MESSAGE_HINT)
        severity = self._get_error_field(DiagField.SEVERITY)
        schema_name = self._get_error_field(DiagField.SCHEMA_NAME)
        table_name = self._get_error_field(DiagField.TABLE_NAME)
        column_name = self._get_error_field(DiagField.COLUMN_NAME)
        datatype_name = self._get_error_field(DiagField.DATATYPE_NAME)
        constraint_name = self._get_error_field(DiagField.CONSTRAINT_NAME)
        context = self._get_error_field(DiagField.CONTEXT)

        position_str = self._get_error_field(DiagField.STATEMENT_POSITION)
        position = int(position_str) if position_str else None

        raise for_sqlstate(sqlstate)(
            primary or self.error_message(),
            sqlstate=sqlstate,
            severity=severity,
            primary=primary,
            detail=detail,
            hint=hint,
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            datatype_name=datatype_name,
            constraint_name=constraint_name,
            position=position,
            context=context,
        )

    def column_names(self) -> List[str]:
        """Return the result's column names, in order."""
        ncols = self._lib.PQnfields(self._res)
        return [self._lib.PQfname(self._res, col).decode() for col in range(ncols)]

    def column_types(self) -> List[int]:
        """Return the result's column type OIDs, in order."""
        ncols = self._lib.PQnfields(self._res)
        return [self._lib.PQftype(self._res, col) for col in range(ncols)]

    def fetch_all(self) -> List[Tuple]:
        """
        Fetch all rows and convert to Python types.
        Returns a list of tuples, with values converted based on their PostgreSQL type.
        """
        nrows = self._lib.PQntuples(self._res)
        ncols = self._lib.PQnfields(self._res)

        # Get type OIDs for each column
        type_oids = [self._lib.PQftype(self._res, col) for col in range(ncols)]

        results = []
        for row in range(nrows):
            row_data: list = []
            for col in range(ncols):
                if self._lib.PQgetisnull(self._res, row, col):
                    row_data.append(None)
                else:
                    value = self._lib.PQgetvalue(self._res, row, col).decode()
                    row_data.append(_convert_pg_value(value, type_oids[col]))
            results.append(tuple(row_data))

        return results

    def data(self) -> ResultData:
        """Return a ResultData snapshot: status, column names/types, and rows."""
        status = self.status()
        if status == ExecStatus.PGRES_TUPLES_OK:
            return ResultData(
                status=int(status),
                names=self.column_names(),
                types=self.column_types(),
                rows=self.fetch_all(),
            )
        return ResultData(status=int(status))
