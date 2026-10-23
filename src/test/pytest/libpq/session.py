# Copyright (c) 2025, PostgreSQL Global Development Group

"""The in-process libpq connection (Session) and connect()/connstr() helpers.

``PGconn`` wraps a live connection: ``exec()`` returns the raw result, ``sql()``
returns simplified Python values (1x1 -> scalar, Nx1 -> list, ...), and
``query()`` returns a structured :class:`ResultData` with column names/types.
All raise the SQLSTATE-specific :class:`LibpqError` subclass on failure.
"""

import contextlib
import ctypes
from typing import Any, Callable, Dict, Optional

from .constants import ConnectionStatus, ExecStatus, _PGconn_p
from .errors import LibpqError
from .oids import simplify_query_results
from .pgnotify import read_notification
from .result import PGresult, ResultData


class PGconn(contextlib.AbstractContextManager):
    """Wraps a raw _PGconn_p with a more friendly interface."""

    def __init__(
        self,
        lib: ctypes.CDLL,
        handle: _PGconn_p,  # type: ignore[valid-type]
        stack: contextlib.ExitStack,
    ):
        self._lib = lib
        # Cleared to None on __exit__ once the connection has been finished.
        self._handle = handle
        self._stack = stack
        # The most recent result, cleared when the next exec() runs so a long
        # polling loop on one connection does not accumulate unfreed PGresults.
        self._last_result: Optional[PGresult] = None

    def __exit__(self, *exc):
        self._lib.PQfinish(self._handle)
        self._handle = None

    def exec(self, query: str) -> PGresult:
        """
        Executes a query via PQexec() and returns a PGresult.

        The previous result from this connection is cleared first, so issuing
        many queries on one connection (e.g. a poll loop) frees each PGresult
        promptly rather than deferring all of them to end-of-test cleanup.
        """
        if self._last_result is not None:
            self._last_result.close()
            self._last_result = None
        res = self._lib.PQexec(self._handle, query.encode())
        result = self._stack.enter_context(PGresult(self._lib, res))
        self._last_result = result
        return result

    def sql(self, query: str):  # pylint: disable=inconsistent-return-statements
        """
        Executes a query and raises an exception if it fails.
        Returns the query results with automatic type conversion and simplification.
        For commands that don't return data (INSERT, UPDATE, etc.), returns None.

        Examples:
        - SELECT 1 -> 1
        - SELECT 1, 2 -> (1, 2)
        - SELECT * FROM generate_series(1, 3) -> [1, 2, 3]
        - SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t -> [(1, 'a'), (2, 'b')]
        - CREATE TABLE ... -> None
        - INSERT INTO ... -> None
        """
        res = self.exec(query)
        status = res.status()

        if status == ExecStatus.PGRES_COMMAND_OK:
            return None
        if status == ExecStatus.PGRES_TUPLES_OK:
            results = res.fetch_all()
            return simplify_query_results(results)
        # PGRES_FATAL_ERROR and anything else: raise (raise_error is NoReturn).
        res.raise_error()

    def query(self, query: str) -> ResultData:
        """Execute *query* and return a structured ResultData.

        Unlike sql(), this never simplifies: the caller gets the column names,
        column type OIDs, and the full row list, which is what tests asserting
        on result shape or column typing need. Raises on a failed status.
        """
        res = self.exec(query)
        status = res.status()
        if status not in (ExecStatus.PGRES_TUPLES_OK, ExecStatus.PGRES_COMMAND_OK):
            res.raise_error()
        return res.data()

    def get_notification(self):
        """Return one pending LISTEN/NOTIFY notification, or None.

        Consumes any input waiting on the socket first, then pops a single
        notification. The result is ``{"channel", "pid", "payload"}``. This is
        the in-process libpq equivalent of reading psql's notification echo.
        """
        self._lib.PQconsumeInput(self._handle)
        raw = self._lib.PQnotifies(self._handle)
        return read_notification(self._lib, raw)

    def get_all_notifications(self):
        """Return all currently-pending notifications as a list of dicts."""
        notifications = []
        while True:
            notify = self.get_notification()
            if notify is None:
                return notifications
            notifications.append(notify)


def connstr(opts: Dict[str, Any]) -> str:
    """
    Flattens the provided options into a libpq connection string. Values
    are converted to str and quoted/escaped as necessary.
    """
    settings = []

    for k, v in opts.items():
        v = str(v)
        if not v:
            v = "''"
        else:
            v = v.replace("\\", "\\\\")
            v = v.replace("'", "\\'")

            if " " in v:
                v = f"'{v}'"

        settings.append(f"{k}={v}")

    return " ".join(settings)


def connect(
    libpq_handle: ctypes.CDLL,
    stack: contextlib.ExitStack,
    remaining_timeout_fn: Callable[[], float],
    **opts,
) -> PGconn:
    """
    Connects to a server, using the given connection options, and
    returns a PGconn object wrapping the connection handle. A
    failure will raise LibpqError.

    Connections honor PG_TEST_TIMEOUT_DEFAULT unless connect_timeout is
    explicitly overridden in opts.

    Args:
        libpq_handle: ctypes.CDLL handle to libpq library
        stack: ExitStack for managing connection cleanup
        remaining_timeout_fn: Function that returns remaining timeout in seconds
        **opts: Connection options (host, port, dbname, etc.)

    Returns:
        PGconn: Connected database connection

    Raises:
        LibpqError: If connection fails
    """

    if "connect_timeout" not in opts:
        t = int(remaining_timeout_fn())
        opts["connect_timeout"] = max(t, 1)

    conn_p = libpq_handle.PQconnectdb(connstr(opts).encode())

    # Check connection status before adding to stack
    if libpq_handle.PQstatus(conn_p) != ConnectionStatus.CONNECTION_OK:
        msg = libpq_handle.PQerrorMessage(conn_p)
        error_msg = msg.decode() if msg else "connection failed (out of memory?)"
        # Manually close the failed connection
        libpq_handle.PQfinish(conn_p)
        raise LibpqError(error_msg)

    # Connection succeeded - add to stack for cleanup
    conn = stack.enter_context(PGconn(libpq_handle, conn_p, stack=stack))
    return conn
