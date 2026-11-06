# Copyright (c) 2025, PostgreSQL Global Development Group

"""
libpq testing utilities - ctypes bindings and helpers for PostgreSQL's libpq library.

This module provides Python wrappers around libpq for use in pytest tests.
"""

from . import errors
from .errors import (
    LibpqError,
    for_sqlstate,
    SyntaxErrorState,
    UndefinedTable,
    UndefinedColumn,
    InsufficientPrivilege,
    UniqueViolation,
    ForeignKeyViolation,
    NotNullViolation,
    CheckViolation,
    SerializationFailure,
    DeadlockDetected,
    QueryCanceled,
    AdminShutdown,
    CrashShutdown,
    CannotConnectNow,
    ReadOnlySqlTransaction,
    ObjectInUse,
)
from .constants import ConnectionStatus, DiagField, ExecStatus
from .findlib import load_libpq_handle, libpq_abi_skip_reason
from .oids import register_type_info
from .pgnotify import PGnotify, read_notification
from .result import PGresult, ResultData
from .session import PGconn, connect, connstr

__all__ = [
    "errors",
    "LibpqError",
    "for_sqlstate",
    "SyntaxErrorState",
    "UndefinedTable",
    "UndefinedColumn",
    "InsufficientPrivilege",
    "UniqueViolation",
    "ForeignKeyViolation",
    "NotNullViolation",
    "CheckViolation",
    "SerializationFailure",
    "DeadlockDetected",
    "QueryCanceled",
    "AdminShutdown",
    "CrashShutdown",
    "CannotConnectNow",
    "ReadOnlySqlTransaction",
    "ObjectInUse",
    "ConnectionStatus",
    "DiagField",
    "ExecStatus",
    "PGconn",
    "PGresult",
    "ResultData",
    "PGnotify",
    "read_notification",
    "connect",
    "connstr",
    "load_libpq_handle",
    "libpq_abi_skip_reason",
    "register_type_info",
]
