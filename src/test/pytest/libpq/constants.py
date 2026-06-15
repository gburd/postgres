# Copyright (c) 2025, PostgreSQL Global Development Group

"""ctypes declarations and libpq enums for the in-process binding.

The opaque ``PGconn``/``PGresult`` C structs and their pointer types, plus the
status and diagnostic-field enumerations from postgres_ext.h. Kept in one place
so the rest of the libpq layer (findlib, result, session) shares a single set of
type declarations.
"""

import ctypes
import enum


# PG_DIAG field identifiers from postgres_ext.h
class DiagField(enum.IntEnum):
    """PG_DIAG_* field identifiers used with PQresultErrorField()."""

    SEVERITY = ord("S")
    SEVERITY_NONLOCALIZED = ord("V")
    SQLSTATE = ord("C")
    MESSAGE_PRIMARY = ord("M")
    MESSAGE_DETAIL = ord("D")
    MESSAGE_HINT = ord("H")
    STATEMENT_POSITION = ord("P")
    INTERNAL_POSITION = ord("p")
    INTERNAL_QUERY = ord("q")
    CONTEXT = ord("W")
    SCHEMA_NAME = ord("s")
    TABLE_NAME = ord("t")
    COLUMN_NAME = ord("c")
    DATATYPE_NAME = ord("d")
    CONSTRAINT_NAME = ord("n")
    SOURCE_FILE = ord("F")
    SOURCE_LINE = ord("L")
    SOURCE_FUNCTION = ord("R")


class ConnectionStatus(enum.IntEnum):
    """PostgreSQL connection status codes from libpq."""

    CONNECTION_OK = 0
    CONNECTION_BAD = 1


class ExecStatus(enum.IntEnum):
    """PostgreSQL result status codes from PQresultStatus."""

    PGRES_EMPTY_QUERY = 0
    PGRES_COMMAND_OK = 1
    PGRES_TUPLES_OK = 2
    PGRES_COPY_OUT = 3
    PGRES_COPY_IN = 4
    PGRES_BAD_RESPONSE = 5
    PGRES_NONFATAL_ERROR = 6
    PGRES_FATAL_ERROR = 7
    PGRES_COPY_BOTH = 8
    PGRES_SINGLE_TUPLE = 9
    PGRES_PIPELINE_SYNC = 10
    PGRES_PIPELINE_ABORTED = 11


class _PGconn(ctypes.Structure):
    pass


class _PGresult(ctypes.Structure):
    pass


_PGconn_p = ctypes.POINTER(_PGconn)
_PGresult_p = ctypes.POINTER(_PGresult)
