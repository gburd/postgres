# Copyright (c) 2025, PostgreSQL Global Development Group

"""Locate and load libpq at runtime for the in-process ctypes binding.

The framework dlopens libpq and calls it directly, so it must find the right
library for the build under test and confirm the interpreter shares its ABI
before loading (a 64-bit Python against a 32-bit libpq otherwise fails every
test). load_libpq_handle() returns a CDLL with the PQ* prototypes initialized.
"""

import ctypes
import os
import platform

from .constants import _PGconn_p, _PGresult_p


def _libpq_path(libdir, bindir):
    """Return the platform-specific full path to libpq for this build."""
    system = platform.system()
    if system in ("Linux", "FreeBSD", "NetBSD", "OpenBSD"):
        return os.path.join(libdir, "libpq.so.5")
    if system == "Darwin":
        return os.path.join(libdir, "libpq.5.dylib")
    if system == "Windows":
        # On Windows, libpq.dll is confusingly in bindir, not libdir.
        return os.path.join(bindir, "libpq.dll")
    raise AssertionError("the libpq fixture must be updated for {}".format(system))


def _elf_class(path):
    """Return 1 (ELFCLASS32), 2 (ELFCLASS64), or None if path is not ELF."""
    try:
        with open(path, "rb") as fh:
            ident = fh.read(5)
    except OSError:
        return None
    if ident[:4] != b"\x7fELF":
        return None
    return ident[4]  # e_ident[EI_CLASS]: 1 = 32-bit, 2 = 64-bit


def libpq_abi_skip_reason(libdir, bindir):
    """Return a reason to skip if this Python cannot load the build's libpq.

    The framework loads libpq in-process via ctypes, so the interpreter and the
    library must share an ABI. The common mismatch is a 64-bit Python against a
    32-bit libpq (meson's -m32 build), which otherwise fails every test with
    OSError: wrong ELF class. Detect it by reading the library's ELF header
    rather than dlopen()ing it -- a trial dlopen of an ASan-instrumented libpq
    would abort the process, not raise. Returns None when the ABI matches, when
    libpq cannot be located, or when the file is not ELF (macOS/Windows).

    Co-authored-by: Andrew Dunstan <andrew@dunslane.net>
    """
    try:
        path = _libpq_path(libdir, bindir)
    except AssertionError:
        return None
    elf_class = _elf_class(path)
    if elf_class is None:
        return None
    py_bits = ctypes.sizeof(ctypes.c_void_p) * 8
    lib_bits = 64 if elf_class == 2 else 32
    if py_bits != lib_bits:
        return (
            "{py}-bit Python cannot load {lib}-bit libpq ({path}); the "
            "in-process libpq framework needs a {lib}-bit interpreter".format(
                py=py_bits, lib=lib_bits, path=path
            )
        )
    return None


def load_libpq_handle(libdir, bindir):
    """
    Loads a ctypes handle for libpq. Some common function prototypes are
    initialized for general use.
    """
    libpq_path = _libpq_path(libdir, bindir)
    lib = ctypes.CDLL(libpq_path)

    #
    # Function Prototypes
    #

    lib.PQconnectdb.restype = _PGconn_p
    lib.PQconnectdb.argtypes = [ctypes.c_char_p]

    lib.PQstatus.restype = ctypes.c_int
    lib.PQstatus.argtypes = [_PGconn_p]

    lib.PQexec.restype = _PGresult_p
    lib.PQexec.argtypes = [_PGconn_p, ctypes.c_char_p]

    lib.PQresultStatus.restype = ctypes.c_int
    lib.PQresultStatus.argtypes = [_PGresult_p]

    lib.PQclear.restype = None
    lib.PQclear.argtypes = [_PGresult_p]

    lib.PQerrorMessage.restype = ctypes.c_char_p
    lib.PQerrorMessage.argtypes = [_PGconn_p]

    lib.PQfinish.restype = None
    lib.PQfinish.argtypes = [_PGconn_p]

    lib.PQresultErrorMessage.restype = ctypes.c_char_p
    lib.PQresultErrorMessage.argtypes = [_PGresult_p]

    lib.PQntuples.restype = ctypes.c_int
    lib.PQntuples.argtypes = [_PGresult_p]

    lib.PQnfields.restype = ctypes.c_int
    lib.PQnfields.argtypes = [_PGresult_p]

    lib.PQfname.restype = ctypes.c_char_p
    lib.PQfname.argtypes = [_PGresult_p, ctypes.c_int]

    lib.PQgetvalue.restype = ctypes.c_char_p
    lib.PQgetvalue.argtypes = [_PGresult_p, ctypes.c_int, ctypes.c_int]

    lib.PQgetisnull.restype = ctypes.c_int
    lib.PQgetisnull.argtypes = [_PGresult_p, ctypes.c_int, ctypes.c_int]

    lib.PQftype.restype = ctypes.c_uint
    lib.PQftype.argtypes = [_PGresult_p, ctypes.c_int]

    lib.PQresultErrorField.restype = ctypes.c_char_p
    lib.PQresultErrorField.argtypes = [_PGresult_p, ctypes.c_int]

    # Asynchronous notification (LISTEN/NOTIFY) handling.
    lib.PQconsumeInput.restype = ctypes.c_int
    lib.PQconsumeInput.argtypes = [_PGconn_p]

    lib.PQnotifies.restype = ctypes.c_void_p
    lib.PQnotifies.argtypes = [_PGconn_p]

    lib.PQfreemem.restype = None
    lib.PQfreemem.argtypes = [ctypes.c_void_p]

    return lib
