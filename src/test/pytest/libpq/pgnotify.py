# Copyright (c) 2025, PostgreSQL Global Development Group

"""The PGnotify struct and a helper to read a LISTEN/NOTIFY notification.

PQnotifies() returns a pointer to a heap-allocated PGnotify that the caller must
free with PQfreemem(); read_notification() casts the pointer to read the fields,
decodes the strings while the memory is still valid, then frees it.

Adopted from Andrew Dunstan's pgnotify module: it lets the in-process libpq
channel (PGconn) consume asynchronous notifications, which the psql channel gets
for free from psql's text echo.
"""

import ctypes


class PGnotify(ctypes.Structure):
    """typedef struct pgNotify { char *relname; int be_pid; char *extra; }."""

    _fields_ = [
        ("relname", ctypes.c_char_p),  # notification channel name
        ("be_pid", ctypes.c_int),  # PID of the notifying backend
        ("extra", ctypes.c_char_p),  # notification payload string
    ]


_PGnotify_p = ctypes.POINTER(PGnotify)


def read_notification(lib, raw):
    """Turn the raw PQnotifies pointer *raw* into a dict and free it.

    Returns ``{"channel", "pid", "payload"}`` or ``None`` if *raw* is NULL.
    """
    if not raw:
        return None
    notify = ctypes.cast(raw, _PGnotify_p).contents
    # Decode while the memory is still valid (before PQfreemem).
    result = {
        "channel": (
            notify.relname.decode("utf-8", "replace") if notify.relname else None
        ),
        "pid": notify.be_pid,
        "payload": (notify.extra.decode("utf-8", "replace") if notify.extra else None),
    }
    lib.PQfreemem(ctypes.c_void_p(raw))
    return result
