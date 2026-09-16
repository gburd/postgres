# Copyright (c) 2025, PostgreSQL Global Development Group

"""Enumerations for the small set of mode strings the framework passes through.

Each enum subclasses ``str`` so its members compare equal to, and serialize as,
the exact literal PostgreSQL expects (the value handed to ``pg_ctl --mode`` or
interpolated into a ``pg_stat_replication`` column name). Methods accept either
the enum or the bare string, so call sites can adopt the enums incrementally
while old string call sites keep working.
"""

from __future__ import annotations

from enum import Enum


class StopMode(str, Enum):
    """How ``pg_ctl stop`` / ``restart`` shuts the server down."""

    SMART = "smart"
    FAST = "fast"
    IMMEDIATE = "immediate"


class CatchupMode(str, Enum):
    """Which ``pg_stat_replication`` LSN column ``wait_for_catchup`` waits on."""

    SENT = "sent"
    WRITE = "write"
    FLUSH = "flush"
    REPLAY = "replay"


class SlotCatchupMode(str, Enum):
    """Which slot LSN ``wait_for_slot_catchup`` waits on."""

    RESTART = "restart"
    CONFIRMED_FLUSH = "confirmed_flush"
