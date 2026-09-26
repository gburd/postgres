# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Minimal System V shared-memory helper via ctypes.

Python twin of the IPC::SharedMem usage in src/test/recovery/t/017_shm.pl: create
a shared-memory segment with an explicit key (the test uses the data directory's
inode, the same key PostgreSQL derives) so the server detects a conflicting
pre-existing segment, and remove it again.
"""

import ctypes
import ctypes.util

_IPC_CREAT = 0o1000
_IPC_EXCL = 0o2000
_IPC_RMID = 0
_S_IRUSR = 0o400
_S_IWUSR = 0o200

_libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
_libc.shmget.argtypes = [ctypes.c_int, ctypes.c_size_t, ctypes.c_int]
_libc.shmget.restype = ctypes.c_int
_libc.shmctl.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
_libc.shmctl.restype = ctypes.c_int


class SysVSharedMemory:
    """A System V shared-memory segment created with an explicit key.

    Mirrors IPC::SharedMem->new(key, size, IPC_CREAT|IPC_EXCL|S_IRUSR|S_IWUSR).
    create() returns an instance on success or None if the segment could not be
    created (e.g. it already exists); remove() deletes it.
    """

    def __init__(self, shmid):
        self.shmid = shmid

    @classmethod
    def create(cls, key, size=1024):
        """Create a new segment for key, or return None if creation failed."""
        ctypes.set_errno(0)
        shmid = _libc.shmget(key, size, _IPC_CREAT | _IPC_EXCL | _S_IRUSR | _S_IWUSR)
        if shmid < 0:
            return None
        return cls(shmid)

    def remove(self):
        """Remove the segment (IPC_RMID). Idempotent."""
        if self.shmid is None:
            return
        _libc.shmctl(self.shmid, _IPC_RMID, None)
        self.shmid = None
