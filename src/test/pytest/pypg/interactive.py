# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""A PTY-backed interactive psql session.

Python analog of PostgreSQL::Test::BackgroundPsql in interactive mode (which
Perl drives via IO::Pty). psql is run on a pseudo-terminal so it believes it is
interactive and enables readline/libedit (needed for tab-completion and
line-editing tests). The single combined output stream (stdout+stderr on the
PTY) is accumulated and polled for a regex, mirroring query_until.
"""

import os
import pty
import re
import select
import struct
import subprocess
import termios
import time
import fcntl

from ._env import test_timeout_default


class InteractivePsql:
    """An interactive (PTY) psql session driven by sending input + matching output.

    Mirrors the interactive form of PostgreSQL::Test::BackgroundPsql: send raw
    bytes (including tab/control characters) and wait until the accumulated
    terminal output matches a regex.
    """

    def __init__(self, cmd, env, timeout=None):
        self._cmd = cmd
        self._env = env
        self._timeout = timeout if timeout is not None else test_timeout_default()
        self._master, slave = pty.openpty()
        self._proc = subprocess.Popen(  # pylint: disable=consider-using-with
            cmd,
            stdin=slave,
            stdout=slave,
            stderr=slave,
            env=env,
            close_fds=True,
        )
        os.close(slave)
        self._buf = ""
        self.timed_out = False

    def set_winsize(self, rows, cols):
        """Set the terminal window size (rows x cols) for pagination tests."""
        fcntl.ioctl(
            self._master, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0)
        )

    def set_query_timer_restart(self):
        """No-op timer reset (kept for parity with BackgroundPsql)."""

    def _drain(self, deadline):
        while time.monotonic() < deadline:
            ready, _, _ = select.select([self._master], [], [], 0.05)
            if not ready:
                continue
            try:
                chunk = os.read(self._master, 4096)
            except OSError:
                return
            if not chunk:
                return
            self._buf += chunk.decode("utf-8", errors="replace")
            return

    def query_until(self, pattern, send):
        """Send input, then read until the accumulated output matches pattern.

        Returns the output consumed up to and including the match (then clears
        the buffer). Sets timed_out and returns what was seen on timeout.
        """
        regex = pattern if hasattr(pattern, "search") else re.compile(pattern)
        if send:
            os.write(self._master, send.encode("utf-8"))
        deadline = time.monotonic() + self._timeout
        self.timed_out = False
        while True:
            if regex.search(self._buf):
                out = self._buf
                self._buf = ""
                return out
            if time.monotonic() > deadline:
                self.timed_out = True
                out = self._buf
                self._buf = ""
                return out
            self._drain(deadline)

    def send(self, data):
        """Write raw data to the terminal without waiting for output."""
        os.write(self._master, data.encode("utf-8"))

    def quit(self):
        """Close the session (send \\q and EOF), returning the exit code."""
        try:
            os.write(self._master, "\\q\n".encode("utf-8"))
        except OSError:
            pass
        try:
            self._proc.wait(timeout=self._timeout)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait()
        os.close(self._master)
        return self._proc.returncode
