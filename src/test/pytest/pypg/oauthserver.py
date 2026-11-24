# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Python port of src/test/modules/oauth_validator/t/OAuth/Server.pm.

Glue between the pytest tests and the mock OAuth authorization server daemon
implemented in t/oauth_server.py. The daemon serves HTTPS on 127.0.0.1 (IPv4
only) using the SSL certificates under cert_dir; libpq must point PGOAUTHCAFILE
at the matching CA. The daemon prints its ephemeral port number to stdout and
then closes stdout, so the parent reads to EOF to learn the port (mirroring the
popen()-based handshake in Server.pm).
"""

import os
import pathlib
import signal
import subprocess
import sys
from typing import Optional


class OAuthServer:
    """Runs the mock OAuth authorization server daemon for a test module.

    Mirrors OAuth::Server: run() launches t/oauth_server.py, captures the
    advertised port, and stop() sends SIGTERM and waits for the daemon to exit.
    """

    def __init__(self) -> None:
        self._proc: Optional[subprocess.Popen] = None
        self._port: Optional[int] = None

    @property
    def port(self) -> int:
        """Return the port the daemon is listening on (set by run())."""
        if self._port is None:
            raise RuntimeError("OAuth server has not been started")
        return self._port

    def run(self) -> None:
        """Launch the authorization server daemon in t/oauth_server.py.

        Uses the PYTHON interpreter from the environment when set (as
        Server.pm does), falling back to the interpreter running the tests.
        The daemon prints its port to stdout and then closes stdout; we read
        the entire stream to obtain the port number.
        """
        script = (
            pathlib.Path(__file__).resolve().parents[4]
            / "src"
            / "test"
            / "modules"
            / "oauth_validator"
            / "t"
            / "oauth_server.py"
        )
        python = os.environ.get("PYTHON") or sys.executable

        # pylint: disable-next=consider-using-with
        self._proc = subprocess.Popen(
            [python, str(script)],
            stdout=subprocess.PIPE,
            encoding="utf-8",
        )

        assert self._proc.stdout is not None
        line = self._proc.stdout.read()
        if not line:
            raise RuntimeError("failed to read port number from OAuth server")

        text = line.strip()
        if not text.isdigit():
            raise RuntimeError(
                "OAuth server did not advertise a valid port: {!r}".format(text)
            )
        self._port = int(text)

    def stop(self) -> None:
        """Send SIGTERM to the daemon and wait for it to exit.

        Idempotent: a second call (or a call before run()) is a no-op, matching
        the END-block guard in the Perl tests.
        """
        if self._proc is None:
            return

        self._proc.send_signal(signal.SIGTERM)
        if self._proc.stdout is not None:
            self._proc.stdout.close()
        self._proc.wait()
        self._proc = None
        self._port = None
