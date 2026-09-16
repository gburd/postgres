# Copyright (c) 2025, PostgreSQL Global Development Group

"""
An interactive psql session running in the background, mirroring
PostgreSQL::Test::BackgroundPsql.

psql is run with `--file -`, reading from a pipe we keep open, so queries can be
fed incrementally. Two reader threads accumulate stdout/stderr into buffers that
the pump-until helpers poll for a pattern (the Python analog of IPC::Run::pump).
"""

import re
import subprocess
import threading
import time
from typing import List, Optional

from ._env import test_timeout_default


class BackgroundPsql:
    """A long-lived psql session driven by feeding stdin and matching output."""

    def __init__(self, cmd: List[str], env, timeout=None, wait=True):
        self._cmd = cmd
        self._env = env
        self._timeout = timeout if timeout is not None else test_timeout_default()
        self._proc: Optional[subprocess.Popen] = None
        self._stdout = ""
        self._stderr = ""
        self._last_stderr = ""
        self._lock = threading.Lock()
        self._threads: List[threading.Thread] = []
        self._query_cnt = 1
        self._start()
        if wait:
            self.wait_connect()

    def _start(self):
        # pylint: disable=consider-using-with  # long-lived; closed in quit()
        self._proc = subprocess.Popen(
            self._cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=self._env,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        self._stdout = ""
        self._stderr = ""
        self._threads = [
            threading.Thread(target=self._reader, args=("out",), daemon=True),
            threading.Thread(target=self._reader, args=("err",), daemon=True),
        ]
        for thread in self._threads:
            thread.start()

    def _reader(self, which):
        assert self._proc is not None
        stream = self._proc.stdout if which == "out" else self._proc.stderr
        assert stream is not None
        for line in iter(stream.readline, ""):
            with self._lock:
                if which == "out":
                    self._stdout += line
                else:
                    self._stderr += line

    def _send(self, text):
        assert self._proc is not None and self._proc.stdin is not None
        self._proc.stdin.write(text)
        self._proc.stdin.flush()

    def send(self, text):
        """Feed raw text to psql's stdin without waiting for output."""
        self._send(text)

    def signal(self, sig):
        """Send a signal (e.g. signal.SIGINT) to the psql process."""
        assert self._proc is not None
        self._proc.send_signal(sig)

    @property
    def stdout(self):
        """The accumulated stdout captured so far.

        Mirrors Perl's ``$session->{stdout}``: lets a test inspect output that a
        fire-and-forget statement (sent via send()/query_until()) leaves behind
        once it finishes, e.g. the final result of a blocking WAIT FOR after the
        session is quit().
        """
        with self._lock:
            return self._stdout

    @property
    def stderr(self):
        """The accumulated stderr captured so far."""
        with self._lock:
            return self._stderr

    @property
    def last_stderr(self):
        """The stderr produced by the most recent query()/query_safe().

        Mirrors Perl's ``$session->{stderr}`` immediately after ``$session->query``:
        query() captures the statement's stderr separately and clears the live
        buffer, so a test that needs to assert on (or match a regex against) the
        error text of the just-run statement reads it here.
        """
        with self._lock:
            return self._last_stderr

    def _pump_until(self, want_out=None, want_err=None):
        rx_out = re.compile(want_out) if want_out else None
        rx_err = re.compile(want_err) if want_err else None
        deadline = time.monotonic() + self._timeout
        # Poll with an adaptive backoff: a tight initial interval keeps
        # per-query latency low for fast local statements (workloads that
        # issue thousands of round-trips), backing off to a coarse interval so
        # genuinely long waits don't busy-spin.
        interval = 0.0005
        while True:
            with self._lock:
                stdout, stderr = self._stdout, self._stderr
                ok_out = rx_out is None or rx_out.search(stdout)
                ok_err = rx_err is None or rx_err.search(stderr)
            if ok_out and ok_err:
                return
            if time.monotonic() > deadline:
                raise TimeoutError(
                    "background psql timed out waiting for "
                    "out={!r} err={!r}\nstdout:\n{}\nstderr:\n{}".format(
                        want_out, want_err, stdout, stderr
                    )
                )
            time.sleep(interval)
            if interval < 0.02:
                interval = min(interval * 2, 0.02)

    def wait_connect(self):
        """Wait until psql is connected and ready to consume input."""
        banner = "background_psql: ready"
        self._send("\\echo '{0}'\n\\warn '{0}'\n".format(banner))
        match = banner + r"\r?\n"
        self._pump_until(want_out=match, want_err=match)
        with self._lock:
            self._stdout = ""
            self._stderr = ""

    def query(self, query):
        """Run query and return its output (waits for completion via a banner)."""
        cnt = self._query_cnt
        self._query_cnt += 1
        banner = "background_psql: QUERY_SEPARATOR {}:".format(cnt)
        self._send("{q}\n;\n\\echo '{b}'\n\\warn '{b}'\n".format(q=query, b=banner))
        match = banner + r"\r?\n"
        self._pump_until(want_out=match, want_err=match)
        strip = r"\r?\n?" + re.escape(banner) + r"\r?\n"
        with self._lock:
            output = re.sub(strip, "", self._stdout)
            self._last_stderr = re.sub(strip, "", self._stderr)
            self._stderr = ""
            self._stdout = ""
        return output

    def query_safe(self, query):
        """Run query and return its output, raising if psql wrote any stderr.

        Mirrors PostgreSQL::Test::BackgroundPsql->query_safe, which dies on any
        non-empty stderr from the statement (so a WARNING/NOTICE is fatal too,
        not only ERROR/FATAL/PANIC).
        """
        output = self.query(query)
        if self._last_stderr != "":
            raise RuntimeError(
                "query_safe failed: {}\nquery was: {}".format(
                    self._last_stderr.strip(), query
                )
            )
        return output

    def set_query_timer_restart(self):
        """Reset the per-query timeout window.

        Mirrors BackgroundPsql->set_query_timer_restart. pypg recomputes the
        deadline at the start of every query/pump, so the Perl timer-restart
        behaviour is already the default; this is a no-op kept for parity.
        """

    def query_until(self, until, query=""):
        """Send query and pump stdout until the until regex appears; return it."""
        if query:
            self._send(query)
        self._pump_until(want_out=until)
        with self._lock:
            ret = self._stdout
            self._stdout = ""
        return ret

    def wait_for_stderr(self, until, query=""):
        """Send query and pump stderr until the until regex appears."""
        if query:
            self._send(query)
        self._pump_until(want_err=until)
        with self._lock:
            self._stderr = ""

    def clear(self):
        """Discard any accumulated stdout/stderr."""
        with self._lock:
            self._stdout = ""
            self._stderr = ""

    def quit(self):
        """Close the session, returning the psql exit code."""
        if self._proc is None:
            return None
        try:
            if self._proc.stdin and not self._proc.stdin.closed:
                self._send("\\q\n")
                self._proc.stdin.close()
        except (BrokenPipeError, OSError):
            pass
        try:
            self._proc.wait(timeout=self._timeout)
        except subprocess.TimeoutExpired:
            self._proc.kill()
        for thread in self._threads:
            thread.join(timeout=1)
        return self._proc.returncode

    finish = quit

    def __enter__(self) -> "BackgroundPsql":
        return self

    def __exit__(self, *exc) -> None:
        self.quit()

    def restart(self):
        """Quit (if needed) and start a fresh psql session with the same params."""
        self.quit()
        self._start()
        self.wait_connect()

    def reconnect_and_clear(self):
        """Restart the session and discard buffered output.

        Mirrors PostgreSQL::Test::BackgroundPsql->reconnect_and_clear: used after
        a recovery conflict terminates the backend, to get a fresh connection.
        """
        self.restart()
        self.clear()
