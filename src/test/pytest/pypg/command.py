# Copyright (c) 2025, PostgreSQL Global Development Group

"""
Helpers for running PostgreSQL programs and asserting on their results.

These mirror the command_* and program_* helpers from PostgreSQL::Test::Utils,
so Perl TAP tests can be ported with equivalent assertions. Binaries are
resolved against a bindir (prepended to PATH) so tests work whether or not the
install directory is already on PATH.
"""

import os
import re
import subprocess
from collections import namedtuple
from typing import Dict, List, Optional, Sequence

from .util import run_captured

CommandResult = namedtuple("CommandResult", ["rc", "stdout", "stderr"])

# Programs are expected to keep --help output lines within this width. Matches
# PostgreSQL::Test::Utils::program_help_ok.
_MAX_HELP_LINE_LENGTH = 95


def _argv(cmd: Sequence) -> List:
    """Build an argv list. If any element is bytes (e.g. a non-UTF8 database
    name), encode the rest to bytes too so the argv is homogeneous."""
    raw = list(cmd)
    if any(isinstance(c, (bytes, bytearray)) for c in raw):
        return [
            bytes(c) if isinstance(c, (bytes, bytearray)) else os.fsencode(str(c))
            for c in raw
        ]
    return [str(c) for c in raw]


def _describe(cmd: Sequence, result: CommandResult) -> str:
    argv = " ".join(
        c.decode("utf-8", "replace") if isinstance(c, (bytes, bytearray)) else str(c)
        for c in _argv(cmd)
    )
    return (
        f"command: {argv}\n"
        f"exit code: {result.rc}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )


def _assert_msg(msg: Optional[str], what: str, cmd: Sequence, result: CommandResult):
    prefix = f"{msg}: " if msg else ""
    return prefix + what + "\n" + _describe(cmd, result)


class PgBin:
    """
    Runs PostgreSQL client programs and asserts on exit code and output.

    Args:
        bindir: PostgreSQL bin directory; prepended to PATH for each run.
        extra_env: Extra environment variables (e.g. PGHOST/PGPORT for a node).
    """

    def __init__(self, bindir, extra_env: Optional[Dict[str, str]] = None):
        self._bindir = bindir
        self._extra_env = dict(extra_env) if extra_env else {}

    def _env(self, extra_env: Optional[Dict[str, str]]) -> Dict[str, str]:
        env = dict(os.environ)
        env["PATH"] = str(self._bindir) + os.pathsep + env.get("PATH", "")
        env.update(self._extra_env)
        if extra_env:
            env.update(extra_env)
        return env

    def result(self, cmd: Sequence, *, extra_env=None) -> CommandResult:
        """Run cmd, capturing output. Never raises on a nonzero exit.

        Output is captured through temporary files rather than subprocess pipes
        (see util.run_captured): a program that starts a server -- e.g.
        pg_basebackup or pg_ctl start -- leaves a postmaster holding the pipe's
        write end open, which would deadlock a pipe read to EOF.

        Co-authored-by: Andrew Dunstan <andrew@dunslane.net>
        """
        returncode, stdout, stderr = run_captured(_argv(cmd), env=self._env(extra_env))
        return CommandResult(returncode, stdout, stderr)

    def popen(self, cmd: Sequence, *, extra_env=None) -> subprocess.Popen:
        """Start cmd as a long-lived background process (PATH set to bindir).

        The caller is responsible for terminating/waiting on it (e.g. via
        send_signal + wait). stdout/stderr are discarded.
        """
        return subprocess.Popen(  # pylint: disable=consider-using-with
            _argv(cmd),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=self._env(extra_env),
        )

    def run_redirect_stderr(self, cmd, stderr_path) -> int:
        """Run cmd, appending its stderr to stderr_path; return the exit code.

        Used by pg_rewind's growing-file test, where stderr is redirected into
        the very file being copied so that file grows during the copy.
        """
        with open(stderr_path, "a", encoding="utf-8") as fh:
            return subprocess.run(
                _argv(cmd),
                stdout=subprocess.DEVNULL,
                stderr=fh,
                env=self._env(None),
                check=False,
            ).returncode

    def run_command(self, cmd: Sequence, *, extra_env=None) -> CommandResult:
        """Run cmd capturing chomped output, mirroring Utils::run_command.

        Both stdout and stderr have a single trailing newline removed, like the
        Perl helper, so equality checks against the captured strings match.
        """
        result = self.result(cmd, extra_env=extra_env)
        return CommandResult(
            result.rc,
            result.stdout.removesuffix("\n"),
            result.stderr.removesuffix("\n"),
        )

    def command_ok(self, cmd, msg=None, *, extra_env=None) -> CommandResult:
        """Assert the command exits with code 0."""
        result = self.result(cmd, extra_env=extra_env)
        assert result.rc == 0, _assert_msg(msg, "expected success", cmd, result)
        return result

    def command_fails(self, cmd, msg=None, *, extra_env=None) -> CommandResult:
        """Assert the command exits with a nonzero code."""
        result = self.result(cmd, extra_env=extra_env)
        assert result.rc != 0, _assert_msg(msg, "expected failure", cmd, result)
        return result

    def command_exit_is(self, cmd, code, msg=None, *, extra_env=None) -> CommandResult:
        """Assert the command exits with the given code."""
        result = self.result(cmd, extra_env=extra_env)
        assert result.rc == code, _assert_msg(msg, f"expected exit {code}", cmd, result)
        return result

    def command_like(self, cmd, pattern, msg=None, *, extra_env=None) -> CommandResult:
        """Assert success and that stdout matches pattern."""
        result = self.result(cmd, extra_env=extra_env)
        assert result.rc == 0, _assert_msg(msg, "expected success", cmd, result)
        assert re.search(pattern, result.stdout), _assert_msg(
            msg, f"stdout did not match {pattern!r}", cmd, result
        )
        return result

    def command_fails_like(self, cmd, pattern, msg=None, *, extra_env=None):
        """Assert failure and that stderr matches pattern."""
        result = self.result(cmd, extra_env=extra_env)
        assert result.rc != 0, _assert_msg(msg, "expected failure", cmd, result)
        assert re.search(pattern, result.stderr), _assert_msg(
            msg, f"stderr did not match {pattern!r}", cmd, result
        )
        return result

    def command_ok_or_fails_like(
        self, cmd, expected_stdout, expected_stderr, msg=None, *, extra_env=None
    ):
        """Run cmd; if it fails, assert its stdout/stderr match the patterns.

        Mirrors PostgreSQL::Test::Utils::command_ok_or_fails_like: a successful
        run is accepted with no output checks (returns True); a failed run must
        have stdout matching expected_stdout and stderr matching
        expected_stderr (returns False). Used where a command may legitimately
        be unsupported on the platform (e.g. pg_upgrade --clone).
        """
        result = self.result(cmd, extra_env=extra_env)
        if result.rc != 0:
            assert re.search(expected_stdout, result.stdout), _assert_msg(
                msg, f"stdout did not match {expected_stdout!r}", cmd, result
            )
            assert re.search(expected_stderr, result.stderr), _assert_msg(
                msg, f"stderr did not match {expected_stderr!r}", cmd, result
            )
            return False
        return True

    def command_checks_all(self, cmd, exit_code, stdout_res, stderr_res, msg=None):
        """Assert the exit code and that every stdout/stderr regex matches."""
        result = self.result(cmd)
        assert result.rc == exit_code, _assert_msg(
            msg, f"expected exit {exit_code}", cmd, result
        )
        for pattern in stdout_res:
            assert re.search(pattern, result.stdout), _assert_msg(
                msg, f"stdout did not match {pattern!r}", cmd, result
            )
        for pattern in stderr_res:
            assert re.search(pattern, result.stderr), _assert_msg(
                msg, f"stderr did not match {pattern!r}", cmd, result
            )
        return result

    def program_help_ok(self, name):
        """--help exits 0, writes stdout, nothing to stderr, lines <= 95 chars."""
        cmd = [name, "--help"]
        result = self.result(cmd)
        assert result.rc == 0, _describe(cmd, result)
        assert result.stdout != "", f"{name} --help produced no stdout"
        assert result.stderr == "", f"{name} --help wrote to stderr:\n{result.stderr}"
        long_lines = [
            ln for ln in result.stdout.splitlines() if len(ln) > _MAX_HELP_LINE_LENGTH
        ]
        assert not long_lines, "help lines exceed length limit:\n" + "\n".join(
            long_lines
        )
        return result

    def program_version_ok(self, name):
        """--version exits 0, writes stdout, nothing to stderr."""
        cmd = [name, "--version"]
        result = self.result(cmd)
        assert result.rc == 0, _describe(cmd, result)
        assert result.stdout != "", f"{name} --version produced no stdout"
        assert result.stderr == "", f"{name} --version wrote stderr:\n{result.stderr}"
        return result

    def program_options_handling_ok(self, name):
        """An invalid option gives a nonzero exit and an error message."""
        cmd = [name, "--not-a-valid-option"]
        result = self.result(cmd)
        assert result.rc != 0, f"{name} accepted an invalid option"
        assert result.stderr != "", f"{name} printed no error for an invalid option"
        return result

    def check_pg_config(self, regexp):
        """Return True if a line in the installed pg_config.h matches regexp.

        Mirrors PostgreSQL::Test::Utils::check_pg_config (the pattern is
        anchored at the start of the line).
        """
        includedir = self.result(["pg_config", "--includedir"]).stdout.strip()
        header = os.path.join(includedir, "pg_config.h")
        with open(header, encoding="utf-8", errors="replace") as f:
            return any(re.match(regexp, line) for line in f)
