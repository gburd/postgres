# Copyright (c) 2025, PostgreSQL Global Development Group

import os
import re
import shlex
import socket
import stat
import subprocess
import sys
import tempfile


def run_captured(argv, *, env=None, combine_stderr=False, timeout=None):
    """Run argv, capturing output through temporary files instead of pipes.

    Returns ``(returncode, stdout, stderr)`` as text. With combine_stderr,
    stderr is folded into stdout and the returned stderr is "".

    Output is captured to temporary files rather than subprocess.PIPE because of
    how starting a server behaves: ``pg_ctl start`` launches a postmaster that
    inherits and holds open the write end of the parent's stdout/stderr pipe for
    its whole lifetime. Reading such a pipe to EOF -- as subprocess does to
    collect output -- then blocks until the postmaster exits, i.e. forever
    (notably on Windows, and under constrained CI process models). A regular
    file handle has no EOF dependency on the writer staying alive, so the parent
    reads the captured output as soon as the launched program returns.

    Co-authored-by: Andrew Dunstan <andrew@dunslane.net>
    """
    out = tempfile.TemporaryFile()  # pylint: disable=consider-using-with
    err = subprocess.STDOUT if combine_stderr else tempfile.TemporaryFile()
    try:
        proc = subprocess.run(
            argv, env=env, stdout=out, stderr=err, timeout=timeout, check=False
        )
        out.seek(0)
        stdout = _decode(out.read())
        if combine_stderr:
            stderr = ""
        else:
            err.seek(0)
            stderr = _decode(err.read())
    finally:
        out.close()
        if err is not subprocess.STDOUT:
            err.close()
    return proc.returncode, stdout, stderr


def _decode(data):
    """Decode captured output as text, folding CRLF/CR to LF.

    Programs may emit non-UTF-8 bytes (e.g. LATIN1 object names) that we only
    regex-match, so decode leniently. Reading a file gives no universal-newline
    handling, so normalize line endings to match text-mode capture.
    """
    text = data.decode("utf-8", "replace")
    return text.replace("\r\n", "\n").replace("\r", "\n")


def eprint(*args, **kwargs):
    """eprint prints to stderr"""
    print(*args, file=sys.stderr, **kwargs)


def run(*command, check=True, shell=None, silent=False, **kwargs):
    """run runs the given command and prints it to stderr"""

    __tracebackhide__ = True  # pylint: disable=unused-variable

    if shell is None:
        shell = len(command) == 1 and isinstance(command[0], str)

    if shell:
        command = command[0]
    else:
        command = list(map(str, command))

    if not silent:
        if shell:
            eprint(f"+ {command}")
        else:
            # We could normally use shlex.join here, but it's not available in
            # Python 3.6 which we still like to support
            unsafe_string_cmd = " ".join(map(shlex.quote, command))
            eprint(f"+ {unsafe_string_cmd}")

    if silent:
        kwargs.setdefault("stdout", subprocess.DEVNULL)

    result = subprocess.run(command, check=False, shell=shell, **kwargs)

    # Manually throw CalledProcessError to avoid subprocess.run's huge body
    # poluting stack traces.
    if check and result.returncode:
        raise subprocess.CalledProcessError(
            result.returncode, command, result.stdout, result.stderr
        )

    return result


def capture(command, *args, stdout=subprocess.PIPE, encoding="utf-8", **kwargs):
    __tracebackhide__ = True  # pylint: disable=unused-variable

    return run(
        command, *args, stdout=stdout, encoding=encoding, **kwargs
    ).stdout.removesuffix("\n")


def slurp_file(path, offset=0):
    """Read and return a file's contents, optionally starting at a byte offset.

    Mirrors PostgreSQL::Test::Utils::slurp_file.
    """
    with open(path, encoding="utf-8", errors="replace") as f:
        if offset:
            f.seek(offset)
        return f.read()


def append_to_file(path, data):
    """Append data to a file, creating it if necessary."""
    with open(path, "a", encoding="utf-8") as f:
        f.write(data)


def slurp_dir(path):
    """Return the entries of a directory (cf. PostgreSQL::Test::Utils::slurp_dir)."""
    return os.listdir(path)


def check_pg_config(regexp):
    """Return True if a line of the installed pg_config.h matches regexp at start.

    Mirrors PostgreSQL::Test::Utils::check_pg_config: runs `pg_config
    --includedir` (pg_config resolved from PG_CONFIG or PATH) and greps
    pg_config.h for ``^regexp``.
    """
    pg_config = os.environ.get("PG_CONFIG", "pg_config")
    proc = subprocess.run(
        [pg_config, "--includedir"],
        stdout=subprocess.PIPE,
        encoding="utf-8",
        check=True,
    )
    includedir = proc.stdout.strip()
    with open(os.path.join(includedir, "pg_config.h"), encoding="utf-8") as fh:
        return any(re.match(regexp, line) for line in fh)


def scan_server_header(header_path, regexp):
    """Return the regexp capture groups from the first matching server-header line.

    Mirrors PostgreSQL::Test::Utils::scan_server_header: runs `pg_config
    --includedir-server` and greps header_path (relative) for ``^regexp``,
    returning the captured groups of the first match. Raises if no line matches.
    """
    pg_config = os.environ.get("PG_CONFIG", "pg_config")
    proc = subprocess.run(
        [pg_config, "--includedir-server"],
        stdout=subprocess.PIPE,
        encoding="utf-8",
        check=True,
    )
    includedir = proc.stdout.strip()
    with open(os.path.join(includedir, header_path), encoding="utf-8") as fh:
        for line in fh:
            match = re.match(regexp, line)
            if match:
                return match.groups()
    raise RuntimeError("could not find match in header {}".format(header_path))


def get_free_port():
    """Reserve and return a likely-free localhost TCP port.

    Like PostgreSQL::Test::Cluster::get_free_port, this is best-effort: the
    port is released before it is returned, so a caller must bind it promptly.
    """
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def check_mode_recursive(directory, dir_mode, file_mode):
    """Check permissions recursively under directory.

    Returns True if every directory has mode dir_mode and every regular file
    has mode file_mode (comparing the low 12 permission bits). Mirrors
    PostgreSQL::Test::Utils::check_mode_recursive. Files that vanish mid-walk
    (a running server may delete them) are tolerated.
    """
    result = True

    for dirpath, _dirs, files in os.walk(directory):
        try:
            if stat.S_IMODE(os.stat(dirpath).st_mode) != dir_mode:
                eprint("{} mode must be {:04o}".format(dirpath, dir_mode))
                result = False
        except FileNotFoundError:
            pass

        for fname in files:
            fpath = os.path.join(dirpath, fname)
            try:
                st = os.stat(fpath)
            except FileNotFoundError:
                continue
            if stat.S_ISREG(st.st_mode) and stat.S_IMODE(st.st_mode) != file_mode:
                eprint("{} mode must be {:04o}".format(fpath, file_mode))
                result = False

    return result


def chmod_recursive(directory, dir_mode, file_mode):
    """chmod every directory (dir_mode) and regular file (file_mode) under
    directory. Mirrors PostgreSQL::Test::Utils::chmod_recursive.
    """
    os.chmod(directory, dir_mode)
    for dirpath, dirs, files in os.walk(directory):
        for name in dirs:
            path = os.path.join(dirpath, name)
            if not os.path.islink(path):
                os.chmod(path, dir_mode)
        for name in files:
            path = os.path.join(dirpath, name)
            if not os.path.islink(path):
                try:
                    os.chmod(path, file_mode)
                except FileNotFoundError:
                    pass


def compare_files(file_a, file_b, msg, line_filter=None):
    """Assert two files are line-by-line equal, with an optional line filter.

    Mirrors PostgreSQL::Test::Utils::compare_files: if line_filter is given it
    is applied to each (line_a, line_b) pair before comparison (e.g. to mask
    environment-specific text); pairs the filter accepts as equal are skipped.
    """
    with open(file_a, encoding="utf-8") as fa, open(file_b, encoding="utf-8") as fb:
        lines_a = fa.readlines()
        lines_b = fb.readlines()
    assert len(lines_a) == len(lines_b), "{}: differing line counts".format(msg)
    for line_a, line_b in zip(lines_a, lines_b):
        if line_filter is not None:
            line_a, line_b = line_filter(line_a, line_b)
        assert line_a == line_b, "{}: line differs: {!r} != {!r}".format(
            msg, line_a, line_b
        )


def wait_for_file(filename, regexp, offset=0):
    """Wait until filename exists and its contents (from offset) match regexp.

    Mirrors PostgreSQL::Test::Utils::wait_for_file: polls up to the default
    timeout, returning the new end offset (offset + matched length) on success
    and raising on timeout.
    """
    import time as _time  # pylint: disable=import-outside-toplevel
    from ._env import test_timeout_default  # pylint: disable=import-outside-toplevel

    max_attempts = 10 * test_timeout_default()
    for _ in range(max_attempts):
        if os.path.exists(filename):
            contents = slurp_file(filename, offset)
            if re.search(regexp, contents):
                return offset + len(contents)
        _time.sleep(0.1)
    raise TimeoutError(
        "timed out waiting for file {} contents to match: {}".format(filename, regexp)
    )


def wait_until(error_message="condition not met", timeout=None, interval=0.1):
    """Poll for a condition, yielding once per attempt until it holds.

    A general-purpose alternative to poll_query_until for conditions that are
    not a single query comparing equal to a fixed string -- for example,
    waiting for a crashed server to accept connections again while swallowing
    the connection errors::

        for _ in wait_until("server did not come back after crash", timeout=180):
            try:
                node.sql("SELECT 1")
                break
            except PgSqlError:
                pass

    The loop runs until ``break`` (success) or *timeout* seconds elapse, at
    which point a TimeoutError carrying *error_message* is raised. *timeout*
    defaults to PG_TEST_TIMEOUT_DEFAULT. Progress is logged every 5s for long
    waits.
    """
    import time as _time  # pylint: disable=import-outside-toplevel
    from ._env import test_timeout_default  # pylint: disable=import-outside-toplevel

    if timeout is None:
        timeout = test_timeout_default()
    start = _time.monotonic()
    end = start + timeout
    last_progress = start
    while _time.monotonic() < end:
        now = _time.monotonic()
        if timeout > 5 and now - last_progress > 5:
            last_progress = now
            print("{} after {:.0f}s - will retry".format(error_message, now - start))
        yield
        _time.sleep(interval)
    raise TimeoutError(error_message + " in time")
