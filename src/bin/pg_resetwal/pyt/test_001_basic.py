# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_resetwal/t/001_basic.pl."""

import os
import platform
import re

import pypg

windows_os = platform.system() == "Windows"

# (args, stderr_pattern, test_name) for option-validation failures.
_OPTION_ERRORS = [
    (["-c", "foo"], r"error: invalid argument for option -c", "incorrect -c option"),
    (["-c", "10,bar"], r"error: invalid argument for option -c", "incorrect -c part 2"),
    (["-c", "1,10"], r"greater than", "-c ids value 1 part 1"),
    (["-c", "10,1"], r"greater than", "-c value 1 part 2"),
    (["-e", "foo"], r"error: invalid argument for option -e", "incorrect -e option"),
    (["-e", "-1"], r"error: invalid argument for option -e", "-e value -1"),
    (["-l", "foo"], r"error: invalid argument for option -l", "incorrect -l option"),
    (["-m", "foo"], r"error: invalid argument for option -m", "incorrect -m option"),
    (["-m", "10,bar"], r"error: invalid argument for option -m", "incorrect -m part 2"),
    (["-m", "0,10"], r"must not be 0", "-m value 0 in the first part"),
    (["-m", "10,0"], r"must not be 0", "-m value 0 in the second part"),
    (["-o", "foo"], r"error: invalid argument for option -o", "incorrect -o option"),
    (["-o", "0"], r"must not be 0", "-o value 0"),
    (["-O", "foo"], r"error: invalid argument for option -O", "incorrect -O option"),
    (["-O", "-1"], r"error: invalid argument for option -O", "-O value -1"),
    (["--wal-segsize", "foo"], r"error: invalid value", "incorrect --wal-segsize"),
    (["--wal-segsize", "13"], r"must be a power", "invalid --wal-segsize value"),
    (["-u", "foo"], r"error: invalid argument for option -u", "incorrect -u option"),
    (["-u", "1"], r"must be greater than", "-u value too small"),
    (["-x", "foo"], r"error: invalid argument for option -x", "incorrect -x option"),
    (["-x", "1"], r"must be greater than", "-x value too small"),
    (["-x", "-1"], r"error: invalid argument for option -x", "-x value -1"),
    (["-x", "-100"], r"error: invalid argument for option -x", "negative -x value"),
    (["-x", "10000000000"], r"error: invalid argument for option -x", "-x too large"),
    (
        ["--char-signedness", "foo"],
        r"error: invalid argument for option --char-signedness",
        "incorrect --char-signedness option",
    ),
]


def test_pg_resetwal(pg_bin, create_pg):
    """pg_resetwal dry run, permissions, running, and option handling."""
    pg_bin.program_help_ok("pg_resetwal")
    pg_bin.program_version_ok("pg_resetwal")
    pg_bin.program_options_handling_ok("pg_resetwal")

    node = create_pg("main", start=False)
    node.append_conf("track_commit_timestamp = on")

    pg_bin.command_like(
        ["pg_resetwal", "-n", node.datadir],
        r"checkpoint",
        "pg_resetwal -n produces output",
    )

    if not windows_os:
        assert pypg.check_mode_recursive(
            node.datadir, 0o700, 0o600
        ), "check PGDATA permissions"

    pg_bin.command_ok(["pg_resetwal", "--pgdata", node.datadir], "pg_resetwal runs")
    node.start()
    assert node.safe_psql("SELECT 1;") == "1", "server running and working after reset"

    pg_bin.command_fails_like(
        ["pg_resetwal", node.datadir],
        r"lock file .* exists",
        "fails if server running",
    )

    node.stop("immediate")
    pg_bin.command_fails_like(
        ["pg_resetwal", node.datadir],
        r"database server was not shut down cleanly",
        "does not run after immediate shutdown",
    )
    pg_bin.command_ok(
        ["pg_resetwal", "--force", node.datadir],
        "runs after immediate shutdown with force",
    )
    node.start()
    assert (
        node.safe_psql("SELECT 1;") == "1"
    ), "server running and working after forced reset"
    node.stop()

    _test_option_errors(pg_bin, node)
    _test_control_overrides(pg_bin, node)

    node.start()


def _test_option_errors(pg_bin, node):
    pg_bin.command_fails_like(
        ["pg_resetwal", "foo"],
        r"error: could not read permissions of directory",
        "fails with nonexistent data directory",
    )
    pg_bin.command_fails_like(
        ["pg_resetwal", "foo", "bar"],
        r"too many command-line arguments",
        "fails with too many command-line arguments",
    )
    pg_bin.command_fails_like(
        ["pg_resetwal"],
        r"no data directory specified",
        "fails with too few command-line arguments",
        extra_env={"PGDATA": str(node.datadir)},  # not used
    )

    for args, pattern, name in _OPTION_ERRORS:
        pg_bin.command_fails_like(
            ["pg_resetwal", *args, node.datadir],
            pattern,
            "fails with {}".format(name),
        )


def _slru_files(node, subdir):
    entries = os.listdir(node.datadir / subdir)
    return sorted(f for f in entries if re.search(r"[0-9A-F]+", f))


def _test_control_overrides(pg_bin, node):
    out = pg_bin.result(["pg_resetwal", "--dry-run", node.datadir]).stdout
    match = re.search(r"^Database block size: *(\d+)$", out, re.M)
    assert match
    blcksz = int(match.group(1))

    cmd = [
        "pg_resetwal",
        "--pgdata",
        node.datadir,
        "--epoch",
        "1",
        "--next-wal-file",
        "00000001000000320000004B",
        "--next-oid",
        "100000",
        "--wal-segsize",
        "1",
    ]

    files = _slru_files(node, "pg_commit_ts")
    cmd += [
        "--commit-timestamp-ids",
        "{},{}".format(
            3 if int(files[0], 16) == 0 else int(files[0], 16), int(files[-1], 16)
        ),
    ]

    files = _slru_files(node, "pg_multixact/offsets")
    mult = 32 * blcksz // 8
    cmd += [
        "--multixact-ids",
        # The Perl original writes hex($files[0] * $mult) for the "old" value
        # (numify the hex string in decimal, then hex()), which is an apparent
        # quirk; we deliberately parse-as-hex then multiply (consistent with the
        # pg_xact case). The two differ only when files[0] != "0000", which does
        # not occur on a freshly-initialized cluster, so the value matches there.
        "{},{}".format(
            (int(files[-1], 16) + 1) * mult,
            1 if int(files[0], 16) == 0 else int(files[0], 16) * mult,
        ),
    ]

    files = _slru_files(node, "pg_multixact/members")
    mult = 32 * int(blcksz / 20) * 4
    cmd += ["--multixact-offset", str((int(files[-1], 16) + 1) * mult)]

    files = _slru_files(node, "pg_xact")
    mult = 32 * blcksz * 4
    cmd += [
        "--oldest-transaction-id",
        str(3 if int(files[0], 16) == 0 else int(files[0], 16) * mult),
        "--next-transaction-id",
        str((int(files[-1], 16) + 1) * mult),
    ]

    pg_bin.command_ok(
        [*cmd, "--dry-run"], "runs with control override options, dry run"
    )
    pg_bin.command_ok(cmd, "runs with control override options")
    pg_bin.command_like(
        ["pg_resetwal", "--dry-run", node.datadir],
        r"(?m)^Latest checkpoint's NextOID: *100000$",
        "spot check that control changes were applied",
    )
