# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_resetwal/t/002_corrupted.pl.

Tests handling of a corrupted pg_control.
"""


def test_corrupted(pg_bin, create_pg):
    """pg_resetwal copes with an all-zero and a partially-zeroed pg_control."""
    node = create_pg("main", start=False)

    pg_control = node.datadir / "global" / "pg_control"
    size = pg_control.stat().st_size

    # Read the head of the file to get PG_CONTROL_VERSION in particular.
    data = pg_control.read_bytes()[:16]

    # Fill pg_control with zeros.
    pg_control.write_bytes(b"\x00" * size)

    pg_bin.command_checks_all(
        ["pg_resetwal", "--dry-run", node.datadir],
        0,
        [r"pg_control version number"],
        [
            r"pg_resetwal: warning: pg_control exists but is broken or wrong "
            r"version; ignoring it"
        ],
        "processes corrupted pg_control all zeroes",
    )

    # Put back the saved header. This uses a different code path internally,
    # allowing a zero WAL segment size to be processed.
    pg_control.write_bytes(data + b"\x00" * (size - 16))

    pg_bin.command_checks_all(
        ["pg_resetwal", "--dry-run", node.datadir],
        0,
        [r"pg_control version number"],
        [
            r"pg_resetwal: warning: pg_control specifies invalid WAL segment "
            r"size \(0 bytes\); proceed with caution"
        ],
        "processes zero WAL segment size",
    )

    # Now try to run it for real.
    pg_bin.command_fails_like(
        ["pg_resetwal", node.datadir],
        r"not proceeding because control file values were guessed",
        "does not run when control file values were guessed",
    )
    pg_bin.command_ok(
        ["pg_resetwal", "--force", node.datadir],
        "runs with force when control file values were guessed",
    )
