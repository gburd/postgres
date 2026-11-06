# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_controldata/t/001_pg_controldata.pl."""


def test_pg_controldata(pg_bin, create_pg):
    """pg_controldata output, argument handling, and corrupted pg_control."""
    pg_bin.program_help_ok("pg_controldata")
    pg_bin.program_version_ok("pg_controldata")
    pg_bin.program_options_handling_ok("pg_controldata")
    pg_bin.command_fails(["pg_controldata"], "pg_controldata without arguments fails")
    pg_bin.command_fails(
        ["pg_controldata", "nonexistent"],
        "pg_controldata with nonexistent directory fails",
    )

    node = create_pg("main", start=False)

    pg_bin.command_like(
        ["pg_controldata", node.datadir],
        r"checkpoint",
        "pg_controldata produces output",
    )

    # Corrupt pg_control by overwriting most of it with zeros. The first 16
    # bytes (pg_control version number) are left intact so we get a checksum
    # mismatch rather than a version-number error.
    pg_control = node.datadir / "global" / "pg_control"
    size = pg_control.stat().st_size
    with open(pg_control, "r+b") as fh:
        fh.seek(16)
        fh.write(b"\x00" * (size - 16))

    pg_bin.command_checks_all(
        ["pg_controldata", node.datadir],
        0,
        [r"."],
        [
            r"warning: calculated CRC checksum does not match value stored in "
            r"control file",
            r"warning: invalid WAL segment size",
        ],
        "pg_controldata with corrupted pg_control",
    )
