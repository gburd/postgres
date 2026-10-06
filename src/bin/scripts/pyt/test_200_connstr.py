# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/scripts/t/200_connstr.pl.

Checks connection-string / database-name handling in the client utilities by
creating databases whose names span the LATIN1 byte range (including bytes that
are not valid UTF-8) and running the --all options over them.
"""


def _generate_ascii_bytes(from_char, to_char):
    """Return the bytes from_char..to_char inclusive (cf. generate_ascii_string)."""
    return bytes(range(from_char, to_char + 1))


def test_connstr(create_pg, monkeypatch):
    """vacuumdb/reindexdb/clusterdb --all cope with unusual database names."""
    # Use byte sequences that aren't valid UTF-8. LATIN1 accepts any byte.
    monkeypatch.setenv("LC_ALL", "C")
    monkeypatch.setenv("PGCLIENTENCODING", "LATIN1")

    # Database names covering the range of LATIN1 characters.
    dbname1 = _generate_ascii_bytes(1, 63)  # contains '='
    dbname2 = _generate_ascii_bytes(67, 129)  # skip 64-66 to keep length to 62
    dbname3 = _generate_ascii_bytes(130, 192)
    dbname4 = _generate_ascii_bytes(193, 255)

    node = create_pg("main", extra=["--locale=C", "--encoding=LATIN1"])

    for dbname in (dbname1, dbname2, dbname3, dbname4, b"CamelCase"):
        # Like run_log: run and ignore the result (some names are rejected,
        # e.g. those containing a newline).
        node.bin.result([b"createdb", dbname])

    node.command_ok(
        ["vacuumdb", "--all", "--echo", "--analyze-only"],
        "vacuumdb --all with unusual database names",
    )
    node.command_ok(
        ["reindexdb", "--all", "--echo"],
        "reindexdb --all with unusual database names",
    )
    node.command_ok(
        ["clusterdb", "--all", "--echo", "--verbose"],
        "clusterdb --all with unusual database names",
    )
