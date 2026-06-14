# Copyright (c) 2020-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/bin/pg_verifybackup/t/005_bad_manifest.pl.

pg_verifybackup rejects malformed backup manifests with a specific diagnostic
for each kind of error: JSON parse errors, missing/invalid required fields in
Files and WAL-Ranges entries, duplicate paths, unrecognized/invalid checksum
algorithms, and a bad manifest checksum. Each manifest is written to a temp dir
and pg_verifybackup is expected to fail with the matching message.
"""

import re
import tempfile


# (kind, description, manifest_contents). kind: 'parse' -> "could not parse
# backup manifest: <desc>"; 'fatal' -> "error: <desc>"; 'raw' -> desc is the
# full regex.
_CASES = [
    (
        "raw",
        r"could not parse backup manifest: The input string ended unexpectedly",
        "{\n",
    ),
    ("parse", "unexpected object end", "{}\n"),
    ("parse", "unexpected array start", "[]\n"),
    ("parse", "expected version indicator", '{"not-expected": 1}\n'),
    (
        "parse",
        "manifest version not an integer",
        '{"PostgreSQL-Backup-Manifest-Version": "phooey"}\n',
    ),
    (
        "parse",
        "unexpected manifest version",
        '{"PostgreSQL-Backup-Manifest-Version": 9876599}\n',
    ),
    (
        "parse",
        "unexpected scalar",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Files": true}\n',
    ),
    (
        "parse",
        "unrecognized top-level field",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Oops": 1}\n',
    ),
    (
        "parse",
        "unexpected object start",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Files": {}}\n',
    ),
    (
        "parse",
        "missing path name",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Files": [{}]}\n',
    ),
    (
        "parse",
        "both path name and encoded path name",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Files": [\n    {"Path": "x", "Encoded-Path": "1234"}\n]}\n',
    ),
    (
        "parse",
        "unexpected file field",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Files": [\n    {"Oops": 1}\n]}\n',
    ),
    (
        "parse",
        "missing size",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Files": [\n    {"Path": "x"}\n]}\n',
    ),
    (
        "parse",
        "file size is not an integer",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Files": [\n    {"Path": "x", "Size": "Oops"}\n]}\n',
    ),
    (
        "parse",
        "could not decode file name",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Files": [\n    {"Encoded-Path": "123", "Size": 0}\n]}\n',
    ),
    (
        "fatal",
        "duplicate path name in backup manifest",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Files": [\n    {"Path": "x", "Size": 0},\n    {"Path": "x", "Size": 0}\n]}\n',
    ),
    (
        "parse",
        "checksum without algorithm",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Files": [\n    {"Path": "x", "Size": 100, "Checksum": "Oops"}\n]}\n',
    ),
    (
        "fatal",
        "unrecognized checksum algorithm",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Files": [\n    {"Path": "x", "Size": 100, "Checksum-Algorithm": "Oops", "Checksum": "00"}\n]}\n',
    ),
    (
        "fatal",
        "invalid checksum for file",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Files": [\n    {"Path": "x", "Size": 100, "Checksum-Algorithm": "CRC32C", "Checksum": "0"}\n]}\n',
    ),
    (
        "parse",
        "missing start LSN",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "WAL-Ranges": [\n    {"Timeline": 1}\n]}\n',
    ),
    (
        "parse",
        "missing end LSN",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "WAL-Ranges": [\n    {"Timeline": 1, "Start-LSN": "0/0"}\n]}\n',
    ),
    (
        "parse",
        "unexpected WAL range field",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "WAL-Ranges": [\n    {"Oops": 1}\n]}\n',
    ),
    (
        "parse",
        "missing timeline",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "WAL-Ranges": [\n    {}\n]}\n',
    ),
    (
        "parse",
        "unexpected object end",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "WAL-Ranges": [\n    {"Timeline": 1, "Start-LSN": "0/0", "End-LSN": "0/0"}\n]}\n',
    ),
    (
        "parse",
        "timeline is not an integer",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "WAL-Ranges": [\n    {"Timeline": true, "Start-LSN": "0/0", "End-LSN": "0/0"}\n]}\n',
    ),
    (
        "parse",
        "could not parse start LSN",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "WAL-Ranges": [\n    {"Timeline": 1, "Start-LSN": "oops", "End-LSN": "0/0"}\n]}\n',
    ),
    (
        "parse",
        "could not parse end LSN",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "WAL-Ranges": [\n    {"Timeline": 1, "Start-LSN": "0/0", "End-LSN": "oops"}\n]}\n',
    ),
    (
        "parse",
        "expected at least 2 lines",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Files": [], "Manifest-Checksum": null}\n',
    ),
    (
        "parse",
        "last line not newline-terminated",
        '{"PostgreSQL-Backup-Manifest-Version": 1,\n "Files": [],\n "Manifest-Checksum": null}',
    ),
    (
        "fatal",
        "invalid manifest checksum",
        '{"PostgreSQL-Backup-Manifest-Version": 1, "Files": [],\n "Manifest-Checksum": "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01234567890-"}\n',
    ),
]


def test_005_bad_manifest(create_pg):
    """pg_verifybackup reports the right diagnostic for each malformed manifest."""
    primary = create_pg("primary")
    tempdir = tempfile.mkdtemp(prefix="badmf_")
    for kind, desc, contents in _CASES:
        with open("{}/backup_manifest".format(tempdir), "w", encoding="utf-8") as fh:
            fh.write(contents)
        if kind == "parse":
            pattern = r"could not parse backup manifest: " + re.escape(desc)
        elif kind == "fatal":
            pattern = r"error: " + re.escape(desc)
        else:
            pattern = desc
        primary.command_fails_like(["pg_verifybackup", tempdir], pattern, desc)
