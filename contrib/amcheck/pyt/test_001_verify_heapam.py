# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of contrib/amcheck/t/001_verify_heapam.pl.

verify_heapam() on plain heap tables and on sequences (which are heaps
under the hood): an uncorrupted table passes under every option combination,
while a table whose first page's line pointers are corrupted is reliably
detected, including interactions with VACUUM FREEZE and the skip option.
"""

import os
import re
import struct

# Line-pointer corruption messages verify_heapam.c emits for the bytes written
# by _corrupt_first_page (chosen to hit the checks on both endiannesses).
_HEAP_CORRUPTION_RES = [
    r"line pointer redirection to item at offset \d+ precedes minimum offset \d+",
    r"line pointer redirection to item at offset \d+ exceeds maximum offset \d+",
    r"line pointer to page offset \d+ is not maximally aligned",
    r"line pointer length \d+ is less than the minimum tuple header size \d+",
    r"line pointer to page offset \d+ with length \d+ ends beyond maximum "
    r"page offset \d+",
]

_FRESH_TABLE = """\
DROP TABLE IF EXISTS {rel} CASCADE;
CREATE TABLE {rel} (a integer, b text);
ALTER TABLE {rel} SET (autovacuum_enabled=false);
ALTER TABLE {rel} ALTER b SET STORAGE external;
INSERT INTO {rel} (a, b)
    (SELECT gs, repeat('b',gs*10) FROM generate_series(1,1000) gs);
BEGIN;
SAVEPOINT s1;
SELECT 1 FROM {rel} WHERE a = 42 FOR UPDATE;
UPDATE {rel} SET b = b WHERE a = 42;
RELEASE s1;
SAVEPOINT s1;
SELECT 1 FROM {rel} WHERE a = 42 FOR UPDATE;
UPDATE {rel} SET b = b WHERE a = 42;
COMMIT;
"""

_FRESH_SEQUENCE = """\
DROP SEQUENCE IF EXISTS {seq} CASCADE;
CREATE SEQUENCE {seq}
    INCREMENT BY 13
    MINVALUE 17
    START WITH 23;
SELECT nextval('{seq}');
SELECT setval('{seq}', currval('{seq}') + nextval('{seq}'));
"""


def _relation_filepath(node, relname):
    """Return the absolute on-disk path of a relation's main fork."""
    rel = node.safe_psql("SELECT pg_relation_filepath('{}')".format(relname))
    assert rel, "path not found for relation {}".format(relname)
    return os.path.join(node.datadir, rel)


def _fresh_test_table(node, relname):
    """(Re)create and populate a test table of the given name."""
    node.safe_psql(_FRESH_TABLE.format(rel=relname))


def _fresh_test_sequence(node, seqname):
    """Create and exercise a test sequence of the given name."""
    node.safe_psql(_FRESH_SEQUENCE.format(seq=seqname))


def _corrupt_first_page(node, relname):
    """Stop the node, corrupt the first page's line pointers, restart it."""
    relpath = _relation_filepath(node, relname)
    node.stop()
    # Corrupt some line pointers. The values are chosen to hit the various
    # line-pointer-corruption checks in verify_heapam.c on both little-endian
    # and big-endian architectures (Perl pack("L*", ...) is native unsigned
    # 32-bit).
    payload = struct.pack(
        "=6I",
        0xAAA15550,
        0xAAA0D550,
        0x00010000,
        0x00008000,
        0x0000800F,
        0x001E8000,
    )
    with open(relpath, "r+b") as fh:
        fh.seek(32)
        fh.write(payload)
    node.start()


def _detects_corruption(node, function, testname, regexes):
    """Assert verify_heapam(...) output matches all of the given regexes."""
    result = node.safe_psql("SELECT * FROM {}".format(function))
    for pattern in regexes:
        assert re.search(pattern, result), "{}\noutput:\n{}".format(testname, result)


def _detects_heap_corruption(node, function, testname):
    """Assert verify_heapam(...) reports the expected line-pointer messages."""
    _detects_corruption(node, function, testname, _HEAP_CORRUPTION_RES)


def _detects_no_corruption(node, function, testname):
    """Assert verify_heapam(...) reports no corruption (empty output)."""
    result = node.safe_psql("SELECT * FROM {}".format(function))
    assert result == "", testname


def _check_all_options_uncorrupted(node, relname, prefix):
    """Every option combination is stable and reports no corruption.

    relname must be an uncorrupted relation.
    """
    for stop in ("true", "false"):
        for check_toast in ("true", "false"):
            for skip in ("'none'", "'all-frozen'", "'all-visible'"):
                for startblock in ("NULL", "0"):
                    for endblock in ("NULL", "0"):
                        opts = (
                            "on_error_stop := {}, "
                            "check_toast := {}, "
                            "skip := {}, "
                            "startblock := {}, "
                            "endblock := {}".format(
                                stop, check_toast, skip, startblock, endblock
                            )
                        )
                        _detects_no_corruption(
                            node,
                            "verify_heapam('{}', {})".format(relname, opts),
                            "{}: {}".format(prefix, opts),
                        )


def _check_corrupt_table(node):
    """Corrupt a fresh table and confirm detection under several options."""
    _fresh_test_table(node, "test")
    _corrupt_first_page(node, "test")
    _detects_heap_corruption(node, "verify_heapam('test')", "plain corrupted table")
    _detects_heap_corruption(
        node,
        "verify_heapam('test', skip := 'all-visible')",
        "plain corrupted table skipping all-visible",
    )
    _detects_heap_corruption(
        node,
        "verify_heapam('test', skip := 'all-frozen')",
        "plain corrupted table skipping all-frozen",
    )
    _detects_heap_corruption(
        node,
        "verify_heapam('test', check_toast := false)",
        "plain corrupted table skipping toast",
    )
    _detects_heap_corruption(
        node,
        "verify_heapam('test', startblock := 0, endblock := 0)",
        "plain corrupted table checking only block zero",
    )


def _check_all_frozen_table(node):
    """A frozen table is clean; corruption is detected unless skipped."""
    _fresh_test_table(node, "test")
    node.safe_psql("VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) test")
    _detects_no_corruption(
        node, "verify_heapam('test')", "all-frozen not corrupted table"
    )
    _corrupt_first_page(node, "test")
    _detects_heap_corruption(
        node, "verify_heapam('test')", "all-frozen corrupted table"
    )
    _detects_no_corruption(
        node,
        "verify_heapam('test', skip := 'all-frozen')",
        "all-frozen corrupted table skipping all-frozen",
    )


def _check_sequence(node):
    """A sequence (heap under the hood) passes checks across mutations."""
    _fresh_test_sequence(node, "test_seq")
    _check_all_options_uncorrupted(node, "test_seq", "plain")
    node.safe_psql("SELECT nextval('test_seq');")
    _check_all_options_uncorrupted(node, "test_seq", "plain")
    node.safe_psql("SELECT setval('test_seq', 102);")
    _check_all_options_uncorrupted(node, "test_seq", "plain")
    node.safe_psql("ALTER SEQUENCE test_seq RESTART WITH 51")
    _check_all_options_uncorrupted(node, "test_seq", "plain")


def test_001_verify_heapam(create_pg):
    """verify_heapam detects line-pointer corruption and passes clean rels."""
    node = create_pg("test", no_data_checksums=True, start=False)
    node.append_conf("autovacuum=off")
    node.start()
    node.safe_psql("CREATE EXTENSION amcheck")

    # Uncorrupted table passes under every option combination.
    _fresh_test_table(node, "test")
    _check_all_options_uncorrupted(node, "test", "plain")

    _check_corrupt_table(node)
    _check_all_frozen_table(node)
    _check_sequence(node)
