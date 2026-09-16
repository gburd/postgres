# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of contrib/amcheck/t/006_verify_gin.pl.

gin_index_check() detects deliberately corrupted GIN index pages: wrong entry
order on leaf and inner entry-tree pages, swapped column attribute numbers,
parent/child key inconsistencies after a split, and a posting-tree leaf whose
TIDs exceed the parent's high key. Each scenario corrupts a specific block on
disk (with the server stopped) and asserts the exact detector message.
"""

import os
import re
import struct

# To force splits fast we want large-but-not-toasted tuples.
_FILLER_SIZE = 1900

_RANDOM_STRING_FN = (
    "CREATE OR REPLACE FUNCTION  random_string( INT ) RETURNS text AS $$\n"
    "SELECT string_agg(substring("
    "'0123456789abcdefghijklmnopqrstuvwxyz', "
    "ceil(random() * 36)::integer, 1), '') "
    "from generate_series(1, $1);\n"
    "$$ LANGUAGE SQL;"
)


def _relation_filepath(node, relname):
    """Return the absolute on-disk path of a relation's main fork."""
    rel = node.safe_psql("SELECT pg_relation_filepath('{}')".format(relname))
    assert rel, "path not found for relation {}".format(relname)
    return os.path.join(node.datadir, rel)


def _string_replace_block(filename, find, replace, blkno, blksize):
    """Replace find with replace within block blkno of filename (raw bytes).

    find may be a bytes literal (substituted everywhere it occurs) or a
    compiled bytes regex (re.sub with backreferences in the bytes replace).
    """
    offset = blkno * blksize
    with open(filename, "r+b") as fh:
        fh.seek(offset)
        buffer = fh.read(blksize)
        if isinstance(find, (bytes, bytearray)):
            buffer = buffer.replace(bytes(find), bytes(replace))
        else:
            buffer = find.sub(replace, buffer)
        assert len(buffer) == blksize, "block size changed during replace"
        fh.seek(offset)
        fh.write(buffer)


def _gin_check_stderr(node, indexname):
    """Run gin_index_check(indexname) and return psql's stderr."""
    result = node.psql_capture(
        "SELECT gin_index_check('{}')".format(indexname), on_error_stop=False
    )
    return result.stderr


def _wrong_order_expected(indexname):
    """The 'wrong tuple order on entry tree page' message for indexname."""
    return (
        'index "{}" has wrong tuple order on entry tree page, '
        "block 1, offset 2, rightlink 4294967295".format(indexname)
    )


def _insert_filler_rows(node, relname, prefixes):
    """Insert one row per prefix with a filler-padded text[] value."""
    for prefix in prefixes:
        node.safe_psql(
            "INSERT INTO {} (a) VALUES "
            "(('{{' || '{}' || random_string({}) ||'}}')::text[]);".format(
                relname, prefix, _FILLER_SIZE
            )
        )


def _invalid_entry_order_leaf_page(node, blksize):
    """Wrong entry order on a leaf page: replace aaaaa with ccccc in root."""
    relname, indexname = "test", "test_gin_idx"
    node.safe_psql(
        "DROP TABLE IF EXISTS {rel};\n"
        "CREATE TABLE {rel} (a text[]);\n"
        "INSERT INTO {rel} (a) VALUES ('{{aaaaa,bbbbb}}');\n"
        "CREATE INDEX {idx} ON {rel} USING gin (a);".format(rel=relname, idx=indexname)
    )
    relpath = _relation_filepath(node, indexname)
    node.stop()
    _string_replace_block(relpath, b"aaaaa", b"ccccc", 1, blksize)
    node.start()
    assert re.search(
        _wrong_order_expected(indexname), _gin_check_stderr(node, indexname)
    )


def _invalid_entry_order_inner_page(node, blksize):
    """Wrong entry order on an inner page (needs two splits)."""
    relname, indexname = "test", "test_gin_idx"
    node.safe_psql(
        "DROP TABLE IF EXISTS {rel};\n"
        "CREATE TABLE {rel} (a text[]);".format(rel=relname)
    )
    _insert_filler_rows(
        node,
        relname,
        [
            "pppppppppp",
            "qqqqqqqqqq",
            "rrrrrrrrrr",
            "ssssssssss",
            "tttttttttt",
            "uuuuuuuuuu",
            "vvvvvvvvvv",
            "wwwwwwwwww",
        ],
    )
    node.safe_psql(
        "CREATE INDEX {idx} ON {rel} USING gin (a);".format(rel=relname, idx=indexname)
    )
    relpath = _relation_filepath(node, indexname)
    node.stop()
    # rrrrrrrrrr and tttttttttt are keys in the root; break order on the first.
    _string_replace_block(relpath, b"rrrrrrrrrr", b"zzzzzzzzzz", 1, blksize)
    node.start()
    assert re.search(
        _wrong_order_expected(indexname), _gin_check_stderr(node, indexname)
    )


def _invalid_entry_columns_order(node, blksize):
    """Swapped attribute numbers in the root produce wrong column order."""
    relname, indexname = "test", "test_gin_idx"
    node.safe_psql(
        "DROP TABLE IF EXISTS {rel};\n"
        "CREATE TABLE {rel} (a text[],b text[]);\n"
        "INSERT INTO {rel} (a,b) VALUES ('{{aaa}}','{{bbb}}');\n"
        "CREATE INDEX {idx} ON {rel} USING gin (a,b);".format(
            rel=relname, idx=indexname
        )
    )
    relpath = _relation_filepath(node, indexname)
    node.stop()
    # root items order before: (1,aaa), (2,bbb); after: (2,aaa), (1,bbb)
    attrno_1 = struct.pack("=h", 1)
    attrno_2 = struct.pack("=h", 2)
    _string_replace_block(
        relpath,
        re.compile(re.escape(attrno_1) + rb"(.)(aaa)", re.DOTALL),
        attrno_2 + rb"\1\2",
        1,
        blksize,
    )
    _string_replace_block(
        relpath,
        re.compile(re.escape(attrno_2) + rb"(.)(bbb)", re.DOTALL),
        attrno_1 + rb"\1\2",
        1,
        blksize,
    )
    node.start()
    assert re.search(
        _wrong_order_expected(indexname), _gin_check_stderr(node, indexname)
    )


def _split_table_lmnxy(node, relname, indexname):
    """Create a GIN index whose entry tree splits (l/m/n/x/y prefixes)."""
    node.safe_psql(
        "DROP TABLE IF EXISTS {rel};\n"
        "CREATE TABLE {rel} (a text[]);".format(rel=relname)
    )
    _insert_filler_rows(
        node,
        relname,
        ["llllllllll", "mmmmmmmmmm", "nnnnnnnnnn", "xxxxxxxxxx", "yyyyyyyyyy"],
    )
    node.safe_psql(
        "CREATE INDEX {idx} ON {rel} USING gin (a);".format(rel=relname, idx=indexname)
    )


def _inconsistent_parent_key_parent_corrupted(node, blksize):
    """Parent key smaller than child keys: inconsistent records on page 3."""
    relname, indexname = "test", "test_gin_idx"
    _split_table_lmnxy(node, relname, indexname)
    relpath = _relation_filepath(node, indexname)
    node.stop()
    # nnnnnnnnnn is a parent key in the root; make it smaller than child keys.
    _string_replace_block(relpath, b"nnnnnnnnnn", b"aaaaaaaaaa", 1, blksize)
    node.start()
    expected = 'index "{}" has inconsistent records on page 3 offset 3'.format(
        indexname
    )
    assert re.search(expected, _gin_check_stderr(node, indexname))


def _inconsistent_parent_key_child_corrupted(node, blksize):
    """Child key bigger than parent key: inconsistent records on page 3."""
    relname, indexname = "test", "test_gin_idx"
    _split_table_lmnxy(node, relname, indexname)
    relpath = _relation_filepath(node, indexname)
    node.stop()
    # nnnnnnnnnn is the parent key in the root; make the child key bigger.
    _string_replace_block(relpath, b"nnnnnnnnnn", b"pppppppppp", 3, blksize)
    node.start()
    expected = 'index "{}" has inconsistent records on page 3 offset 3'.format(
        indexname
    )
    assert re.search(expected, _gin_check_stderr(node, indexname))


def _inconsistent_parent_key_posting_tree(node, blksize):
    """Posting-tree leaf TIDs exceed a corrupted parent high key."""
    relname, indexname = "test", "test_gin_idx"
    node.safe_psql(
        "DROP TABLE IF EXISTS {rel};\n"
        "CREATE TABLE {rel} (a text[]);\n"
        "INSERT INTO {rel} (a) select ('{{aaaaa}}') from generate_series(1,10000);\n"
        "CREATE INDEX {idx} ON {rel} USING gin (a);".format(rel=relname, idx=indexname)
    )
    relpath = _relation_filepath(node, indexname)
    node.stop()
    # Posting tree for 'aaaaa' has its root at block 2 and leaves 3 and 4.
    # Replace block 4's high key with (1,1) so leaf TIDs exceed it.
    find = re.compile(re.escape(struct.pack("=HHH", 0, 4, 0)) + rb"....", re.DOTALL)
    replace = struct.pack("=HHHHH", 0, 4, 0, 1, 1)
    _string_replace_block(relpath, find, replace, 2, blksize)
    node.start()
    expected = (
        'index "{}": tid exceeds parent\'s high key in postingTree '
        "leaf on block 4".format(indexname)
    )
    assert re.search(expected, _gin_check_stderr(node, indexname))


def test_006_verify_gin(create_pg):
    """gin_index_check detects deliberately corrupted GIN index pages."""
    node = create_pg("test", no_data_checksums=True, start=False)
    node.append_conf("autovacuum=off")
    node.start()
    blksize = int(node.safe_psql("SHOW block_size;"))
    node.safe_psql("CREATE EXTENSION amcheck")
    node.safe_psql(_RANDOM_STRING_FN)

    _invalid_entry_order_leaf_page(node, blksize)
    _invalid_entry_order_inner_page(node, blksize)
    _invalid_entry_columns_order(node, blksize)
    _inconsistent_parent_key_parent_corrupted(node, blksize)
    _inconsistent_parent_key_child_corrupted(node, blksize)
    _inconsistent_parent_key_posting_tree(node, blksize)
