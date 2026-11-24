# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_amcheck/t/004_verify_heapam.pl.

Demonstrates that pg_amcheck identifies specific kinds of within-page heap
corruption. A table with a precisely-known on-disk layout is built, then
individual tuples are corrupted one way each (xmin/xmax thresholds, t_hoff,
attribute counts, varlena/toast pointers, and HOT/redirect update-chain
breakage) by reading/rewriting the raw HeapTupleHeader with struct. pg_amcheck
must report exactly the expected corruption messages.
"""

import os
import struct

import pytest

# HeapTupleHeader layout for our (BIGINT, TEXT, TEXT) table, mirroring the Perl
# pack code 'LLLSSSSSCCLLCCCCCCCCCCllLL' (native byte order, 58 bytes total).
_PACK_FMT = "=LLLHHHHHBBLLBBBBBBBBBBllLL"
_PACK_LENGTH = 58
_FIELDS = [
    "t_xmin",
    "t_xmax",
    "t_field3",
    "bi_hi",
    "bi_lo",
    "ip_posid",
    "t_infomask2",
    "t_infomask",
    "t_hoff",
    "t_bits",
    "a_1",
    "a_2",
    "b_header",
    "b_body1",
    "b_body2",
    "b_body3",
    "b_body4",
    "b_body5",
    "b_body6",
    "b_body7",
    "c_va_header",
    "c_va_vartag",
    "c_va_rawsize",
    "c_va_extinfo",
    "c_va_valueid",
    "c_va_toastrelid",
]

# #define constants from access/htup_details.h used while corrupting.
_HEAP_HASNULL = 0x0001
_HEAP_XMIN_COMMITTED = 0x0100
_HEAP_XMIN_INVALID = 0x0200
_HEAP_XMAX_COMMITTED = 0x0400
_HEAP_XMAX_INVALID = 0x0800
_HEAP_NATTS_MASK = 0x07FF
_HEAP_XMAX_IS_MULTI = 0x1000
_HEAP_HOT_UPDATED = 0x4000
_HEAP_ONLY_TUPLE = 0x8000

_ROWCOUNT = 44
_ROWCOUNT_BASIC = 16

_U16 = 0xFFFF


def _read_tuple(fh, offset):
    """Read and unpack one table tuple's header into a field dict."""
    fh.seek(offset)
    buffer = fh.read(_PACK_LENGTH)
    values = struct.unpack(_PACK_FMT, buffer)
    tup = dict(zip(_FIELDS, values))
    tup["b"] = "".join(chr(tup["b_body{}".format(i)]) for i in range(1, 8))
    return tup


def _write_tuple(fh, offset, tup):
    """Pack and write a (possibly modified) tuple header back to the file."""
    buffer = struct.pack(_PACK_FMT, *(tup[f] for f in _FIELDS))
    fh.seek(offset)
    fh.write(buffer)


def _header(blkno, offnum=None, attnum=None):
    """Expected verify_heapam() message prefix for the given location."""
    base = r'heap table "postgres\.public\.test"'
    if attnum is not None:
        return r"{}, block {}, offset {}, attribute {}:\s+".format(
            base, blkno, offnum, attnum
        )
    if offnum is not None:
        return r"{}, block {}, offset {}:\s+".format(base, blkno, offnum)
    return r"{}, block {}:\s+".format(base, blkno)


_SETUP_SQL = """\
CREATE TABLE public.test (a BIGINT, b TEXT, c TEXT);
ALTER TABLE public.test SET (autovacuum_enabled=false);
ALTER TABLE public.test ALTER COLUMN c SET STORAGE EXTERNAL;
CREATE INDEX test_idx ON public.test(a, b);
"""

_JUNK_SQL = """\
CREATE TABLE public.junk AS SELECT 'junk'::TEXT AS junk_column;
ALTER TABLE public.junk SET (autovacuum_enabled=false);
VACUUM FREEZE public.junk
"""


def _populate(node):
    """Insert all rows / HOT chains needed for the corruption scenarios."""
    node.safe_psql(
        "INSERT INTO public.test (a, b, c)\n"
        "    SELECT\n"
        "        x'DEADF9F9DEADF9F9'::bigint,\n"
        "        'abcdefg',\n"
        "        repeat('w', 10000)\n"
        "FROM generate_series(1, {});\n"
        "VACUUM FREEZE public.test;".format(_ROWCOUNT_BASIC)
    )
    # offnum 17/18, redirects after HOT prune; 19/20 the HOT tuples.
    node.safe_psql(
        "INSERT INTO public.test (a, b, c)\n"
        "    VALUES ( x'DEADF9F9DEADF9F9'::bigint, 'abcdefg',\n"
        "             generate_series(1,2));\n"
        "UPDATE public.test SET c = 'a' WHERE c = '1';\n"
        "UPDATE public.test SET c = 'a' WHERE c = '2';"
    )
    node.safe_psql(
        "INSERT INTO public.test (a, b, c)\n"
        "    VALUES ( x'DEADF9F9DEADF9F9'::bigint, 'abcdefg',\n"
        "             generate_series(3,6));\n"
        "UPDATE public.test SET c = 'a' WHERE c = '3';\n"
        "UPDATE public.test SET c = 'a' WHERE c = '4';"
    )
    # Aborted HOT update then re-use of the slot.
    node.safe_psql(
        "BEGIN;\n"
        "UPDATE public.test SET c = 'a' WHERE c = '5';\n"
        "ABORT;\n"
        "VACUUM FREEZE public.test;"
    )
    node.safe_psql(
        "UPDATE public.test SET c = 'a' WHERE c = '6';\nVACUUM FREEZE public.test;"
    )
    # HOT chain data (no freeze).
    node.safe_psql(
        "INSERT INTO public.test (a, b, c)\n"
        "    VALUES ( x'DEADF9F9DEADF9F9'::bigint, 'abcdefg',\n"
        "             generate_series(7,15));\n"
        "UPDATE public.test SET c = 'a' WHERE c = '7';\n"
        "UPDATE public.test SET c = 'a' WHERE c = '10';\n"
        "UPDATE public.test SET c = 'a' WHERE c = '11';\n"
        "UPDATE public.test SET c = 'a' WHERE c = '12';\n"
        "UPDATE public.test SET c = 'a' WHERE c = '13';\n"
        "UPDATE public.test SET c = 'a' WHERE c = '14';\n"
        "UPDATE public.test SET c = 'a' WHERE c = '15';"
    )
    node.safe_psql("BEGIN;\nUPDATE public.test SET c = 'a' WHERE c = '9';\nABORT;")
    node.safe_psql("BEGIN;\nPREPARE TRANSACTION 'in_progress_tx';")


def _get_lp_offsets(node):
    """Per-line-pointer offsets (-1 for redirects), in offnum order."""
    text = node.safe_psql(
        "SELECT CASE WHEN lp_flags = 2 THEN -1 ELSE lp_off END\n"
        "FROM heap_page_items(get_raw_page('test', 'main', 0))"
    )
    return [int(x) for x in text.split("\n")]


def _detect_layout_and_endianness(node, relpath, lp_off):
    """Verify the expected page layout; return 'little'/'big' or skip.

    Mirrors the Perl sanity checks that skip_all if the on-disk layout differs
    from expectations on this platform.
    """
    endianness = None
    with open(relpath, "rb") as fh:
        for tupidx in range(_ROWCOUNT):
            offset = lp_off[tupidx]
            if offset == -1:
                continue
            tup = _read_tuple(fh, offset)
            if (
                tup["a_1"] != 0xDEADF9F9
                or tup["a_2"] != 0xDEADF9F9
                or tup["b"] != "abcdefg"
            ):
                node.clean_node()
                pytest.skip(
                    "Page layout of index {} differs from our "
                    "expectations".format(tupidx)
                )
            endianness = "little" if tup["b_header"] == 0x11 else "big"
    assert endianness is not None
    return endianness


def _corrupt_basic_tuple(tup, offnum, ctx):
    """Apply the offnum-specific basic-validation corruption (offnum 1-16)."""
    if offnum <= 8:
        _corrupt_basic_tuple_lo(tup, offnum, ctx)
    else:
        _corrupt_basic_tuple_hi(tup, offnum, ctx)


def _uncommit_xmin(tup):
    """Clear the XMIN_COMMITTED and XMIN_INVALID infomask bits."""
    tup["t_infomask"] &= ~_HEAP_XMIN_COMMITTED & _U16
    tup["t_infomask"] &= ~_HEAP_XMIN_INVALID & _U16


# verify_heapam() message bodies (single literals, no implicit concatenation).
_M_HOFF = (
    r"tuple data should begin at byte 24, but actually begins at byte {} "
    r"\(3 attributes, no nulls\)"
)
_M_NATTS = r"number of attributes {} exceeds maximum 3 expected for table"
_M_XMIN_FREEZE = r"xmin {} precedes relation freeze threshold 0:\d+"
_M_XMIN_OLDEST = r"xmin {} precedes oldest valid transaction ID 0:\d+"
_M_XMAX_OLDEST = r"xmax {} precedes oldest valid transaction ID 0:\d+"
_M_XMIN_FUTURE = r"xmin {} equals or exceeds next valid transaction ID 0:\d+"
_M_HOFF_BEYOND = r"data begins at offset 152 beyond the tuple length 58"
_M_HOFF_NULLS = (
    r"tuple data should begin at byte 280, but actually begins at byte 24 "
    r"\(2047 attributes, has nulls\)"
)
_M_ATTR_LEN = (
    r"attribute with length \d+ ends at offset \d+ beyond total tuple length \d+"
)
_M_TOAST = r"toast value \d+ not found in toast table"
_M_MXID_EXCEEDS = (
    r"multitransaction ID 4 equals or exceeds next valid multitransaction ID 1"
)
_M_MXID_PRECEDES = (
    r"multitransaction ID 4000000000 precedes relation minimum "
    r"multitransaction ID threshold 1"
)


def _corrupt_basic_tuple_lo(tup, offnum, ctx):
    """Basic-validation corruptions for offnum 1-8 (xmin/xmax/t_hoff)."""
    header = _header(0, offnum)
    expected = ctx["expected"]
    if offnum == 1:
        xmin = ctx["relfrozenxid"] - 1
        tup["t_xmin"] = xmin
        _uncommit_xmin(tup)
        expected.append(header + _M_XMIN_FREEZE.format(xmin))
    elif offnum == 2:
        tup["t_xmin"] = 3
        _uncommit_xmin(tup)
        expected.append(header + _M_XMIN_OLDEST.format(3))
    elif offnum == 3:
        tup["t_xmin"] = 4026531839
        _uncommit_xmin(tup)
        expected.append(header + _M_XMIN_OLDEST.format(4026531839))
    elif offnum == 4:
        tup["t_xmax"] = 4026531839
        tup["t_infomask"] &= ~_HEAP_XMAX_INVALID & _U16
        expected.append(header + _M_XMAX_OLDEST.format(4026531839))
    elif offnum == 5:
        tup["t_hoff"] += 128
        expected.append(header + _M_HOFF_BEYOND)
        expected.append(header + _M_HOFF.format(152))
    elif offnum == 6:
        tup["t_hoff"] += 3
        expected.append(header + _M_HOFF.format(27))
    elif offnum == 7:
        tup["t_hoff"] -= 8
        expected.append(header + _M_HOFF.format(16))
    elif offnum == 8:
        tup["t_hoff"] -= 3
        expected.append(header + _M_HOFF.format(21))


def _corrupt_basic_tuple_hi(tup, offnum, ctx):
    """Basic-validation corruptions for offnum 9-16 (natts/varlena/xmax)."""
    header = _header(0, offnum)
    expected = ctx["expected"]
    if offnum == 9:
        tup["t_infomask2"] |= _HEAP_NATTS_MASK
        expected.append(header + _M_NATTS.format(2047))
    elif offnum == 10:
        tup["t_infomask"] |= _HEAP_HASNULL
        tup["t_infomask2"] |= _HEAP_NATTS_MASK
        tup["t_bits"] = 0xAA
        expected.append(header + _M_HOFF_NULLS)
    elif offnum == 11:
        tup["t_infomask"] |= _HEAP_HASNULL
        tup["t_infomask2"] |= _HEAP_NATTS_MASK & 0x40
        tup["t_bits"] = 0xAA
        tup["t_hoff"] = 32
        expected.append(header + _M_NATTS.format(67))
    elif offnum == 12:
        tup["b_header"] = 0xFC if ctx["endianness"] == "little" else 0x3F
        tup["b_body1"] = 0xFF
        tup["b_body2"] = 0xFF
        tup["b_body3"] = 0xFF
        expected.append(_header(0, offnum, 1) + _M_ATTR_LEN)
    elif offnum == 13:
        tup["c_va_valueid"] = 0xFFFFFFFF
        expected.append(_header(0, offnum, 2) + _M_TOAST)
    elif offnum == 14:
        tup["t_infomask"] |= _HEAP_XMAX_COMMITTED
        tup["t_infomask"] |= _HEAP_XMAX_IS_MULTI
        tup["t_xmax"] = 4
        expected.append(header + _M_MXID_EXCEEDS)
    elif offnum == 15:
        tup["t_infomask"] |= _HEAP_XMAX_COMMITTED
        tup["t_infomask"] |= _HEAP_XMAX_IS_MULTI
        tup["t_xmax"] = 4000000000
        expected.append(header + _M_MXID_PRECEDES)
    elif offnum == 16:
        tup["t_xmin"] = 123456
        _uncommit_xmin(tup)
        expected.append(header + _M_XMIN_FUTURE.format(123456))


# Chain/redirect message bodies (single literals, no implicit concatenation).
_M_REDIR_NONHOT = (
    r"redirected line pointer points to a non-heap-only tuple at offset \d+"
)
_M_REDIR_REDIR = (
    r"redirected line pointer points to another redirected line pointer "
    r"at offset \d+"
)
_M_REDIR_DUP = (
    r"redirect line pointer points to offset \d+, but offset \d+ also points there"
)
_M_NONHOT_PRODUCED_HOT = (
    r"non-heap-only update produced a heap-only tuple at offset \d+"
)
_M_NEWVER_DUP = (
    r"tuple points to new version at offset \d+, but offset \d+ also points there"
)
_M_ABORTED_COMMITTED = (
    r"tuple with aborted xmin \d+ was updated to produce a tuple at offset \d+ "
    r"with committed xmin \d+"
)
_M_ROOT_HOT = r"tuple is root of chain but is marked as heap-only tuple"
_M_HOT_NO_UPDATE = r"tuple is heap only, but not the result of an update"
_M_HOT_PRODUCED_NONHOT = (
    r"heap-only update produced a non-heap only tuple at offset \d+"
)
_M_HOT_XMAX0 = r"tuple has been HOT updated, but xmax is 0"
_M_INPROGRESS_COMMITTED = (
    r"tuple with in-progress xmin \d+ was updated to produce a tuple at "
    r"offset \d+ with committed xmin \d+"
)
_M_ABORTED_INPROGRESS = (
    r"tuple with aborted xmin \d+ was updated to produce a tuple at offset \d+ "
    r"with in-progress xmin \d+"
)


def _corrupt_chain_tuple(fh, tup, offnum, ctx):
    """Apply the offnum-specific HOT/redirect chain corruption (offnum>=17).

    Returns True if tup should be written back, False otherwise (some offnums
    rewrite a raw line pointer directly or leave the tuple unchanged).
    """
    if offnum in (17, 18, 19, 22):
        return _corrupt_chain_redirects(fh, tup, offnum, ctx)
    return _corrupt_chain_hot(tup, offnum, ctx)


def _corrupt_chain_redirects(fh, tup, offnum, ctx):
    """Redirect-related corruptions (offnum 17, 18, 19, 22)."""
    header = _header(0, offnum)
    expected = ctx["expected"]
    little = ctx["endianness"] == "little"
    if offnum == 17:
        assert tup is None, "offnum 17 should be a redirect"
        expected.append(header + _M_REDIR_NONHOT)
        return False
    if offnum == 18:
        assert tup is None, "offnum 18 should be a redirect"
        fh.seek(92)
        fh.write(struct.pack("=L", 0x00010011 if little else 0x00230000))
        expected.append(header + _M_REDIR_REDIR)
        return False
    if offnum == 19:
        tup["t_infomask2"] &= ~_HEAP_ONLY_TUPLE & _U16
        return True
    # offnum == 22
    fh.seek(108)
    fh.write(struct.pack("=L", 0x00010019 if little else 0x00330000))
    expected.append(header + _M_REDIR_DUP)
    return False


def _corrupt_chain_hot(tup, offnum, ctx):  # pylint: disable=too-many-return-statements
    """HOT update-chain corruptions (offnum 28-43)."""
    header = _header(0, offnum)
    expected = ctx["expected"]
    if offnum == 28:
        tup["t_infomask2"] &= ~_HEAP_HOT_UPDATED & _U16
        expected.append(header + _M_NONHOT_PRODUCED_HOT)
        ctx["pred_xmax"] = tup["t_xmax"]
        ctx["pred_posid"] = tup["ip_posid"]
        return True
    if offnum == 29:
        tup["t_xmax"] = ctx["pred_xmax"]
        tup["ip_posid"] = ctx["pred_posid"]
        expected.append(header + _M_NEWVER_DUP)
        return True
    if offnum == 30:
        ctx["aborted_xid"] = tup["t_xmax"]
        return False
    if offnum == 31:
        tup["t_xmin"] = ctx["aborted_xid"]
        tup["t_infomask"] &= ~_HEAP_XMIN_COMMITTED & _U16
        expected.append(header + _M_ABORTED_COMMITTED)
        return True
    if offnum == 32:
        tup["t_infomask2"] |= _HEAP_ONLY_TUPLE
        expected.append(header + _M_ROOT_HOT)
        expected.append(header + _M_HOT_NO_UPDATE)
        return True
    if offnum == 33:
        expected.append(header + _M_HOT_PRODUCED_NONHOT)
        return False
    if offnum == 34:
        tup["t_xmax"] = 0
        expected.append(header + _M_HOT_XMAX0)
        return True
    if offnum == 35:
        tup["t_xmin"] = ctx["in_progress_xid"]
        tup["t_infomask"] &= ~_HEAP_XMIN_COMMITTED & _U16
        expected.append(header + _M_INPROGRESS_COMMITTED)
        return True
    if offnum == 36:
        tup["t_xmin"] = ctx["aborted_xid"]
        tup["t_xmax"] = ctx["in_progress_xid"]
        tup["t_infomask"] &= ~_HEAP_XMIN_COMMITTED & _U16
        expected.append(header + _M_ABORTED_INPROGRESS)
        return True
    if offnum == 40:
        tup["t_infomask2"] &= ~_HEAP_ONLY_TUPLE & _U16
        return True
    if offnum == 43:
        tup["t_xmin"] = ctx["in_progress_xid"]
        tup["t_infomask"] &= ~_HEAP_XMIN_COMMITTED & _U16
        return True
    return False


def _corrupt_all_tuples(relpath, lp_off, ctx):
    """Walk every line pointer, applying one corruption per relevant offnum."""
    with open(relpath, "r+b") as fh:
        for tupidx in range(_ROWCOUNT):
            offnum = tupidx + 1
            offset = lp_off[tupidx]
            tup = None if offset == -1 else _read_tuple(fh, offset)
            if offnum <= _ROWCOUNT_BASIC:
                if tup is None:
                    continue
                _corrupt_basic_tuple(tup, offnum, ctx)
                _write_tuple(fh, offset, tup)
            else:
                should_write = _corrupt_chain_tuple(fh, tup, offnum, ctx)
                if should_write and tup is not None:
                    _write_tuple(fh, offset, tup)


def test_004_verify_heapam(create_pg):
    """pg_amcheck reports each deliberately-injected heap corruption type."""
    os.umask(0o077)
    node = create_pg("test", no_data_checksums=True, start=False)
    node.append_conf("autovacuum=off")
    node.append_conf("max_prepared_transactions=10")
    node.start()
    port = node.port
    node.safe_psql("CREATE EXTENSION amcheck")
    node.safe_psql("CREATE EXTENSION pageinspect")

    node.safe_psql("VACUUM FREEZE")
    node.safe_psql(_SETUP_SQL)
    node.safe_psql(_JUNK_SQL)

    rel = node.safe_psql("SELECT pg_relation_filepath('public.test')")
    relpath = os.path.join(node.datadir, rel)

    _populate(node)

    in_progress_xid = node.safe_psql("SELECT transaction FROM pg_prepared_xacts;")
    relfrozenxid = int(
        node.safe_psql("select relfrozenxid from pg_class where relname = 'test'")
    )
    datfrozenxid = int(
        node.safe_psql(
            "select datfrozenxid from pg_database where datname = 'postgres'"
        )
    )
    if datfrozenxid <= 3 or datfrozenxid >= relfrozenxid:
        node.clean_node()
        pytest.skip(
            "Xid thresholds not as expected: got datfrozenxid = {}, "
            "relfrozenxid = {}".format(datfrozenxid, relfrozenxid)
        )

    lp_off = _get_lp_offsets(node)
    assert len(lp_off) == _ROWCOUNT, "row offset counts mismatch"

    node.stop()
    endianness = _detect_layout_and_endianness(node, relpath, lp_off)
    node.start()

    node.command_ok(
        ["pg_amcheck", "--port", str(port), "postgres"],
        "pg_amcheck test table, prior to corruption",
    )
    node.command_ok(
        ["pg_amcheck", "--port", str(port), "postgres"],
        "pg_amcheck test table and index, prior to corruption",
    )

    node.stop()

    ctx = {
        "relfrozenxid": relfrozenxid,
        "in_progress_xid": int(in_progress_xid),
        "endianness": endianness,
        "expected": [],
        "pred_xmax": None,
        "pred_posid": None,
        "aborted_xid": None,
    }
    _corrupt_all_tuples(relpath, lp_off, ctx)
    node.start()

    node.command_checks_all(
        ["pg_amcheck", "--no-dependent-indexes", "--port", str(port), "postgres"],
        2,
        ctx["expected"],
        [],
        "Expected corruption message output",
    )
    node.safe_psql("COMMIT PREPARED 'in_progress_tx';")

    node.teardown_node()
    node.clean_node()
