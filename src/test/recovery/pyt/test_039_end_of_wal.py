# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/039_end_of_wal.pl.

End-of-WAL detection: by writing crafted bytes at the WAL insert point (a record
header with a zero/short/over-long length, a bad prev-link, a bad CRC, or a page
header with a bad magic/pageaddr/info/contrecord length) and crash-restarting,
recovery must stop at end-of-WAL with the exact diagnostic for each malformation,
covering both single-page records and records whose header spans a page boundary.
"""

import struct

import pypg

_BIG_ENDIAN = struct.pack("L", 0x12345678) == struct.pack(">L", 0x12345678)


def _record_header(xl_tot_len, xl_xid=0, xl_prev=0, xl_info=0, xl_rmid=0, xl_crc=0):
    """Build an XLogRecord header (mirrors build_record_header)."""
    hi, lo = (xl_prev, 0) if _BIG_ENDIAN else (0, xl_prev)
    return struct.pack(
        "<IIIIBBBBI", xl_tot_len, xl_xid, lo, hi, xl_info, xl_rmid, 0, 0, xl_crc
    )


def _page_header(xlp_magic, xlp_info=0, xlp_tli=0, xlp_pageaddr=0, xlp_rem_len=0):
    """Build an XLogPageHeaderData (mirrors build_page_header)."""
    hi, lo = (xlp_pageaddr, 0) if _BIG_ENDIAN else (0, xlp_pageaddr)
    return struct.pack("<HHIIII", xlp_magic, xlp_info, xlp_tli, lo, hi, xlp_rem_len)


class _Wal:
    """Holds the WAL geometry constants and the node under test."""

    def __init__(self, node, tli, seg_size, block_size, magic, contrecord):
        self.node = node
        self.tli = tli
        self.seg = seg_size
        self.block = block_size
        self.magic = magic
        self.contrecord = contrecord

    def next_page(self, lsn):
        return (lsn & ~(self.block - 1)) + self.block

    def prep(self, to_split=False):
        """emit, position near/away from page end, crash; return (prev, end)."""
        self.node.emit_wal(0)
        if to_split:
            end = self.node.advance_wal_to_record_splitting_zone(self.block)
        else:
            end = self.node.advance_wal_out_of_record_splitting_zone(self.block)
        self.node.stop("immediate")
        return end

    def check(self, offset, pattern, msg):
        self.node.start()
        assert self.node.log_matches(pattern, offset), msg


_GB2 = 2 * 1024 * 1024 * 1024


def test_039_end_of_wal(create_pg):
    """Recovery detects each end-of-WAL malformation with its exact message."""
    magic = int(
        pypg.scan_server_header(
            "access/xlog_internal.h", r"#define\s+XLOG_PAGE_MAGIC\s+(\w+)"
        )[0],
        16,
    )
    contrecord = int(
        pypg.scan_server_header(
            "access/xlog_internal.h", r"#define\s+XLP_FIRST_IS_CONTRECORD\s+(\w+)"
        )[0],
        16,
    )
    node = create_pg("node", start=False)
    node.append_conf(
        "wal_level = minimal\nmax_wal_senders = 0\nautovacuum = off\n"
        "checkpoint_timeout = '30min'\n"
    )
    node.start()
    node.safe_psql("CREATE TABLE t AS SELECT 42")
    seg = int(
        node.safe_psql(
            "SELECT setting FROM pg_settings WHERE name = 'wal_segment_size'"
        )
    )
    block = int(
        node.safe_psql("SELECT setting FROM pg_settings WHERE name = 'wal_block_size'")
    )
    tli = int(node.safe_psql("SELECT timeline_id FROM pg_control_checkpoint();"))
    node.safe_psql("SELECT pg_switch_wal();")
    w = _Wal(node, tli, seg, block, magic, contrecord)
    _single_page_cases(w)
    _multi_page_cases(w)
    _split_header_cases(w)


def _single_page_cases(w):
    node = w.node
    end = w.prep()
    sz = node.current_log_position()
    w.check(
        sz,
        r"invalid record length at .*: expected at least 24, got 0",
        "xl_tot_len zero",
    )
    end = w.prep()
    node.write_wal(w.tli, end, w.seg, _record_header(23))
    sz = node.current_log_position()
    w.check(
        sz,
        r"invalid record length at .*: expected at least 24, got 23",
        "xl_tot_len short",
    )
    node.emit_wal(0)
    end = node.advance_wal_to_record_splitting_zone(w.block)
    node.stop("immediate")
    node.write_wal(w.tli, end, w.seg, _record_header(1))
    sz = node.current_log_position()
    w.check(
        sz,
        r"invalid record length at .*: expected at least 24, got 1",
        "xl_tot_len short at end-of-page",
    )
    end = w.prep()
    node.write_wal(w.tli, end, w.seg, _record_header(_GB2, 0, 0xDEADBEEF))
    sz = node.current_log_position()
    w.check(sz, r"record with incorrect prev-link 0/DEADBEEF at .*", "xl_prev bad")
    node.emit_wal(0)
    node.advance_wal_out_of_record_splitting_zone(w.block)
    end = node.emit_wal(10)
    node.stop("immediate")
    node.write_wal(w.tli, end - 8, w.seg, b"!")
    sz = node.current_log_position()
    w.check(
        sz, r"incorrect resource manager data checksum in record at .*", "xl_crc bad"
    )


def _multi_page_cases(w):
    node = w.node
    cases = [
        (None, r"invalid magic number 0000 .* LSN .*", "xlp_magic zero"),
        (
            _page_header(0xCAFE, 0, 1, 0),
            r"invalid magic number CAFE .* LSN .*",
            "xlp_magic bad",
        ),
        (
            ("pageaddr", 0xBAAAAAAD),
            r"unexpected pageaddr 0/BAAAAAAD in .*, LSN .*,",
            "xlp_pageaddr bad",
        ),
        (("info", 0x1234), r"invalid info bits 1234 in .*, LSN .*,", "xlp_info bad"),
        (
            ("nocontre",),
            r"there is no contrecord flag at .*",
            "xlp_info lacks XLP_FIRST_IS_CONTRECORD",
        ),
        (
            ("remlen", 123456),
            r"invalid contrecord length 123456 .* at .*",
            "xlp_rem_len bad",
        ),
    ]
    for spec, pattern, msg in cases:
        node.emit_wal(0)
        prev = node.advance_wal_out_of_record_splitting_zone(w.block)
        end = node.emit_wal(0)
        node.stop("immediate")
        xid = 42 if spec and spec[0] in ("info", "nocontre", "remlen") else 0
        node.write_wal(w.tli, end, w.seg, _record_header(_GB2, xid, prev))
        page = _page_for(w, spec, end)
        if page is not None:
            node.write_wal(w.tli, w.next_page(end), w.seg, page)
        sz = node.current_log_position()
        w.check(sz, pattern, msg)


def _page_for(w, spec, end):
    if spec is None:
        return None
    kind = spec[0]
    nxt = w.next_page(end)
    if kind == "pageaddr":
        return _page_header(w.magic, w.contrecord, 1, spec[1])
    if kind == "info":
        return _page_header(w.magic, spec[1], 1, nxt)
    if kind == "nocontre":
        return _page_header(w.magic, 0, 1, nxt)
    if kind == "remlen":
        return _page_header(w.magic, w.contrecord, 1, nxt, spec[1])
    return spec  # a prebuilt page-header bytes object


def _split_header_cases(w):
    node = w.node
    cases = [
        (
            None,
            r"invalid magic number 0000 .* LSN .*",
            "xlp_magic zero (split record header)",
        ),
        (
            ("pageaddr", 0xBAAAAAAD),
            r"unexpected pageaddr 0/BAAAAAAD in .*, LSN .*,",
            "xlp_pageaddr bad (split record header)",
        ),
        (
            ("remlen", 123456),
            r"invalid contrecord length 123456 .* at .*",
            "xlp_rem_len bad (split record header)",
        ),
    ]
    for spec, pattern, msg in cases:
        node.emit_wal(0)
        end = node.advance_wal_to_record_splitting_zone(w.block)
        node.stop("immediate")
        node.write_wal(w.tli, end, w.seg, _record_header(_GB2, 0, 0xDEADBEEF))
        if spec is not None:
            if spec[0] == "pageaddr":
                page = _page_header(w.magic, w.contrecord, 1, spec[1])
            else:
                page = _page_header(w.magic, w.contrecord, 1, w.next_page(end), spec[1])
            node.write_wal(w.tli, w.next_page(end), w.seg, page)
        sz = node.current_log_position()
        w.check(sz, pattern, msg)
