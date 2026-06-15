# Copyright (c) 2022-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_waldump/t/002_save_fullpage.pl.

pg_waldump --save-fullpage extracts full-page images from WAL to files named
with the page LSN. Each saved file's name encodes an LSN that is at or after the
LSN stored in the page header (the page was written before the WAL record's
LSN), and at least one full-page image is produced.
"""

import glob
import os
import re
import struct

_FILE_RE = re.compile(
    r"^[0-9A-F]{8}-([0-9A-F]{8})-([0-9A-F]{8})"
    r"[.][0-9]+[.][0-9]+[.][0-9]+[.][0-9]+(?:_vm|_init|_fsm|_main)?$"
)


def _get_block_lsn(path, blocksize):
    """Return the (hi, lo) page-header LSN, as 8-hex strings, from a block."""
    with open(path, "rb") as fh:
        block = fh.read(blocksize)
    assert len(block) == blocksize, "could not read block"
    # pd_lsn is stored in host (native) byte order on disk; the Perl original
    # uses unpack('LL', ...) (native), so use the native struct format here too
    # rather than forcing little-endian, which would byte-swap on big-endian.
    lsn_hi, lsn_lo = struct.unpack("=II", block[:8])
    return "{:08X}".format(lsn_hi), "{:08X}".format(lsn_lo)


def test_002_save_fullpage(create_pg, tmp_path):
    """Saved full-page-image filenames carry an LSN at/after the page LSN."""
    node = create_pg("main", start=False)
    node.append_conf("\nwal_level = 'replica'\nmax_wal_senders = 4\n")
    node.start()
    node.safe_psql(
        "SELECT 'init' FROM pg_create_physical_replication_slot"
        "('regress_pg_waldump_slot', true, false);\n"
        "CREATE TABLE test_table AS SELECT generate_series(1,100) a;\n"
        "-- Force FPWs on the next writes.\n"
        "CHECKPOINT;\n"
        "UPDATE test_table SET a = a + 1;\n"
    )
    walfile_name, blocksize = node.safe_psql(
        "SELECT pg_walfile_name(pg_switch_wal()), current_setting('block_size')"
    ).split("|")
    blocksize = int(blocksize)
    relation = node.safe_psql(
        "SELECT format("
        "'%s/%s/%s',"
        " CASE WHEN reltablespace = 0 THEN dattablespace ELSE reltablespace END,"
        " pg_database.oid,"
        " pg_relation_filenode(pg_class.oid))"
        " FROM pg_class, pg_database"
        " WHERE relname = 'test_table' AND datname = current_database()"
    )
    walfile = "{}/pg_wal/{}".format(node.datadir, walfile_name)
    tmp_folder = tmp_path
    assert os.path.isfile(walfile), "Got a WAL file"
    node.command_ok(
        [
            "pg_waldump",
            "--quiet",
            "--save-fullpage",
            "{}/raw".format(tmp_folder),
            "--relation",
            relation,
            walfile,
        ],
        "pg_waldump with --save-fullpage runs",
    )
    file_count = 0
    for fullpath in glob.glob("{}/raw/*".format(tmp_folder)):
        filename = os.path.basename(fullpath)
        match = _FILE_RE.match(filename)
        assert match, "verify filename format for file {}".format(filename)
        file_count += 1
        hi_lsn_fn, lo_lsn_fn = match.group(1), match.group(2)
        hi_lsn_bk, lo_lsn_bk = _get_block_lsn(fullpath, blocksize)
        assert hi_lsn_fn + lo_lsn_fn >= hi_lsn_bk + lo_lsn_bk, (
            "LSN stored in the file {}/{} precedes the one stored in the "
            "block {}/{}".format(hi_lsn_fn, lo_lsn_fn, hi_lsn_bk, lo_lsn_bk)
        )
    assert file_count > 0, "verify that at least one block has been saved"
