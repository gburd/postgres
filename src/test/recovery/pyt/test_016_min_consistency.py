# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/016_min_consistency.pl.

Offline consistency check of on-disk pages against the control file's minimum
recovery LSN, exercising minRecoveryPoint updates from both the startup process
and the checkpointer.
"""

import re
import struct


def _find_largest_lsn(blocksize, filename):
    """Largest page LSN in a relation file, as an integer (hi << 32 | lo).

    The page LSN is stored as two little-endian 4-byte numbers at the start of
    each block.
    """
    max_hi, max_lo = 0, 0
    with open(filename, "rb") as handle:
        while True:
            buf = handle.read(blocksize)
            if not buf:
                break
            assert len(buf) == blocksize, "short read from {}".format(filename)
            hi, lo = struct.unpack_from("<LL", buf)
            if hi > max_hi or (hi == max_hi and lo > max_lo):
                max_hi, max_lo = hi, lo
    return (max_hi << 32) | max_lo


def _lsn_to_int(lsn):
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def test_min_consistency(pg_bin, create_pg):
    """minRecoveryPoint is never older than the max page LSN on disk."""
    primary = create_pg("primary", allows_streaming=True, start=False)
    # Tiny shared_buffers forces non-startup processes (checkpointer) to flush
    # buffers and update minRecoveryPoint; autovacuum off keeps it deterministic.
    primary.append_conf("shared_buffers = 128kB\nautovacuum = off")
    primary.start()

    primary.backup("bkp")
    standby = create_pg(
        "standby", from_backup=(primary, "bkp"), has_streaming=True, start=False
    )
    standby.start()

    primary.safe_psql(
        "CREATE TABLE test1 (a int) WITH (fillfactor = 10);\n"
        "INSERT INTO test1 SELECT generate_series(1, 10000);"
    )
    # Checkpoint + update forces post-checkpoint FPIs the startup process
    # replays, updating minRecoveryPoint.
    primary.safe_psql("CHECKPOINT;")
    primary.safe_psql("UPDATE test1 SET a = a + 1;")
    primary.wait_for_catchup(standby)

    # Fill the standby's shared buffers.
    standby.safe_psql("SELECT count(*) FROM test1;")

    # This update generates no FPIs, so the startup process won't flush pages.
    primary.safe_psql("UPDATE test1 SET a = a + 1;")

    blocksize = int(
        primary.safe_psql(
            "SELECT setting::int FROM pg_settings WHERE name = 'block_size';"
        )
    )
    relfilenode = primary.safe_psql("SELECT pg_relation_filepath('test1'::regclass);")
    primary.wait_for_catchup(standby)

    # Restart point on the standby makes the checkpointer update
    # minRecoveryPoint.
    standby.safe_psql("CHECKPOINT;")

    # Crash the primary so the standby never sees a shutdown checkpoint; the
    # standby stops cleanly so its checkpointer records the restart point.
    primary.stop("immediate")
    standby.stop("fast")

    offline_max_lsn = _find_largest_lsn(blocksize, str(standby.datadir / relfilenode))

    result = pg_bin.result(["pg_controldata", str(standby.datadir)])
    match = re.search(
        r"^Minimum recovery ending location:\s*(.*)$", result.stdout, re.MULTILINE
    )
    assert match, "No minRecoveryPoint in control file found"
    offline_recovery_lsn = _lsn_to_int(match.group(1).strip())

    assert (
        offline_recovery_lsn >= offline_max_lsn
    ), "table data is consistent with minRecoveryPoint"
