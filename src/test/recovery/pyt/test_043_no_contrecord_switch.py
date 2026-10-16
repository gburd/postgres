# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/043_no_contrecord_switch.pl.

A WAL page whose header is zeroed (magic 0000) at a page boundary must stop
replay cleanly with an "invalid magic number" message rather than crashing.
Two standbys restoring from the archive both report the bad page; one is then
promoted, generates WAL on the new timeline, and the second standby streams from
it and catches up to the new content.
"""

import shutil


def _wal_segment_name(tli, segment):
    return "{:08X}{:08X}{:08X}".format(tli, 0, segment)


def _get_int_setting(node, name):
    return int(
        node.safe_psql("SELECT setting FROM pg_settings WHERE name = '{}'".format(name))
    )


def test_043_no_contrecord_switch(create_pg):
    """A zeroed WAL page boundary halts replay cleanly and allows promotion."""
    primary = create_pg(
        "primary", allows_streaming=True, has_archiving=True, start=False
    )
    primary.append_conf(
        "\nautovacuum = off\ncheckpoint_timeout = '30min'\nwal_keep_size = 1GB\n"
    )
    primary.start()
    primary.backup("backup")
    primary.safe_psql("CREATE TABLE t AS SELECT 0")
    wal_segment_size = _get_int_setting(primary, "wal_segment_size")
    wal_block_size = _get_int_setting(primary, "wal_block_size")
    tli = int(primary.safe_psql("SELECT timeline_id FROM pg_control_checkpoint()"))
    primary.emit_wal(0)
    end_lsn = primary.advance_wal_out_of_record_splitting_zone(wal_block_size)
    overflow_size = wal_block_size - (end_lsn % wal_block_size)
    end_lsn = primary.emit_wal(overflow_size)
    primary.stop("immediate")
    start_page = end_lsn & ~(wal_block_size - 1)
    wal_file = primary.write_wal(
        tli, start_page, wal_segment_size, b"\x00" * wal_block_size
    )
    shutil.copy(wal_file, str(primary.archive_dir))
    standby1 = create_pg(
        "standby1",
        from_backup=(primary, "backup"),
        standby=True,
        has_restoring=True,
        start=False,
    )
    standby2 = create_pg(
        "standby2",
        from_backup=(primary, "backup"),
        standby=True,
        has_restoring=True,
        start=False,
    )
    log_size1 = standby1.current_log_position()
    log_size2 = standby2.current_log_position()
    standby1.start()
    standby2.start()
    segment = start_page // wal_segment_size
    offset = start_page % wal_segment_size
    segment_name = _wal_segment_name(tli, segment)
    pattern = r"invalid magic number 0000 .* segment {}.* offset {}".format(
        segment_name, offset
    )
    standby1.wait_for_log(pattern, log_size1)
    standby2.wait_for_log(pattern, log_size2)
    standby1.promote()
    standby1.safe_psql("SELECT pg_switch_wal()")
    standby1.safe_psql("INSERT INTO t SELECT * FROM generate_series(1, 1000)")
    standby2.enable_streaming(standby1)
    standby2.reload()
    standby1.wait_for_replay_catchup(standby2)
    result = standby2.safe_psql("SELECT count(*) FROM t")
    assert result == "1001", "check streamed content on standby2"
