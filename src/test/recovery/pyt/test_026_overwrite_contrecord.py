# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/026_overwrite_contrecord.pl.

Already-propagated WAL segments ending in incomplete WAL records: a standby
missing the last WAL file must replay the "overwrite contrecord" from a
divergent file written after the primary restarts, and promote successfully.
"""

import re

from pypg import slurp_file

# Fill the current WAL segment, leaving room only for the start of a large
# record, then stop.
_FILL_WAL = """\
DO $$
DECLARE
    wal_segsize int := setting::int FROM pg_settings WHERE name = 'wal_segment_size';
    remain int;
    iters  int := 0;
BEGIN
    LOOP
        INSERT into filler
        select g, repeat(encode(sha256(g::text::bytea), 'hex'), (random() * 15 + 1)::int)
        from generate_series(1, 10) g;

        remain := wal_segsize - (pg_current_wal_insert_lsn() - '0/0') % wal_segsize;
        IF remain < 2 * setting::int from pg_settings where name = 'block_size' THEN
            RAISE log 'exiting after % iterations, % bytes to end of WAL segment', iters, remain;
            EXIT;
        END IF;
        iters := iters + 1;
    END LOOP;
END
$$;
"""


def test_overwrite_contrecord(create_pg):
    """A standby replays past an overwritten contrecord and promotes."""
    node = create_pg("primary", allows_streaming=True, start=False)
    node.append_conf("autovacuum = off\nwal_keep_size = 1GB")
    node.start()

    node.safe_psql("create table filler (a int, b text)")
    node.safe_psql(_FILL_WAL)

    initfile = node.safe_psql("SELECT pg_walfile_name(pg_current_wal_insert_lsn())")
    node.safe_psql(
        "SELECT pg_logical_emit_message(true, 'test 026', repeat('xyzxz', 123456))"
    )
    endfile = node.safe_psql("SELECT pg_walfile_name(pg_current_wal_insert_lsn())")
    assert initfile != endfile, "{} differs from {}".format(initfile, endfile)

    # Stop abruptly (no shutdown checkpoint), then remove the tail file; on
    # startup the large message is overwritten with new contents.
    node.stop("immediate")
    (node.datadir / "pg_wal" / endfile).unlink()

    node.backup_fs_cold("backup")
    node_standby = create_pg(
        "standby", from_backup=(node, "backup"), has_streaming=True, start=False
    )
    node_standby.start()
    node.start()

    node.safe_psql("create table foo (a text); insert into foo values ('hello')")
    node.safe_psql("SELECT pg_logical_emit_message(true, 'test 026', 'AABBCC')")

    until_lsn = node.safe_psql("SELECT pg_current_wal_lsn()")
    assert node_standby.poll_query_until(
        "SELECT '{}'::pg_lsn <= pg_last_wal_replay_lsn()".format(until_lsn)
    ), "standby caught up"

    assert (
        node_standby.safe_psql("select * from foo") == "hello"
    ), "standby replays past overwritten contrecord"

    assert re.search(
        r"successfully skipped missing contrecord at", slurp_file(node_standby.log)
    ), "found log line in standby"

    node_standby.promote()
    node.stop()
    node_standby.stop()
