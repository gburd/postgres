# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/brin/t/02_wal_consistency.pl.

BRIN WAL consistency: a BRIN index built and updated on a primary produces revmap WAL records that replay correctly on a streaming standby (wal_consistency_checking).
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_02_wal_consistency(create_pg):
    """BRIN revmap WAL records replay correctly on a standby."""
    whiskey = create_pg("whiskey", allows_streaming=True, start=False)
    whiskey.append_conf("wal_consistency_checking = brin")
    whiskey.start()
    whiskey.safe_psql("create extension pageinspect")
    whiskey.safe_psql("create extension pg_walinspect")
    assert (
        whiskey.psql_capture(
            "SELECT pg_create_physical_replication_slot('standby_1');"
        ).exit_code
        == 0
    ), "physical slot created on primary"
    backup_name = "brinbkp"
    whiskey.backup(backup_name)
    charlie = create_pg(
        "charlie", from_backup=(whiskey, backup_name), has_streaming=True, start=False
    )
    charlie.append_conf("primary_slot_name = standby_1")
    charlie.start()
    whiskey.safe_psql(
        "create table tbl_timestamp0 (d1 timestamp(0) without time zone) with (fillfactor=10);\ncreate index on tbl_timestamp0 using brin (d1) with (pages_per_range = 1, autosummarize=false);"
    )
    start_lsn = whiskey.lsn("insert")
    whiskey.safe_psql(
        "do\n$$\ndeclare\n  current timestamp with time zone := '2019-03-27 08:14:01.123456789 UTC';\nbegin\n  loop\n    insert into tbl_timestamp0 select i from\n      generate_series(current, current + interval '1 day', '28 seconds') i;\n    perform brin_summarize_new_values('tbl_timestamp0_d1_idx');\n    if (brin_metapage_info(get_raw_page('tbl_timestamp0_d1_idx', 0))).lastrevmappage > 1 then\n      exit;\n    end if;\n    current := current + interval '1 day';\n  end loop;\nend\n$$;"
    )
    end_lsn = whiskey.lsn("flush")
    result = whiskey.psql_capture(
        "select count(*) from pg_get_wal_records_info('"
        + str(start_lsn)
        + "', '"
        + str(end_lsn)
        + "')\n\twhere resource_manager = 'BRIN' AND\n\trecord_type ILIKE '%revmap%'"
    )
    assert int(result.stdout) >= 1
    whiskey.wait_for_replay_catchup(charlie)
