# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/test_custom_rmgrs/t/001_basic.pl.

Custom WAL resource manager test module: a custom rmgr (preloaded via
shared_preload_libraries) writes a WAL record whose contents are then read back
and verified via pg_walinspect. Generated from the Perl original via
.agent/gen_golden.py.
"""


def test_001_basic(create_pg):
    """Generated golden port of 001_basic."""
    node = create_pg("main", start=False)
    node.append_conf(
        "\nwal_level = 'replica'\nmax_wal_senders = 4\nshared_preload_libraries = 'test_custom_rmgrs'\n"
    )
    node.start()
    node.safe_psql("CREATE EXTENSION test_custom_rmgrs")
    node.safe_psql("CREATE EXTENSION pg_walinspect")
    start_lsn = node.safe_psql(
        "SELECT lsn FROM pg_create_physical_replication_slot('regress_test_slot1', true, false);"
    )
    record_end_lsn = node.safe_psql(
        "SELECT * FROM test_custom_rmgrs_insert_wal_record('payload123')"
    )
    node.safe_psql("SELECT pg_switch_wal()")
    end_lsn = node.safe_psql("SELECT pg_current_wal_flush_lsn()")
    row_count = node.safe_psql(
        "SELECT count(*) FROM pg_get_wal_resource_managers()\n\t\tWHERE rm_name = 'test_custom_rmgrs';"
    )
    assert (
        row_count == "1"
    ), "custom WAL resource manager has successfully registered with the server"
    expected = (
        str(record_end_lsn)
        + "|test_custom_rmgrs|TEST_CUSTOM_RMGRS_MESSAGE|0|payload (10 bytes): payload123"
    )
    result = node.safe_psql(
        "SELECT end_lsn, resource_manager, record_type, fpi_length, description FROM pg_get_wal_records_info('"
        + str(start_lsn)
        + "', '"
        + str(end_lsn)
        + "')\n\t\tWHERE resource_manager = 'test_custom_rmgrs';"
    )
    assert (
        result == expected
    ), "custom WAL resource manager has successfully written a WAL record"
    node.stop()
