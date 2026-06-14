# Copyright (c) 2017-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/012_subtransactions.pl.

Hot-standby handling of large subtransaction trees and prepared transactions
across restarts and promotions: nextXid is advanced correctly past prepared
subtransactions, a committed 127-deep subxid tree is visible on the standby and
survives promotion, and a PGPROC_MAX_CACHED_SUBXIDS+ prepared transaction is
restored and resolvable (commit/rollback) on a promoted standby.
"""

_FUNC = (
    "CREATE OR REPLACE FUNCTION hs_subxids (n integer)\n"
    "RETURNS void\n"
    "LANGUAGE plpgsql\n"
    "AS $$\n"
    "BEGIN\n"
    "    IF n <= 0 THEN RETURN; END IF;\n"
    "    INSERT INTO t_012_tbl VALUES (n);\n"
    "    PERFORM hs_subxids(n - 1);\n"
    "    RETURN;\n"
    "EXCEPTION WHEN raise_exception THEN NULL; END;\n"
    "$$;"
)
_SUM = "SELECT coalesce(sum(id),-1) FROM t_012_tbl"


def test_012_subtransactions(create_pg):
    """Subtransaction/prepared-xact visibility across restart, promotion, swap."""
    primary = create_pg("primary", allows_streaming=True, start=False)
    primary.append_conf(
        "\n\tmax_prepared_transactions = 10\n\tlog_checkpoints = true\n"
    )
    primary.start()
    primary.backup("primary_backup")
    primary.psql_capture("CREATE TABLE t_012_tbl (id int)")
    standby = create_pg(
        "standby",
        from_backup=(primary, "primary_backup"),
        has_streaming=True,
        start=False,
    )
    standby.start()
    primary.append_conf("\n\tsynchronous_standby_names = '*'\n")
    primary.psql_capture("SELECT pg_reload_conf()")
    primary.psql_capture(
        "\n\tBEGIN;\n\tDELETE FROM t_012_tbl;\n\tINSERT INTO t_012_tbl VALUES (43);\n"
        + "".join(
            "\tSAVEPOINT s{n};\n\tINSERT INTO t_012_tbl VALUES (43);\n".format(n=n)
            for n in range(1, 6)
        )
        + "\tPREPARE TRANSACTION 'xact_012_1';\n\tCHECKPOINT;"
    )
    primary.stop()
    primary.start()
    primary.psql_capture(
        "\n\tBEGIN;\n\tINSERT INTO t_012_tbl VALUES (142);\n\tROLLBACK;\n"
        "\tCOMMIT PREPARED 'xact_012_1';"
    )
    assert (
        primary.psql_capture("SELECT count(*) FROM t_012_tbl").stdout == "6"
    ), "Check nextXid handling for prepared subtransactions"
    primary.psql_capture("DELETE FROM t_012_tbl")
    primary.psql_capture(_FUNC)
    primary.psql_capture("\n\tBEGIN;\n\tSELECT hs_subxids(127);\n\tCOMMIT;")
    primary.wait_for_catchup(standby)
    assert standby.psql_capture(_SUM).stdout == "8128", "Visible"
    primary.stop()
    standby.promote()
    assert standby.psql_capture(_SUM).stdout == "8128", "Visible"
    primary, standby = standby, primary
    standby.enable_streaming(primary)
    standby.start()
    assert standby.psql_capture(_SUM).stdout == "8128", "Visible"
    primary.psql_capture("DELETE FROM t_012_tbl")
    primary.psql_capture(_FUNC)
    primary.psql_capture(
        "\n\tBEGIN;\n\tSELECT hs_subxids(127);\n\tPREPARE TRANSACTION 'xact_012_1';"
    )
    primary.wait_for_catchup(standby)
    assert standby.psql_capture(_SUM).stdout == "-1", "Not visible"
    primary.stop()
    standby.promote()
    assert standby.psql_capture(_SUM).stdout == "-1", "Not visible"
    primary, standby = standby, primary
    standby.enable_streaming(primary)
    standby.start()
    assert primary.psql_capture("COMMIT PREPARED 'xact_012_1'").rc == 0, (
        "Restore of PGPROC_MAX_CACHED_SUBXIDS+ prepared transaction on promoted "
        "standby"
    )
    assert primary.psql_capture(_SUM).stdout == "8128", "Visible"
    primary.psql_capture("DELETE FROM t_012_tbl")
    primary.psql_capture(
        "\n\tBEGIN;\n\tSELECT hs_subxids(201);\n\tPREPARE TRANSACTION 'xact_012_1';"
    )
    primary.wait_for_catchup(standby)
    assert standby.psql_capture(_SUM).stdout == "-1", "Not visible"
    primary.stop()
    standby.promote()
    assert standby.psql_capture(_SUM).stdout == "-1", "Not visible"
    primary, standby = standby, primary
    standby.enable_streaming(primary)
    standby.start()
    assert primary.psql_capture("ROLLBACK PREPARED 'xact_012_1'").rc == 0, (
        "Rollback of PGPROC_MAX_CACHED_SUBXIDS+ prepared transaction on promoted "
        "standby"
    )
    assert primary.psql_capture(_SUM).stdout == "-1", "Not visible"
    primary.stop()
    standby.stop()
