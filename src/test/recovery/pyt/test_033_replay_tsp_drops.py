# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/033_replay_tsp_drops.pl.

Replay of tablespace/database creation and drop, including replaying CREATE
DATABASE WAL records against already-removed directories.
"""

import shutil
import time

_WORKLOAD = """\
CREATE DATABASE dropme_db1 WITH TABLESPACE dropme_ts1 STRATEGY={strategy};
CREATE TABLE t (a int) TABLESPACE dropme_ts2;
CREATE DATABASE dropme_db2 WITH TABLESPACE dropme_ts2 STRATEGY={strategy};
CREATE DATABASE moveme_db TABLESPACE source_ts STRATEGY={strategy};
ALTER DATABASE moveme_db SET TABLESPACE target_ts;
CREATE DATABASE newdb TEMPLATE template_db STRATEGY={strategy};
ALTER DATABASE template_db IS_TEMPLATE = false;
DROP DATABASE dropme_db1;
DROP TABLE t;
DROP DATABASE dropme_db2; DROP TABLESPACE dropme_ts2;
DROP TABLESPACE source_ts;
DROP DATABASE template_db;
"""


def _test_tablespace(create_pg, strategy):
    primary = create_pg("primary1_" + strategy, allows_streaming=True)
    primary.safe_psql(
        "SET allow_in_place_tablespaces=on;\n"
        "CREATE TABLESPACE dropme_ts1 LOCATION '';\n"
        "CREATE TABLESPACE dropme_ts2 LOCATION '';\n"
        "CREATE TABLESPACE source_ts  LOCATION '';\n"
        "CREATE TABLESPACE target_ts  LOCATION '';\n"
        "CREATE DATABASE template_db IS_TEMPLATE = true;\n"
        "SELECT pg_create_physical_replication_slot('slot', true);"
    )
    primary.backup("my_backup")

    standby = create_pg(
        "standby2_" + strategy,
        from_backup=(primary, "my_backup"),
        has_streaming=True,
        start=False,
    )
    standby.append_conf("allow_in_place_tablespaces = on")
    standby.append_conf("primary_slot_name = slot")
    standby.start()
    primary.wait_for_catchup(standby, "write")

    # Immediate shutdown right after CREATE/DROP DATABASE/TABLESPACE makes
    # CREATE DATABASE WAL records apply to already-removed directories.
    primary.safe_psql(_WORKLOAD.format(strategy=strategy))
    primary.wait_for_catchup(standby, "write")

    standby.safe_psql("ALTER SYSTEM SET log_min_messages TO debug1;")
    standby.stop("immediate")
    standby.start()  # standby node must start for this strategy
    standby.stop("immediate")


def test_replay_tsp_drops(create_pg):
    """Replaying CREATE/DROP database/tablespace tolerates missing dirs."""
    _test_tablespace(create_pg, "FILE_COPY")
    _test_tablespace(create_pg, "WAL_LOG")

    # A missing tablespace directory during CREATE DATABASE replay must be
    # detected once the standby is consistent (FILE_COPY only).
    primary = create_pg("primary2", allows_streaming=True)
    primary.safe_psql(
        "SET allow_in_place_tablespaces=on;\nCREATE TABLESPACE ts1 LOCATION ''"
    )
    primary.safe_psql("CREATE DATABASE db1 WITH TABLESPACE ts1 STRATEGY=FILE_COPY")
    primary.backup("my_backup")

    standby = create_pg(
        "standby3", from_backup=(primary, "my_backup"), has_streaming=True, start=False
    )
    standby.append_conf("allow_in_place_tablespaces = on")
    standby.start()
    standby.poll_query_until("SELECT 1", expected="1")

    # Remove the standby's tablespace directory so it is missing on replay.
    tspoid = standby.safe_psql("SELECT oid FROM pg_tablespace WHERE spcname = 'ts1';")
    shutil.rmtree(standby.datadir / "pg_tblspc" / tspoid)

    logstart = standby.current_log_position()
    primary.safe_psql(
        "CREATE TABLE should_not_replay_insertion(a int);\n"
        "CREATE DATABASE db2 WITH TABLESPACE ts1 STRATEGY=FILE_COPY;\n"
        "INSERT INTO should_not_replay_insertion VALUES (1);"
    )

    pattern = r"WARNING: ( [A-Z0-9]+:)? creating missing directory: pg_tblspc/"
    detected = False
    for _ in range(10 * 180):
        if standby.log_matches(pattern, logstart):
            detected = True
            break
        time.sleep(0.1)
    assert detected, "invalid directory creation is detected"
