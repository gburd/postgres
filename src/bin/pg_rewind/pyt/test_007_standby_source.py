# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_rewind/t/007_standby_source.pl.

pg_rewind can use a standby (here node_b, a cascading source) as its source
server. A chain A->B->C is built; C is promoted and diverges from A (which keeps
streaming to B). C is then rewound from B (--source-server --write-recovery-conf),
so C rejoins A's history and continues replaying A's later changes through B.
"""

import os
import shutil


def test_007_standby_source(rewind_test, create_pg, tmp_path):
    """pg_rewind uses a (cascading) standby as its source server."""
    tmp_folder = str(tmp_path)
    rewind_test.setup_cluster("a")
    rewind_test.start_primary()
    node_a = rewind_test.primary
    node_a.safe_psql("CREATE TABLE tbl1 (d text)")
    node_a.safe_psql("INSERT INTO tbl1 VALUES ('in A')")
    rewind_test.primary_psql("CHECKPOINT")
    node_a.backup("my_backup")
    node_b = create_pg(
        "node_b", from_backup=(node_a, "my_backup"), has_streaming=True, start=False
    )
    node_b.set_standby_mode()
    node_b.start()
    node_b.backup("my_backup")
    node_c = create_pg(
        "node_c", from_backup=(node_b, "my_backup"), has_streaming=True, start=False
    )
    node_c.set_standby_mode()
    node_c.start()
    node_a.safe_psql("INSERT INTO tbl1 values ('in A, before promotion')")
    node_a.safe_psql("CHECKPOINT")
    lsn = node_a.lsn("write")
    node_a.wait_for_catchup("node_b", "write", lsn)
    node_b.wait_for_catchup("node_c", "write", lsn)
    node_c.promote()
    node_a.safe_psql("INSERT INTO tbl1 VALUES ('in A, after C was promoted')")
    node_a.wait_for_catchup("node_b")
    node_c.safe_psql("INSERT INTO tbl1 VALUES ('in C, after C was promoted')")
    node_c_pgdata = str(node_c.datadir)
    node_c.stop("fast")
    saved_conf = os.path.join(tmp_folder, "node_c-postgresql.conf.tmp")
    shutil.copy(os.path.join(node_c_pgdata, "postgresql.conf"), saved_conf)
    node_c.bin.command_ok(
        [
            "pg_rewind",
            "--debug",
            "--source-server",
            node_b.connstr("postgres"),
            "--target-pgdata",
            node_c_pgdata,
            "--no-sync",
            "--write-recovery-conf",
        ],
        "pg_rewind remote",
        extra_env={"PGAPPNAME": ""},
    )
    shutil.move(saved_conf, os.path.join(node_c_pgdata, "postgresql.conf"))
    node_c.start()
    rewind_test.primary = node_c
    rewind_test.check_query(
        "SELECT * FROM tbl1",
        "in A\nin A, before promotion\nin A, after C was promoted",
        "table content after rewind",
    )
    node_a.safe_psql("INSERT INTO tbl1 values ('in A, after rewind')")
    node_b.wait_for_replay_catchup("node_c", node_a)
    rewind_test.check_query(
        "SELECT * FROM tbl1",
        "in A\nin A, before promotion\nin A, after C was promoted\n"
        "in A, after rewind",
        "table content after rewind and insert",
    )
    node_a.teardown_node()
    node_b.teardown_node()
    node_c.teardown_node()
