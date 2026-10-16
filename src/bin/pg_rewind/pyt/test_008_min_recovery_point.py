# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_rewind/t/008_min_recovery_point.pl.

A three-node chain (node_1, node_2, node_3 all from one backup) is reconfigured
across two promotions so that node_2 diverges from the latest primary (node_1).
pg_rewind --source-server rewinds node_2 onto node_1; afterward node_2 has
node_1's history (table foo rows kept) and the divergent change on node_3 (the
extra bar row) is gone.
"""

import os
import shutil
import tempfile


def test_008_min_recovery_point(create_pg):
    """pg_rewind rewinds a diverged standby across promotions via --source-server."""
    tmp_folder = tempfile.mkdtemp(prefix="minrp_")
    node1 = create_pg("node_1", allows_streaming=True, start=False)
    node1.append_conf("\nwal_keep_size='100 MB'\n")
    node1.start()
    node1.safe_psql("CREATE TABLE public.foo (t TEXT)")
    node1.safe_psql("CREATE TABLE public.bar (t TEXT)")
    node1.safe_psql("INSERT INTO public.bar VALUES ('in both')")
    node1.backup("my_backup")
    node2 = create_pg(
        "node_2", from_backup=(node1, "my_backup"), has_streaming=True, start=False
    )
    node2.start()
    node3 = create_pg(
        "node_3", from_backup=(node1, "my_backup"), has_streaming=True, start=False
    )
    node3.start()
    node1.wait_for_catchup("node_3")
    node1.stop("fast")
    node3.promote()
    node3_connstr = node3.connstr()
    node1.append_conf("\nprimary_conninfo='{}'\n".format(node3_connstr))
    node1.set_standby_mode()
    node1.start()
    node2.append_conf("\nprimary_conninfo='{}'\n".format(node3_connstr))
    node2.restart()
    node3.wait_for_catchup("node_1")
    node1.promote()
    node1.safe_psql("INSERT INTO public.foo (t) VALUES ('keep this')")
    node3.safe_psql("INSERT INTO public.bar (t) VALUES ('rewind this')")
    node1.safe_psql("INSERT INTO public.foo (t) VALUES ('and this')")
    node1.safe_psql("INSERT INTO public.foo (t) VALUES ('and this too')")
    assert node2.poll_query_until("SELECT COUNT(*) > 1 FROM public.bar", "t")
    node2.stop("fast")
    node3.stop("fast")
    node2_pgdata = str(node2.datadir)
    saved_conf = os.path.join(tmp_folder, "node_2-postgresql.conf.tmp")
    shutil.copy(os.path.join(node2_pgdata, "postgresql.conf"), saved_conf)
    node2.command_ok(
        [
            "pg_rewind",
            "--source-server",
            node1.connstr(),
            "--target-pgdata",
            node2_pgdata,
            "--debug",
        ],
        "run pg_rewind",
    )
    shutil.move(saved_conf, os.path.join(node2_pgdata, "postgresql.conf"))
    node2.start()
    assert node2.safe_psql("SELECT * FROM public.foo") == (
        "keep this\nand this\nand this too"
    ), "table foo after rewind"
    assert (
        node2.safe_psql("SELECT * FROM public.bar") == "in both"
    ), "table bar after rewind"
