# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_plan_advice/t/001_replan_regress.pl.

Runs the core regression suite against a server with test_plan_advice preloaded
(and feedback warnings on), so that plan advice is generated and replayed for
every regression query. The whole pg_regress parallel schedule must pass.
"""

import os
import subprocess

import pypg


def test_001_replan_regress(create_pg, tmp_check):
    """The core regression suite passes with test_plan_advice preloaded."""
    node = create_pg("main", start=False)
    node.append_conf(
        "shared_preload_libraries='test_plan_advice'\n"
        "wal_level=replica\n"
        "pg_plan_advice.always_explain_supplied_advice=false\n"
        "pg_plan_advice.feedback_warnings=true\n"
    )
    node.start()
    srcdir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
    )
    dlpath = os.path.dirname(os.environ["REGRESS_SHLIB"])
    outputdir = str(tmp_check)
    inputdir = os.path.join(srcdir, "src", "test", "regress")
    cmd = [
        os.environ["PG_REGRESS"],
        "--bindir=",
        "--dlpath=" + dlpath,
        "--host=" + str(node.host),
        "--port=" + str(node.port),
        "--schedule=" + os.path.join(inputdir, "parallel_schedule"),
        "--max-concurrent-tests=20",
        "--inputdir=" + inputdir,
        "--outputdir=" + outputdir,
    ]
    rc = subprocess.run(cmd, check=False).returncode
    if rc != 0:
        diffs = os.path.join(outputdir, "regression.diffs")
        if os.path.exists(diffs):
            print("=== dumping {} ===".format(diffs))
            print(pypg.slurp_file(diffs))
            print("=== EOF ===")
    assert rc == 0, "regression tests pass"
