# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/libpq_pipeline/t/001_libpq_pipeline.pl.

Runs every sub-test reported by the libpq_pipeline C program against a live
server using the latest protocol version, and for the trace-producing tests
compares the emitted libpq trace against the expected trace checked into the
source tree. Also exercises query cancellation over protocol 3.0.
"""

import os

import pypg

_NUMROWS = 700
_CMPTRACE = {
    "simple_pipeline",
    "nosync",
    "multi_pipelines",
    "prepared",
    "singlerow",
    "pipeline_abort",
    "pipeline_idle",
    "transaction",
    "disallowed_in_pipeline",
}
_TRACES_DIR = os.path.join(os.path.dirname(__file__), "..", "traces")


def test_001_libpq_pipeline(create_pg, pg_bin, tmp_check):
    """Each libpq_pipeline sub-test passes; trace tests match expected traces."""
    node = create_pg("main")
    result = pg_bin.run_command(["libpq_pipeline", "tests"])
    assert result.stderr == "", "oops: {}".format(result.stderr)
    tests = result.stdout.split()
    out_traces = tmp_check / "traces"
    out_traces.mkdir(exist_ok=True)
    connstr = node.connstr("postgres")
    for testname in tests:
        extraargs = ["-r", str(_NUMROWS)]
        cmptrace = testname in _CMPTRACE
        traceout = out_traces / "{}.trace".format(testname)
        if cmptrace:
            extraargs += ["-t", str(traceout)]
        node.command_ok(
            ["libpq_pipeline"]
            + extraargs
            + [testname, connstr + " max_protocol_version=latest"],
            "libpq_pipeline {}".format(testname),
        )
        if cmptrace:
            expected = pypg.slurp_file(
                os.path.join(_TRACES_DIR, "{}.trace".format(testname))
            )
            if expected == "":
                continue
            actual = pypg.slurp_file(str(traceout))
            if actual == "":
                continue
            assert actual == expected, "{} trace match".format(testname)
    node.command_ok(
        ["libpq_pipeline", "cancel", connstr + " max_protocol_version=3.0"],
        "libpq_pipeline cancel with protocol 3.0",
    )
    node.stop("fast")
