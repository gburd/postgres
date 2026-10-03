# Copyright (c) 2017-2026, PostgreSQL Global Development Group

"""Port of src/tools/pg_bsd_indent/t/001_pg_bsd_indent.pl.

Runs pg_bsd_indent over each *.0 fixture in the tests/ directory with its
matching *.pro profile and checks the formatted output matches the recorded
*.0.stdout, accumulating any differences in a test.diffs file. Also checks
--version.
"""

import glob
import os
import shutil
import subprocess
import sys
import tempfile


def test_001_pg_bsd_indent(pg_bin):
    """pg_bsd_indent formats each fixture to its recorded expected output."""
    src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    workdir = tempfile.mkdtemp(prefix="bsdindent_")
    pg_bin.command_ok(["pg_bsd_indent", "--version"], "pg_bsd_indent --version")
    diffopts = ["-U3"]
    if sys.platform == "win32":
        diffopts.append("--strip-trailing-cr")
    for listfile in glob.glob(os.path.join(src_dir, "tests", "*.list")):
        shutil.copy(listfile, workdir)
    # pg_bsd_indent resolves the typedef *.list files (named in each *.pro) from
    # the current directory, so run from workdir as the Perl chdir's to tmp_check.
    prev_cwd = os.getcwd()
    os.chdir(workdir)
    try:
        _run_fixtures(pg_bin, src_dir, diffopts)
    finally:
        os.chdir(prev_cwd)


def _run_fixtures(pg_bin, src_dir, diffopts):
    diffs_file = "test.diffs"
    for test_src in sorted(glob.glob(os.path.join(src_dir, "tests", "*.0"))):
        test = os.path.basename(test_src)[:-2]
        out = test + ".out"
        pg_bin.command_ok(
            [
                "pg_bsd_indent",
                test_src,
                out,
                "-P{}".format(os.path.join(src_dir, "tests", test + ".pro")),
            ],
            "pg_bsd_indent succeeds on {}".format(test),
        )
        with open(diffs_file, "a", encoding="utf-8") as fh:
            rc = subprocess.run(
                ["diff", *diffopts, test_src + ".stdout", out],
                stdout=fh,
                check=False,
            ).returncode
        assert rc == 0, "pg_bsd_indent output matches for {}".format(test)
