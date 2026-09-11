# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of contrib/sepgsql/t/001_sepgsql.pl.

Runs the sepgsql label/dml/ddl/alter/misc(/truncate) regression suite, but only
when the platform is a properly configured SELinux host: PG_TEST_EXTRA must
include 'sepgsql', the SELinux tools (matchpathcon/runcon/sestatus) must be
present, SELinux must be enforcing in the unconfined_t domain, and the
sepgsql-regtest policy module with its booleans must be installed. On systems
that do not meet these conditions (e.g. non-SELinux Linux) the test skips, just
as the Perl original bails out.
"""

import os
import subprocess
import sys

import pytest


def _cmd_ok(argv):
    try:
        return (
            subprocess.run(
                argv,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            ).returncode
            == 0
        )
    except OSError:
        return False


def _sestatus_field(label):
    try:
        out = subprocess.run(
            ["sestatus"],
            stdout=subprocess.PIPE,
            encoding="utf-8",
            env={**os.environ, "LANG": "C"},
            check=False,
        ).stdout
    except OSError:
        return ""
    for line in out.splitlines():
        if line.startswith(label):
            return line.split(":", 1)[1].strip()
    return ""


def _require_selinux():
    if "sepgsql" not in os.environ.get("PG_TEST_EXTRA", "").split():
        pytest.skip("Potentially unsafe test sepgsql not enabled in PG_TEST_EXTRA")
    for tool in (["matchpathcon", "-n", "."], ["runcon", "--help"], ["sestatus"]):
        if not _cmd_ok(tool):
            pytest.skip("{} (SELinux tooling) not available".format(tool[0]))
    try:
        domain = subprocess.run(
            ["id", "-Z"], stdout=subprocess.PIPE, encoding="utf-8", check=False
        ).stdout.split(":")
    except OSError:
        pytest.skip("id -Z (SELinux) not available")
    if len(domain) < 3 or domain[2] != "unconfined_t":
        pytest.skip("tests must run from the unconfined_t SELinux domain")
    if _sestatus_field("Current mode:") != "enforcing":
        pytest.skip("SELinux must be enabled and in enforcing mode")
    mnt = _sestatus_field("SELinuxfs mount:")
    if not mnt or not os.path.exists(
        os.path.join(mnt, "booleans", "sepgsql_regression_test_mode")
    ):
        pytest.skip("the sepgsql-regtest policy module is not installed")
    for policy in ("sepgsql_regression_test_mode", "sepgsql_enable_users_ddl"):
        out = subprocess.run(
            ["getsebool", policy],
            stdout=subprocess.PIPE,
            encoding="utf-8",
            check=False,
        ).stdout.split()
        if len(out) < 3 or out[2] != "on":
            pytest.skip("SELinux boolean {} must be on".format(policy))


@pytest.mark.skipif(sys.platform != "linux", reason="sepgsql is Linux/SELinux only")
def test_001_sepgsql(create_pg):
    """Run the sepgsql regression suite on a configured SELinux host."""
    _require_selinux()
    node = create_pg("test", start=False)
    node.append_conf("log_statement=none")
    sepgsql_sql = os.path.join(os.environ["share_contrib_dir"], "sepgsql.sql")
    with open(sepgsql_sql, encoding="utf-8") as fh:
        result = subprocess.run(
            [
                os.path.join(str(node.bin_dir), "postgres"),
                "--single",
                "-F",
                "-c",
                "exit_on_error=true",
                "-D",
                str(node.datadir),
                "template0",
            ],
            stdin=fh,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=node.connenv,
            check=False,
        )
    assert result.returncode == 0, "sepgsql installation script"
    node.append_conf("shared_preload_libraries=sepgsql")
    node.start()
    tests = ["label", "dml", "ddl", "alter", "misc"]
    if os.path.isfile("/sys/fs/selinux/class/db_table/perms/truncate"):
        tests.append("truncate")
    node.command_ok(
        [
            os.environ["PG_REGRESS"],
            "--bindir",
            "",
            "--inputdir",
            ".",
            "--launcher",
            "./launcher",
            *tests,
        ],
        "sepgsql tests",
    )
