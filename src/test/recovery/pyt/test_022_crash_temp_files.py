# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/022_crash_temp_files.pl.

When a backend is SIGKILLed mid-INSERT (after spilling a temp file) and the
server crash-restarts, remove_temp_files_after_crash governs cleanup: with it
on, base/pgsql_tmp is empty after recovery; with it off, the orphaned temp file
survives the crash recovery and is only cleared by a later clean restart. A
second session blocked on a unique-index lock is used to guarantee the victim
backend has spilled its temp file before the kill.
"""

import sys

import pytest

import pypg

_CRASH_ERR = (
    r"WARNING:  terminating connection because of crash of another server "
    r"process|server closed the connection unexpectedly|connection to server "
    r"was lost|could not send data to server"
)


def _spill_and_kill(node, killme, killme2, table):
    """Set up the two sessions, spill a temp file, SIGKILL the victim."""
    pid = killme.query_until(r"[0-9]+[\r\n]", "SELECT pg_backend_pid();\n").strip()
    killme2.query_until(
        r"insert-tuple-to-lock-next-insert",
        "BEGIN;\nINSERT INTO {} (a) VALUES(1);\n"
        "SELECT $$insert-tuple-to-lock-next-insert$$;\n".format(table),
    )
    killme.query_until(
        r"in-progress-before-sigkill",
        "BEGIN;\nSELECT $$in-progress-before-sigkill$$;\n"
        "INSERT INTO {} (a) SELECT i FROM generate_series(1, 5000) s(i);\n".format(
            table
        ),
    )
    killme2.query_until(
        r"insert-tuple-lock-waiting",
        "DO $c$\nDECLARE\n  c INT;\nBEGIN\n  LOOP\n"
        "    SELECT COUNT(*) INTO c FROM pg_locks WHERE pid = {} "
        "AND NOT granted;\n"
        "    IF c > 0 THEN\n      EXIT;\n    END IF;\n  END LOOP;\nEND; $c$;\n"
        "SELECT $$insert-tuple-lock-waiting$$;\n".format(pid),
    )
    node.pg_ctl("kill", "KILL", pid)
    killme.quit()
    killme2.wait_for_stderr(_CRASH_ERR, "SELECT pg_sleep({});\n".format(_timeout()))
    killme2.quit()
    assert node.poll_query_until("", ""), "server crash-recovered"


def _timeout():
    return pypg.test_timeout_default()


@pytest.mark.skipif(sys.platform == "win32", reason="tests hang on Windows")
def test_022_crash_temp_files(create_pg):
    """remove_temp_files_after_crash controls temp-file cleanup across a crash."""
    node = create_pg("node_crash")
    node.safe_psql(
        "ALTER SYSTEM SET remove_temp_files_after_crash = on;\n"
        "ALTER SYSTEM SET log_connections = receipt;\n"
        "ALTER SYSTEM SET work_mem = '64kB';\n"
        "ALTER SYSTEM SET restart_after_crash = on;\n"
        "SELECT pg_reload_conf();"
    )
    node.safe_psql("CREATE TABLE tab_crash (a integer UNIQUE);")
    killme = node.background_psql("postgres", on_error_stop=True)
    killme2 = node.background_psql("postgres", on_error_stop=True)
    _spill_and_kill(node, killme, killme2, "tab_crash")
    assert (
        node.safe_psql("SELECT COUNT(1) FROM pg_ls_dir($$base/pgsql_tmp$$)") == "0"
    ), "no temporary files"
    node.safe_psql(
        "ALTER SYSTEM SET remove_temp_files_after_crash = off;\n"
        "SELECT pg_reload_conf();"
    )
    killme = node.background_psql("postgres", on_error_stop=True)
    killme2 = node.background_psql("postgres", on_error_stop=True)
    _spill_and_kill(node, killme, killme2, "tab_crash")
    assert (
        node.safe_psql("SELECT COUNT(1) FROM pg_ls_dir($$base/pgsql_tmp$$)") == "1"
    ), "one temporary file"
    node.restart()
    assert (
        node.safe_psql("SELECT COUNT(1) FROM pg_ls_dir($$base/pgsql_tmp$$)") == "0"
    ), "temporary file was removed"
    node.stop()
