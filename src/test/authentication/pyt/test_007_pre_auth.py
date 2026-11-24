# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/authentication/t/007_pre_auth.pl.

Connections that are still authenticating are visible in pg_stat_activity with
state 'starting' and wait_event 'init-pre-auth'; once authentication completes
they reach 'idle'. Uses an injection point to hold a backend in pre-auth.
Requires an injection-points build with the injection_points extension.
"""

import os
import time

import pytest


def test_007_pre_auth(create_pg):
    """Authenticating backends appear in pg_stat_activity, then reach idle."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    node = create_pg("primary", start=False)
    node.append_conf("\nlog_connections = 'receipt,authentication'\n")
    node.start()
    if not node.check_extension("injection_points"):
        pytest.skip("Extension injection_points not installed")
    node.safe_psql("CREATE EXTENSION injection_points")
    psql = node.background_psql("postgres")
    psql.query_safe("SELECT injection_points_attach('init-pre-auth', 'wait')")
    conn = node.background_psql("postgres", wait=False)
    pid = ""
    while pid == "":
        pid = psql.query(
            "SELECT pid FROM pg_stat_activity\n"
            "  WHERE backend_type = 'client backend'\n"
            "    AND state = 'starting'\n"
            "    AND wait_event = 'init-pre-auth';"
        )
        if pid == "":
            time.sleep(0.1)
    psql.query_safe("SELECT injection_points_wakeup('init-pre-auth');")
    conn.wait_connect()
    state = ""
    while state != "idle":
        state = psql.query(
            "SELECT state FROM pg_stat_activity WHERE pid = {};".format(pid)
        )
        if state != "idle":
            time.sleep(0.1)
    psql.query_safe("SELECT injection_points_detach('init-pre-auth');")
    psql.quit()
    conn.quit()
