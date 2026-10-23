# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/postmaster/t/003_start_stop.pl.

With a tiny connection budget, opening dead-end raw connections (each probing
SSL negotiation, which the postmaster rejects with 'N' once the backend is
launched) eventually exhausts the slots, so a real connection is rejected with
"too many clients already". After a fast stop and restart, normal connections
work again.
"""

import struct
import sys

import pytest

import pypg


@pytest.mark.skipif(sys.platform == "win32", reason="requires raw_connect()")
def test_003_start_stop(create_pg):
    """Dead-end connections exhaust slots; restart restores connectivity."""
    auth_timeout = max(pypg.test_timeout_default(), 600)
    node = create_pg("main", start=False)
    for line in (
        "max_connections = 5",
        "max_wal_senders = 0",
        "autovacuum_max_workers = 1",
        "max_worker_processes = 1",
        "log_connections = 'receipt,authentication,authorization'",
        "log_min_messages = debug2",
        "authentication_timeout = '{} s'".format(auth_timeout),
        "trace_connection_negotiation=on",
    ):
        node.append_conf(line)
    node.start()
    if not node.raw_connect_works():
        pytest.skip("this test requires working raw_connect()")
    node.restart()
    negotiate_ssl = struct.pack(">IHH", 8, 1234, 5679)
    raw_connections = []
    for i in range(21):
        sock = node.raw_connect()
        # Probe SSL negotiation before opening the next connection: the server
        # rejects it with 'N', proving the backend was launched and we can open
        # another connection reliably.
        sock.send(negotiate_ssl)
        assert sock.recv(1) == b"N", "dead-end connection {}".format(i)
        raw_connections.append(sock)
    node.connect_fails(
        "dbname=postgres user=invalid_user",
        "connection is rejected when all slots are in use",
        expected_stderr=r"FATAL:  sorry, too many clients already",
    )
    extra = node.raw_connect()
    node.stop("fast")
    node.start()
    node.connect_ok("dbname=postgres", "works after restart")
    for sock in raw_connections:
        sock.close()
    extra.close()
