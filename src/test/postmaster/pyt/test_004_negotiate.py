# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/postmaster/t/004_negotiate.pl.

On a raw connection the server rejects SSLRequest and GSSENCRequest packets with
'N' (no SSL/GSS support in this build). After both were tried, a further
SSLRequest must not re-enter SSL negotiation; the server logs the unsupported
protocol and stays alive, still accepting normal connections.
"""

import struct
import sys

import pytest


@pytest.mark.skipif(sys.platform == "win32", reason="requires raw_connect()")
def test_004_negotiate(create_pg):
    """Server rejects SSL/GSS requests and survives a bad negotiation attempt."""
    node = create_pg("main", start=False)
    node.append_conf("log_min_messages = debug2")
    node.append_conf("log_connections = 'receipt,authentication,authorization'")
    node.append_conf("trace_connection_negotiation=on")
    node.start()
    if not node.raw_connect_works():
        pytest.skip("this test requires working raw_connect()")
    sock = node.raw_connect()
    ssl_request = struct.pack(">IHH", 8, 1234, 5679)
    gss_request = struct.pack(">IHH", 8, 1234, 5680)
    sock.send(ssl_request)
    if sock.recv(1) != b"N":
        sock.close()
        pytest.skip("server accepted SSL; test requires SSL to be rejected")
    sock.send(gss_request)
    if sock.recv(1) != b"N":
        sock.close()
        pytest.skip("server accepted GSS; test requires GSS to be rejected")
    log_offset = node.current_log_position()
    sock.send(ssl_request)
    reply = sock.recv(1024)
    assert (
        reply != b"N"
    ), "server does not re-enter SSL negotiation after SSL+GSS were both tried"
    sock.close()
    node.wait_for_log(r"FATAL: .* unsupported frontend protocol 1234.5679", log_offset)
    assert node.safe_psql("select 1;") == "1", "server able to accept connection"
    assert node.is_alive(), "server still running after negotiation attempt"
    node.stop()
