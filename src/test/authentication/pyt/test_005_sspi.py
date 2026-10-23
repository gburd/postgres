# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/authentication/t/005_sspi.pl.

Windows SSPI authentication: require_auth=sspi connects, while require_auth=!sspi
and require_auth=scram-sha-256 are rejected because the server requests SSPI.
Requires Windows without PG_TEST_USE_UNIX_SOCKETS; always skips elsewhere.
"""

import os
import platform

import pytest


def test_005_sspi(create_pg):
    """SSPI auth: required works; forbidden and SCRAM-required fail."""
    windows_os = platform.system() == "Windows"
    use_unix_sockets = bool(os.environ.get("PG_TEST_USE_UNIX_SOCKETS"))
    if not windows_os or use_unix_sockets:
        pytest.skip("SSPI tests require Windows (without PG_TEST_USE_UNIX_SOCKETS)")
    node = create_pg("primary", start=False)
    node.append_conf("log_connections = authentication\n")
    node.start()
    huge_pages_status = node.safe_psql("SHOW huge_pages_status;")
    assert huge_pages_status != "unknown", "check huge_pages_status"
    node.connect_ok(
        "require_auth=sspi",
        "SSPI authentication required, works with SSPI auth",
    )
    node.connect_fails(
        "require_auth=!sspi",
        "SSPI authentication forbidden, fails with SSPI auth",
        expected_stderr=r'authentication method requirement "!sspi" failed: server requested SSPI authentication',
    )
    node.connect_fails(
        "require_auth=scram-sha-256",
        "SCRAM authentication required, fails with SSPI auth",
        expected_stderr=r'authentication method requirement "scram-sha-256" failed: server requested SSPI authentication',
    )
