# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Shared fixtures for the oauth_validator pytest suite.

Exposes the mock OAuth authorization server (see pypg.oauthserver) as a
module-scoped ``webserver`` fixture, mirroring the OAuth::Server usage in the
Perl tests. The harness now lives in the shared pypg package, so it is a plain
import.
"""

import pytest

from pypg.oauthserver import OAuthServer


@pytest.fixture(scope="module")
def webserver():
    """Run the mock OAuth authorization server for the duration of a module."""
    server = OAuthServer()
    server.run()
    try:
        yield server
    finally:
        server.stop()
