# Copyright (c) 2022-2026, PostgreSQL Global Development Group

"""Shared fixtures for the ldap_password_func pytest suite.

Reuses the shared LDAP server infrastructure from ``pypg.ldapserver`` (the
Python twin of ``use lib ".../ldap"; use LdapServer;`` in the Perl original) by
re-exporting its ``ldap_server`` fixture. Because the helper now lives in the
shared pypg package, no sys.path manipulation is needed.
"""

# pylint: disable=unused-import
from pypg.ldapserver import (  # noqa: F401
    ldap_server_fixture as ldap_server,
)
