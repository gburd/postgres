# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Shared fixtures for the LDAP pytest suite.

Imports the ``ldap_server`` factory fixture from the shared ``pypg.ldapserver``
helper so it is discovered by the tests in this directory. The fixture is the
Python twin of using ``LdapServer->new`` in the Perl TAP tests.
"""

# pylint: disable=unused-import
from pypg.ldapserver import (  # noqa: F401
    ldap_server_fixture as ldap_server,
)
