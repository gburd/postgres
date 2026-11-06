# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/ldap/t/002_bindpasswd.pl.

Exercises LDAP search+bind authentication with the ldapbindpasswd option
against an LDAP server set up with 'users' (non-anonymous) auth: a wrong
ldapbindpasswd must fail, the correct one must succeed.
"""

import os

import pytest


def _test_access(node, role, expected_res, test_name):
    """Connect as role; assert success (0) or failure, mirroring test_access."""
    connstr = "user={}".format(role)
    if expected_res == 0:
        node.connect_ok(connstr, test_name)
    else:
        # No checks of the error message, only the status code.
        node.connect_fails(connstr, test_name)


def test_002_bindpasswd(create_pg, ldap_server):
    """search+bind with ldapbindpasswd: wrong fails, correct succeeds."""
    ldap_rootpw = "secret"
    ldap = ldap_server(ldap_rootpw, "users")  # no anonymous auth
    ldap.ldapadd_file(os.path.join(os.path.dirname(__file__), "..", "authdata.ldif"))
    ldap.ldapsetpw("uid=test1,dc=example,dc=net", "secret1")
    ldap.ldapsetpw("uid=test2,dc=example,dc=net", "secret2")

    ldap_server_host, ldap_port, ldap_basedn, ldap_rootdn = ldap.prop(
        "server", "port", "basedn", "rootdn"
    )

    node = create_pg("node", start=False)
    node.append_conf("log_connections = all\n")
    node.start()

    node.safe_psql("CREATE USER test0;")
    node.safe_psql("CREATE USER test1;")
    node.safe_psql('CREATE USER "test2@example.net";')

    old_pgpassword = os.environ.get("PGPASSWORD")
    try:
        # Note: this hba line preserves the deliberately malformed quoting from
        # the Perl original (a missing close-quote after $ldap_rootdn), which is
        # why authentication fails here.
        _write_hba(
            node,
            'local all all ldap ldapserver={} ldapport={} ldapbasedn="{}" '
            'ldapbinddn="{} ldapbindpasswd=wrong'.format(
                ldap_server_host, ldap_port, ldap_basedn, ldap_rootdn
            ),
        )
        node.restart()

        os.environ["PGPASSWORD"] = "secret1"
        _test_access(
            node,
            "test1",
            2,
            "search+bind authentication fails with wrong ldapbindpasswd",
        )

        _write_hba(
            node,
            'local all all ldap ldapserver={} ldapport={} ldapbasedn="{}" '
            'ldapbinddn="{}" ldapbindpasswd="{}"'.format(
                ldap_server_host, ldap_port, ldap_basedn, ldap_rootdn, ldap_rootpw
            ),
        )
        node.restart()

        _test_access(
            node,
            "test1",
            0,
            "search+bind authentication succeeds with ldapbindpasswd",
        )
    finally:
        if old_pgpassword is None:
            os.environ.pop("PGPASSWORD", None)
        else:
            os.environ["PGPASSWORD"] = old_pgpassword


def _write_hba(node, line):
    """Replace pg_hba.conf with a single line (mirrors unlink + append_conf)."""
    hba = node.datadir / "pg_hba.conf"
    hba.unlink()
    node.append_conf(line, filename="pg_hba.conf")


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
