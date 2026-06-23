# Copyright (c) 2022-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/ldap_password_func/t/001_mutated_bindpasswd.pl.

Verifies the ldap_password_func contrib module, which mutates the configured
ldapbindpasswd by rot13. With the module preloaded: a wrong bindpasswd fails,
the clear-text password fails (because the hook rot13's it before the bind),
and the rot13'd password succeeds (the hook rot13's it back to clear text).
Reuses the LDAP server infrastructure from src/test/ldap/pyt.
"""

import os


def _test_access(node, role, expected_res, test_name):
    """Connect as role; assert success (0) or failure, mirroring test_access."""
    connstr = "user={}".format(role)
    if expected_res == 0:
        node.connect_ok(connstr, test_name)
    else:
        # No checks of the error message, only the status code.
        node.connect_fails(connstr, test_name)


def _write_hba(node, line):
    """Replace pg_hba.conf with a single line (mirrors unlink + append_conf)."""
    hba = node.datadir / "pg_hba.conf"
    hba.unlink()
    node.append_conf(line, filename="pg_hba.conf")


def test_001_mutated_bindpasswd(create_pg, ldap_server):
    """ldap_password_func rot13's ldapbindpasswd: only the rot13'd value works."""
    clear_ldap_rootpw = "FooBaR1"
    rot13_ldap_rootpw = "SbbOnE1"

    ldap = ldap_server(clear_ldap_rootpw, "users")  # no anonymous auth
    authdata = os.path.normpath(
        os.path.join(
            os.path.dirname(__file__), "..", "..", "..", "ldap", "authdata.ldif"
        )
    )
    ldap.ldapadd_file(authdata)
    ldap.ldapsetpw("uid=test1,dc=example,dc=net", "secret1")

    ldap_server_host, ldap_port, ldap_basedn, ldap_rootdn = ldap.prop(
        "server", "port", "basedn", "rootdn"
    )

    node = create_pg("node", start=False)
    node.append_conf("log_connections = 'receipt,authentication,authorization'\n")
    node.append_conf("shared_preload_libraries = 'ldap_password_func'")
    node.start()

    node.safe_psql("CREATE USER test1;")

    old_pgpassword = os.environ.get("PGPASSWORD")
    os.environ["PGPASSWORD"] = "secret1"
    try:
        _write_hba(
            node,
            "local all all ldap ldapserver={} ldapport={} "
            'ldapbasedn="{}" ldapbinddn="{}" ldapbindpasswd=wrong'.format(
                ldap_server_host, ldap_port, ldap_basedn, ldap_rootdn
            ),
        )
        node.restart()
        _test_access(
            node,
            "test1",
            2,
            "search+bind authentication fails with wrong ldapbindpasswd",
        )

        _write_hba(
            node,
            "local all all ldap ldapserver={} ldapport={} "
            'ldapbasedn="{}" ldapbinddn="{}" ldapbindpasswd="{}"'.format(
                ldap_server_host, ldap_port, ldap_basedn, ldap_rootdn, clear_ldap_rootpw
            ),
        )
        node.restart()
        _test_access(
            node, "test1", 2, "search+bind authentication fails with clear password"
        )

        _write_hba(
            node,
            "local all all ldap ldapserver={} ldapport={} "
            'ldapbasedn="{}" ldapbinddn="{}" ldapbindpasswd="{}"'.format(
                ldap_server_host, ldap_port, ldap_basedn, ldap_rootdn, rot13_ldap_rootpw
            ),
        )
        node.restart()
        _test_access(
            node,
            "test1",
            0,
            "search+bind authentication succeeds with rot13ed password",
        )
    finally:
        if old_pgpassword is None:
            os.environ.pop("PGPASSWORD", None)
        else:
            os.environ["PGPASSWORD"] = old_pgpassword
