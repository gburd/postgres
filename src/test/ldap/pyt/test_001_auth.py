# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/ldap/t/001_auth.pl.

Exhaustive test of LDAP authentication via pg_hba.conf: simple bind,
search+bind, multiple servers, LDAP URLs, search filters (direct and embedded
in URLs), a diagnostic-message failure, and the TLS variants (StartTLS, LDAPS,
LDAPS via URL, and the rejected StartTLS+LDAPS combination). The LDAP server is
set up with anonymous auth.
"""

import contextlib
import os

# Patterns asserting an authenticated identity in the server log.
_AUTH_TEST1 = (
    r'connection authenticated: identity="uid=test1,dc=example,dc=net" method=ldap'
)
_AUTH_TEST2 = (
    r'connection authenticated: identity="uid=test2,dc=example,dc=net" method=ldap'
)
_NOT_AUTHENTICATED = r"connection authenticated:"


@contextlib.contextmanager
def _pgpassword(value):
    """Temporarily set PGPASSWORD (mirrors the Perl ``$ENV{PGPASSWORD}``)."""
    saved = os.environ.get("PGPASSWORD")
    os.environ["PGPASSWORD"] = value
    try:
        yield
    finally:
        if saved is None:
            os.environ.pop("PGPASSWORD", None)
        else:
            os.environ["PGPASSWORD"] = saved


def _test_access(node, role, expected_res, test_name, **params):
    """Connect as role; assert success (0) or failure, mirroring test_access."""
    connstr = "user={}".format(role)
    if expected_res == 0:
        node.connect_ok(connstr, test_name, **params)
    else:
        # No checks of the error message, only the status code (plus any
        # log_like/log_unlike params).
        node.connect_fails(connstr, test_name, **params)


def _write_hba(node, line):
    """Replace pg_hba.conf with a single line (mirrors unlink + append_conf)."""
    hba = node.datadir / "pg_hba.conf"
    hba.unlink()
    node.append_conf(line, filename="pg_hba.conf")


def _setup_node(create_pg):
    """Create and start the PostgreSQL instance with the test users."""
    node = create_pg("node", start=False)
    node.append_conf("log_connections = all\n")
    # Needed to allow connect_fails to inspect postmaster log:
    node.append_conf("log_min_messages = debug2")
    node.start()
    node.safe_psql("CREATE USER test0;")
    node.safe_psql("CREATE USER test1;")
    node.safe_psql('CREATE USER "test2@example.net";')
    return node


def _setup_ldap(ldap_server):
    """Start the LDAP server and load the test data (anonymous auth)."""
    ldap_rootpw = "secret"
    ldap = ldap_server(ldap_rootpw, "anonymous")  # use anonymous auth
    ldap.ldapadd_file(os.path.join(os.path.dirname(__file__), "..", "authdata.ldif"))
    ldap.ldapsetpw("uid=test1,dc=example,dc=net", "secret1")
    ldap.ldapsetpw("uid=test2,dc=example,dc=net", "secret2")
    return ldap


def test_001_auth(create_pg, ldap_server):
    """Full matrix of LDAP authentication scenarios via pg_hba.conf."""
    ldap = _setup_ldap(ldap_server)
    (
        ldap_server_host,
        ldap_port,
        ldaps_port,
        ldap_url,
        ldaps_url,
        ldap_basedn,
        ldap_rootdn,
    ) = ldap.prop("server", "port", "s_port", "url", "s_url", "basedn", "rootdn")

    # don't bother to check the server's cert (though perhaps we should)
    os.environ["LDAPTLS_REQCERT"] = "never"

    node = _setup_node(create_pg)

    ctx = {
        "node": node,
        "host": ldap_server_host,
        "port": ldap_port,
        "ldaps_port": ldaps_port,
        "url": ldap_url,
        "ldaps_url": ldaps_url,
        "basedn": ldap_basedn,
        "rootdn": ldap_rootdn,
    }

    _simple_bind(ctx)
    _search_bind(ctx)
    _multiple_servers(ctx)
    _ldap_urls(ctx)
    _search_filters(ctx)
    _search_filters_in_urls(ctx)
    _diagnostic_message(ctx)
    _tls(ctx)


def _simple_bind(ctx):
    """Simple bind authentication with ldapprefix/ldapsuffix."""
    node = ctx["node"]
    _write_hba(
        node,
        "local all all ldap ldapserver={host} ldapport={port} "
        'ldapprefix="uid=" ldapsuffix=",dc=example,dc=net"'.format(**ctx),
    )
    node.restart()

    with _pgpassword("wrong"):
        _test_access(
            node,
            "test0",
            2,
            "simple bind authentication fails if user not found in LDAP",
            log_unlike=[_NOT_AUTHENTICATED],
        )
        _test_access(
            node,
            "test1",
            2,
            "simple bind authentication fails with wrong password",
            log_unlike=[_NOT_AUTHENTICATED],
        )

    with _pgpassword("secret1"):
        _test_access(
            node,
            "test1",
            0,
            "simple bind authentication succeeds",
            log_like=[_AUTH_TEST1],
        )
        # require_auth=password should complete successfully; other methods
        # should fail.
        node.connect_ok(
            "user=test1 require_auth=password",
            "password authentication required, works with ldap auth",
        )
        node.connect_fails(
            "user=test1 require_auth=scram-sha-256",
            "SCRAM authentication required, fails with ldap auth",
        )


def _search_bind(ctx):
    """Search+bind authentication with ldapbasedn."""
    node = ctx["node"]
    _write_hba(
        node,
        "local all all ldap ldapserver={host} ldapport={port} "
        'ldapbasedn="{basedn}"'.format(**ctx),
    )
    node.restart()

    with _pgpassword("wrong"):
        _test_access(
            node,
            "test0",
            2,
            "search+bind authentication fails if user not found in LDAP",
        )
        _test_access(
            node, "test1", 2, "search+bind authentication fails with wrong password"
        )
    with _pgpassword("secret1"):
        _test_access(
            node,
            "test1",
            0,
            "search+bind authentication succeeds",
            log_like=[_AUTH_TEST1],
        )


def _multiple_servers(ctx):
    """Search+bind authentication with two ldapserver entries."""
    node = ctx["node"]
    _write_hba(
        node,
        'local all all ldap ldapserver="{host} {host}" ldapport={port} '
        'ldapbasedn="{basedn}"'.format(**ctx),
    )
    node.restart()

    with _pgpassword("wrong"):
        _test_access(
            node,
            "test0",
            2,
            "search+bind authentication fails if user not found in LDAP",
        )
        _test_access(
            node, "test1", 2, "search+bind authentication fails with wrong password"
        )
    with _pgpassword("secret1"):
        _test_access(node, "test1", 0, "search+bind authentication succeeds")


def _ldap_urls(ctx):
    """Simple bind and search+bind via ldapurl."""
    node = ctx["node"]
    _write_hba(
        node,
        'local all all ldap ldapurl="{url}" ldapprefix="uid=" '
        'ldapsuffix=",dc=example,dc=net"'.format(**ctx),
    )
    node.restart()

    with _pgpassword("wrong"):
        _test_access(
            node,
            "test0",
            2,
            "simple bind with LDAP URL authentication fails if user not found in LDAP",
        )
        _test_access(
            node,
            "test1",
            2,
            "simple bind with LDAP URL authentication fails with wrong password",
        )
    with _pgpassword("secret1"):
        _test_access(
            node, "test1", 0, "simple bind with LDAP URL authentication succeeds"
        )

    _write_hba(
        node,
        'local all all ldap ldapurl="{url}/{basedn}?uid?sub"'.format(**ctx),
    )
    node.restart()

    with _pgpassword("wrong"):
        _test_access(
            node,
            "test0",
            2,
            "search+bind with LDAP URL authentication fails if user not found in LDAP",
        )
        _test_access(
            node,
            "test1",
            2,
            "search+bind with LDAP URL authentication fails with wrong password",
        )
    with _pgpassword("secret1"):
        _test_access(
            node, "test1", 0, "search+bind with LDAP URL authentication succeeds"
        )


def _search_filters(ctx):
    """ldapsearchfilter that matches by uid or mail."""
    node = ctx["node"]
    _write_hba(
        node,
        "local all all ldap ldapserver={host} ldapport={port} "
        'ldapbasedn="{basedn}" '
        'ldapsearchfilter="(|(uid=$username)(mail=$username))"'.format(**ctx),
    )
    node.restart()

    with _pgpassword("secret1"):
        _test_access(
            node, "test1", 0, "search filter finds by uid", log_like=[_AUTH_TEST1]
        )
    with _pgpassword("secret2"):
        _test_access(
            node,
            "test2@example.net",
            0,
            "search filter finds by mail",
            log_like=[_AUTH_TEST2],
        )


def _search_filters_in_urls(ctx):
    """ldapsearchfilter embedded in an ldapurl, then combined with the option."""
    node = ctx["node"]
    _write_hba(
        node,
        "local all all ldap "
        'ldapurl="{url}/{basedn}??sub?(|(uid=$username)(mail=$username))"'.format(
            **ctx
        ),
    )
    node.restart()

    with _pgpassword("secret1"):
        _test_access(node, "test1", 0, "search filter finds by uid")
    with _pgpassword("secret2"):
        _test_access(node, "test2@example.net", 0, "search filter finds by mail")

    # This is not documented: You can combine ldapurl and other ldap*
    # settings.  ldapurl is always parsed first, then the other settings
    # override.  It might be useful in a case like this.
    _write_hba(
        node,
        'local all all ldap ldapurl="{url}/{basedn}??sub" '
        'ldapsearchfilter="(|(uid=$username)(mail=$username))"'.format(**ctx),
    )
    node.restart()

    with _pgpassword("secret1"):
        _test_access(node, "test1", 0, "combined LDAP URL and search filter")


def _diagnostic_message(ctx):
    """Bad ldapprefix with a question mark triggers a diagnostic message."""
    node = ctx["node"]
    _write_hba(
        node,
        "local all all ldap ldapserver={host} ldapport={port} "
        'ldapprefix="?uid=" ldapsuffix=""'.format(**ctx),
    )
    node.restart()

    with _pgpassword("secret1"):
        _test_access(node, "test1", 2, "any attempt fails due to bad search pattern")


def _tls(ctx):
    """StartTLS, LDAPS, LDAPS via URL, and the bad StartTLS+LDAPS combination."""
    node = ctx["node"]

    # request StartTLS with ldaptls=1
    _write_hba(
        node,
        "local all all ldap ldapserver={host} ldapport={port} "
        'ldapbasedn="{basedn}" ldapsearchfilter="(uid=$username)" '
        "ldaptls=1".format(**ctx),
    )
    node.restart()
    with _pgpassword("secret1"):
        _test_access(node, "test1", 0, "StartTLS")

    # request LDAPS with ldapscheme=ldaps
    _write_hba(
        node,
        "local all all ldap ldapserver={host} ldapscheme=ldaps "
        'ldapport={ldaps_port} ldapbasedn="{basedn}" '
        'ldapsearchfilter="(uid=$username)"'.format(**ctx),
    )
    node.restart()
    with _pgpassword("secret1"):
        _test_access(node, "test1", 0, "LDAPS")

    # request LDAPS with ldapurl=ldaps://...
    _write_hba(
        node,
        "local all all ldap "
        'ldapurl="{ldaps_url}/{basedn}??sub?(uid=$username)"'.format(**ctx),
    )
    node.restart()
    with _pgpassword("secret1"):
        _test_access(node, "test1", 0, "LDAPS with URL")

    # bad combination of LDAPS and StartTLS
    _write_hba(
        node,
        "local all all ldap "
        'ldapurl="{ldaps_url}/{basedn}??sub?(uid=$username)" ldaptls=1'.format(**ctx),
    )
    node.restart()
    with _pgpassword("secret1"):
        _test_access(node, "test1", 2, "bad combination of LDAPS and StartTLS")
