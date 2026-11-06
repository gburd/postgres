# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/kerberos/t/001_auth.pl.

Sets up a KDC and then runs a variety of tests to make sure that the
GSSAPI/Kerberos authentication and encryption are working properly, that the
options in pg_hba.conf and pg_ident.conf are handled correctly, that the
server-side pg_stat_gssapi view reports what we expect to see for each test and
that SYSTEM_USER returns what we expect to see.

Also tests that GSSAPI delegation is working properly and that those
credentials can be used to make dblink / postgres_fdw connections.

Since this requires setting up a full KDC, it doesn't make much sense to have
multiple test scripts (since they'd have to also create their own KDC and that
could cause race conditions or other problems) -- so just add whatever other
tests are needed to here.
"""

import os
import pathlib
import re
import stat

import pytest

import pypg
from pypg import KerberosServer

# This suite stands up a full KDC and opens local TCP ports, so it is gated
# behind PG_TEST_EXTRA=kerberos, exactly like the Perl 001_auth.pl plan check.
pytestmark = pypg.require_test_extras("kerberos")

# psql inherits PGAPPNAME, which the server logs as application_name. The Perl
# harness sets PGAPPNAME to basename($0); mirror that with this file's name so
# the "connection authorized: ... application_name=..." log assertions match.
APPLICATION = "test_001_auth.py"

DBNAME = "postgres"
USERNAME = "test1"
HOST = "auth-test-localhost.postgresql.example.com"
HOSTADDR = "127.0.0.1"
REALM = "EXAMPLE.COM"
TEST1_PASSWORD = "secret1"


def _skip_if_no_gssapi():
    """Skip unless the build links GSSAPI (mirrors the with_gssapi plan check)."""
    if os.environ.get("with_gssapi") != "yes":
        pytest.skip("GSSAPI/Kerberos not supported by this build")


@pytest.fixture(scope="module")
def pgpass(tmp_check):
    """A .pgpass file that must never be used (mirrors the Perl $pgpass setup).

    It is deliberately filled with a wrong password so that any code path which
    accidentally falls back to it is caught.
    """
    path = tmp_check / ".pgpass"
    pypg.append_to_file(path, "*:*:*:*:abc123")
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    return path


@pytest.fixture(scope="module")
def krb(tmp_check, datadir):
    """Stand up the KDC, add the test1 principal, and tear it down at the end.

    Mirrors `PostgreSQL::Test::Kerberos->new` plus the test1 principal creation
    done at the top of 001_auth.pl. The log directory mirrors the Perl
    log_path; here it lives under the data directory's parent.
    """
    _skip_if_no_gssapi()
    log_path = pathlib.Path(datadir).parent / "log"
    log_path.mkdir(parents=True, exist_ok=True)
    server = KerberosServer.setup(tmp_check, log_path, HOST, HOSTADDR, REALM)
    server.create_principal("test1", TEST1_PASSWORD)
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="module")
def node(krb, create_pg_module):
    """Initialize and start the PostgreSQL server configured for GSSAPI.

    Mirrors the "setting up PostgreSQL instance" block: listen on the test
    interface, point krb_server_keyfile at the KDC-issued service keytab, and
    enable verbose connection logging so the log assertions can fire.
    """
    pg = create_pg_module("node", start=False)
    # psql inherits PGAPPNAME as its application_name; the Perl harness sets it
    # to basename($0). Set it here so the connection-authorized log lines name
    # this test file, matching the application_name assertions.
    os.environ["PGAPPNAME"] = APPLICATION
    pg.append_conf(
        "\n".join(
            [
                "listen_addresses = '{}'".format(HOSTADDR),
                "krb_server_keyfile = '{}'".format(krb.keytab),
                "log_connections = all",
                "log_min_messages = debug2",
                "lc_messages = 'C'",
            ]
        )
    )
    pg.start()
    return pg


@pytest.fixture(scope="module")
def setup_sql(node, pgpass):
    """Create the users, extensions, foreign servers and tables under test.

    Mirrors the long sequence of safe_psql() statements after server start.
    Skips the whole module if postgres_fdw / dblink are not available, since
    the delegation tests depend on them (the Perl script assumes contrib).
    """
    port = node.port
    for ext in ("postgres_fdw", "dblink"):
        if not node.check_extension(ext):
            pytest.skip("{} contrib extension is required".format(ext))

    node.safe_psql("CREATE USER test1;")
    node.safe_psql("CREATE USER test2 WITH ENCRYPTED PASSWORD 'abc123';")
    node.safe_psql("CREATE EXTENSION postgres_fdw;")
    node.safe_psql("CREATE EXTENSION dblink;")
    node.safe_psql(
        "CREATE SERVER s1 FOREIGN DATA WRAPPER postgres_fdw OPTIONS "
        "(host '{host}', hostaddr '{addr}', port '{port}', dbname 'postgres');".format(
            host=HOST, addr=HOSTADDR, port=port
        )
    )
    node.safe_psql(
        "CREATE SERVER s2 FOREIGN DATA WRAPPER postgres_fdw OPTIONS "
        "(port '{port}', dbname 'postgres', passfile '{pgpass}');".format(
            port=port, pgpass=pgpass
        )
    )
    node.safe_psql("GRANT USAGE ON FOREIGN SERVER s1 TO test1;")
    node.safe_psql("CREATE USER MAPPING FOR test1 SERVER s1 OPTIONS (user 'test1');")
    node.safe_psql("CREATE USER MAPPING FOR test1 SERVER s2 OPTIONS (user 'test2');")
    node.safe_psql("CREATE TABLE t1 (c1 int);")
    node.safe_psql("INSERT INTO t1 VALUES (1);")
    node.safe_psql(
        "CREATE FOREIGN TABLE tf1 (c1 int) SERVER s1 OPTIONS "
        "(schema_name 'public', table_name 't1');"
    )
    node.safe_psql("GRANT SELECT ON t1 TO test1;")
    node.safe_psql("GRANT SELECT ON tf1 TO test1;")
    node.safe_psql(
        "CREATE FOREIGN TABLE tf2 (c1 int) SERVER s2 OPTIONS "
        "(schema_name 'public', table_name 't1');"
    )
    node.safe_psql("GRANT SELECT ON tf2 TO test1;")
    node.safe_psql(
        "CREATE TABLE ids (id) AS SELECT 'gss:test1@{realm}' "
        "FROM generate_series(1, 10);".format(realm=REALM)
    )
    node.safe_psql("GRANT SELECT ON ids TO public;")


def _connstr(node, role, gssencmode):
    """Build the TCP+GSS connection string used by every test_access/query.

    Mirrors `$node->connstr('postgres') . " user=$role host=$host
    hostaddr=$hostaddr $gssencmode"`. Later keywords override earlier ones in
    libpq, so the appended host/hostaddr replace the socket host in connstr().
    """
    base = node.connstr("postgres")
    return "{base} user={role} host={host} hostaddr={addr} {mode}".format(
        base=base, role=role, host=HOST, addr=HOSTADDR, mode=gssencmode
    ).rstrip()


def _test_access(node, role, query, expected_res, gssencmode, test_name, log_msgs=None):
    """Connect over TCP/IP for Kerberos; assert success/failure + log lines.

    Mirrors the Perl test_access(): on expected_res==0 the connection must
    succeed and the query must return SQL true ("t"); otherwise it must fail.
    log_msgs are matched literally against the server log emitted during the
    attempt.
    """
    connstr = _connstr(node, role, gssencmode)
    log_like = [re.escape(m) for m in log_msgs] if log_msgs else None
    if expected_res == 0:
        node.connect_ok(
            connstr,
            test_name,
            sql=query,
            expected_stdout=r"^t$",
            log_like=log_like,
        )
    else:
        node.connect_fails(connstr, test_name, log_like=log_like)


def _test_query(node, role, query, expected, gssencmode, test_name):
    """Connect over TCP/IP and assert an arbitrary query result.

    Mirrors the Perl test_query(): the connection must succeed and stdout must
    match the expected regex.
    """
    connstr = _connstr(node, role, gssencmode)
    node.connect_ok(connstr, test_name, sql=query, expected_stdout=expected)


def _reset_hba(node, lines):
    """Replace pg_hba.conf with the given lines and restart (mirrors the Perl).

    The Perl script unlinks pg_hba.conf and writes fresh contents before each
    HBA scenario, then restarts. We rewrite the file in place to the same
    effect.
    """
    hba = pathlib.Path(node.datadir) / "pg_hba.conf"
    with open(hba, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    node.restart()


STAT_NOT_DELEGATED = (
    "SELECT gss_authenticated AND encrypted AND NOT credentials_delegated "
    "FROM pg_stat_gssapi WHERE pid = pg_backend_pid();"
)
STAT_DELEGATED = (
    "SELECT gss_authenticated AND encrypted AND credentials_delegated "
    "FROM pg_stat_gssapi WHERE pid = pg_backend_pid();"
)
STAT_NOENC_DELEGATED = (
    "SELECT gss_authenticated AND NOT encrypted AND credentials_delegated "
    "FROM pg_stat_gssapi WHERE pid = pg_backend_pid();"
)


def test_001_auth(node, krb, setup_sql, pgpass):
    """Full GSSAPI/Kerberos auth, encryption, delegation and mapping scenarios."""
    port = node.port

    # --- host hba, no ident map yet -------------------------------------
    _reset_hba(
        node,
        [
            "local all test2 scram-sha-256",
            "host all all {}/32 gss map=mymap".format(HOSTADDR),
        ],
    )

    _test_access(node, "test1", "SELECT true", 2, "", "fails without ticket")

    krb.create_ticket("test1", TEST1_PASSWORD)

    _test_access(
        node,
        "test1",
        "SELECT true",
        2,
        "",
        "fails without mapping",
        log_msgs=[
            'connection authenticated: identity="test1@{}" method=gss'.format(REALM),
            'no match in usermap "mymap" for user "test1"',
        ],
    )

    node.append_conf("mymap  /^(.*)@{}$  \\1".format(REALM), filename="pg_ident.conf")
    node.restart()

    _test_access(
        node,
        "test1",
        STAT_NOT_DELEGATED,
        0,
        "",
        "succeeds with mapping with default gssencmode and host hba, ticket not forwardable",
        log_msgs=_auth_msgs("yes", "no"),
    )
    _test_access(
        node,
        "test1",
        STAT_NOT_DELEGATED,
        0,
        "gssencmode=prefer",
        "succeeds with GSS-encrypted access preferred with host hba, ticket not forwardable",
        log_msgs=_auth_msgs("yes", "no"),
    )
    _test_access(
        node,
        "test1",
        STAT_NOT_DELEGATED,
        0,
        "gssencmode=require",
        "succeeds with GSS-encrypted access required with host hba, ticket not forwardable",
        log_msgs=_auth_msgs("yes", "no"),
    )
    _test_access(
        node,
        "test1",
        STAT_NOT_DELEGATED,
        0,
        "gssencmode=prefer gssdelegation=1",
        "succeeds with GSS-encrypted access preferred with host hba and credentials not delegated even though asked for (ticket not forwardable)",
        log_msgs=_auth_msgs("yes", "no"),
    )
    _test_access(
        node,
        "test1",
        STAT_NOT_DELEGATED,
        0,
        "gssencmode=require gssdelegation=1",
        "succeeds with GSS-encrypted access required with host hba and credentials not delegated even though asked for (ticket not forwardable)",
        log_msgs=_auth_msgs("yes", "no"),
    )

    # Test that we can transport a reasonable amount of data.
    _test_query(
        node,
        "test1",
        "SELECT * FROM generate_series(1, 100000);",
        r"^1\n(?:.*\n)*1024\n(?:.*\n)*9999\n(?:.*\n)*100000$",
        "gssencmode=require",
        "receiving 100K lines works",
    )
    _test_query(
        node,
        "test1",
        "CREATE TEMP TABLE mytab (f1 int primary key);\n"
        "COPY mytab FROM STDIN;\n"
        + "\n".join(str(i) for i in range(1, 100001))
        + "\n\\.\n"
        + "SELECT COUNT(*) FROM mytab;",
        r"^100000$",
        "gssencmode=require",
        "sending 100K lines works",
    )

    _require_auth_host(node)

    # Test that SYSTEM_USER works.
    _test_query(
        node,
        "test1",
        "SELECT SYSTEM_USER;",
        r"^gss:test1@{}$".format(re.escape(REALM)),
        "gssencmode=require",
        "testing system_user",
    )
    # Test that SYSTEM_USER works with parallel workers.
    _test_query(
        node,
        "test1",
        "\n".join(
            [
                "SET min_parallel_table_scan_size TO 0;",
                "SET parallel_setup_cost TO 0;",
                "SET parallel_tuple_cost TO 0;",
                "SET max_parallel_workers_per_gather TO 2;",
                "SELECT bool_and(SYSTEM_USER = id) FROM ids;",
            ]
        ),
        r"^t$",
        "gssencmode=require",
        "testing system_user with parallel workers",
    )

    _hostgssenc_scenarios(node, krb, pgpass, port)
    _hostnogssenc_scenarios(node, pgpass, port)
    _include_realm_scenarios(node, port)


def _auth_msgs(encrypted, delegated):
    """Return the unescaped expected-log fragments for a GSS connection."""
    authorized = (
        "connection authorized: user={user} database={db} "
        "application_name={app} GSS (authenticated=yes, encrypted={enc}, "
        "delegated_credentials={deleg}, principal=test1@{realm})".format(
            user=USERNAME,
            db=DBNAME,
            app=APPLICATION,
            enc=encrypted,
            deleg=delegated,
            realm=REALM,
        )
    )
    return [
        'connection authenticated: identity="test1@{}" method=gss'.format(REALM),
        authorized,
    ]


def _require_auth_host(node):
    """require_auth=gss/sspi checks against the host hba (mirrors the Perl)."""
    node.connect_ok(
        _connstr(node, "test1", "gssencmode=disable require_auth=gss"),
        "GSS authentication requested, works with non-encrypted GSS",
    )
    node.connect_ok(
        _connstr(node, "test1", "gssencmode=require require_auth=gss"),
        "GSS authentication requested, works with encrypted GSS auth",
    )
    node.connect_fails(
        _connstr(node, "test1", "gssencmode=disable require_auth=sspi"),
        "SSPI authentication requested, fails with non-encrypted GSS",
        expected_stderr=r'authentication method requirement "sspi" failed: server requested GSSAPI authentication',
    )
    node.connect_fails(
        _connstr(node, "test1", "gssencmode=require require_auth=sspi"),
        "SSPI authentication requested, fails with encrypted GSS",
        expected_stderr=r'authentication method requirement "sspi" failed: server did not complete authentication',
    )


def _delegation_fails(node, sql, connstr, msg):
    """Assert a dblink/postgres_fdw psql call fails (rc=3) without delegation.

    Mirrors the repeated $node->psql(...) blocks that expect exit code 3 and a
    "password or GSSAPI delegated credentials required" stderr with empty
    stdout.
    """
    result = node.psql_capture(sql, connstr=connstr)
    assert result.exit_code == 3, "{}: expected exit 3, got {}\n{}".format(
        msg, result.exit_code, result.stderr
    )
    assert re.search(
        r"password or GSSAPI delegated credentials required", result.stderr
    ), "{}: stderr was {!r}".format(msg, result.stderr)
    assert re.search(r"^$", result.stdout), "{}: stdout was {!r}".format(
        msg, result.stdout
    )


def _hostgssenc_scenarios(node, krb, pgpass, port):
    """hostgssenc hba: forwardable ticket, gss_accept_delegation off/on."""
    _reset_hba(
        node,
        [
            "    local all test2 scram-sha-256",
            "\thostgssenc all all {}/32 gss map=mymap".format(HOSTADDR),
        ],
    )

    # Re-create the ticket, with the forwardable flag set.
    krb.create_ticket("test1", TEST1_PASSWORD, forwardable=True)

    _test_access(
        node,
        "test1",
        STAT_NOT_DELEGATED,
        0,
        "gssencmode=prefer gssdelegation=1",
        "succeeds with GSS-encrypted access preferred and hostgssenc hba and credentials not forwarded (server does not accept them, default)",
        log_msgs=_auth_msgs("yes", "no"),
    )
    _test_access(
        node,
        "test1",
        STAT_NOT_DELEGATED,
        0,
        "gssencmode=require gssdelegation=1",
        "succeeds with GSS-encrypted access required and hostgssenc hba and credentials not forwarded (server does not accept them, default)",
        log_msgs=_auth_msgs("yes", "no"),
    )

    node.append_conf("gss_accept_delegation=off")
    node.restart()

    _test_access(
        node,
        "test1",
        STAT_NOT_DELEGATED,
        0,
        "gssencmode=prefer gssdelegation=1",
        "succeeds with GSS-encrypted access preferred and hostgssenc hba and credentials not forwarded (server does not accept them, explicitly disabled)",
        log_msgs=_auth_msgs("yes", "no"),
    )
    _test_access(
        node,
        "test1",
        STAT_NOT_DELEGATED,
        0,
        "gssencmode=require gssdelegation=1",
        "succeeds with GSS-encrypted access required and hostgssenc hba and credentials not forwarded (server does not accept them, explicitly disabled)",
        log_msgs=_auth_msgs("yes", "no"),
    )

    node.append_conf("gss_accept_delegation=on")
    node.restart()

    _test_access(
        node,
        "test1",
        STAT_DELEGATED,
        0,
        "gssencmode=prefer gssdelegation=1",
        "succeeds with GSS-encrypted access preferred and hostgssenc hba and credentials forwarded",
        log_msgs=_auth_msgs("yes", "yes"),
    )
    _test_access(
        node,
        "test1",
        STAT_DELEGATED,
        0,
        "gssencmode=require gssdelegation=1",
        "succeeds with GSS-encrypted access required and hostgssenc hba and credentials forwarded",
        log_msgs=_auth_msgs("yes", "yes"),
    )
    _test_access(
        node,
        "test1",
        STAT_NOT_DELEGATED,
        0,
        "gssencmode=prefer",
        "succeeds with GSS-encrypted access preferred and hostgssenc hba and credentials not forwarded",
        log_msgs=_auth_msgs("yes", "no"),
    )
    _test_access(
        node,
        "test1",
        STAT_NOT_DELEGATED,
        0,
        "gssencmode=require gssdelegation=0",
        "succeeds with GSS-encrypted access required and hostgssenc hba and credentials explicitly not forwarded",
        log_msgs=_auth_msgs("yes", "no"),
    )

    require_connstr = "user=test1 host={host} hostaddr={addr} gssencmode=require gssdelegation=0".format(
        host=HOST, addr=HOSTADDR
    )
    _delegation_fails(
        node,
        "SELECT * FROM dblink('user=test1 dbname={db} host={host} hostaddr={addr} port={port}','select 1') as t1(c1 int);".format(
            db=DBNAME, host=HOST, addr=HOSTADDR, port=port
        ),
        require_connstr,
        "dblink attempt fails without delegated credentials",
    )
    _delegation_fails(
        node,
        "SELECT * FROM dblink('user=test2 dbname={db} port={port} passfile={pgpass}','select 1') as t1(c1 int);".format(
            db=DBNAME, port=port, pgpass=pgpass
        ),
        require_connstr,
        "dblink does not work without delegated credentials and with passfile",
    )
    _delegation_fails(
        node,
        "TABLE tf1;",
        require_connstr,
        "postgres_fdw does not work without delegated credentials",
    )
    _delegation_fails(
        node,
        "TABLE tf2;",
        require_connstr,
        "postgres_fdw does not work without delegated credentials and with passfile",
    )

    _test_access(
        node,
        "test1",
        "SELECT true",
        2,
        "gssencmode=disable",
        "fails with GSS encryption disabled and hostgssenc hba",
    )

    node.connect_ok(
        _connstr(node, "test1", "gssencmode=require require_auth=gss"),
        "GSS authentication requested, works with GSS encryption",
    )
    node.connect_ok(
        _connstr(node, "test1", "gssencmode=require require_auth=gss,scram-sha-256"),
        "multiple authentication types requested, works with GSS encryption",
    )


def _hostnogssenc_scenarios(node, pgpass, port):
    """hostnogssenc hba: delegated, unencrypted GSS connections."""
    _reset_hba(
        node,
        [
            "    local all test2 scram-sha-256",
            "\thostnogssenc all all {}/32 gss map=mymap".format(HOSTADDR),
        ],
    )

    _test_access(
        node,
        "test1",
        STAT_NOENC_DELEGATED,
        0,
        "gssencmode=prefer gssdelegation=1",
        "succeeds with GSS-encrypted access preferred and hostnogssenc hba, but no encryption",
        log_msgs=_auth_msgs("no", "yes"),
    )
    _test_access(
        node,
        "test1",
        "SELECT true",
        2,
        "gssencmode=require",
        "fails with GSS-encrypted access required and hostnogssenc hba",
    )
    _test_access(
        node,
        "test1",
        STAT_NOENC_DELEGATED,
        0,
        "gssencmode=disable gssdelegation=1",
        "succeeds with GSS encryption disabled and hostnogssenc hba",
        log_msgs=_auth_msgs("no", "yes"),
    )

    _test_query(
        node,
        "test1",
        "SELECT * FROM dblink('user=test1 dbname={db} host={host} hostaddr={addr} port={port}','select 1') as t1(c1 int);".format(
            db=DBNAME, host=HOST, addr=HOSTADDR, port=port
        ),
        r"^1$",
        "gssencmode=prefer gssdelegation=1",
        "dblink works not-encrypted (server not configured to accept encrypted GSSAPI connections)",
    )
    _test_query(
        node,
        "test1",
        "TABLE tf1;",
        r"^1$",
        "gssencmode=prefer gssdelegation=1",
        "postgres_fdw works not-encrypted (server not configured to accept encrypted GSSAPI connections)",
    )

    prefer_connstr = "user=test1 host={host} hostaddr={addr} gssencmode=prefer gssdelegation=1".format(
        host=HOST, addr=HOSTADDR
    )
    _delegation_fails(
        node,
        "SELECT * FROM dblink('user=test2 dbname={db} port={port} passfile={pgpass}','select 1') as t1(c1 int);".format(
            db=DBNAME, port=port, pgpass=pgpass
        ),
        prefer_connstr,
        "dblink does not work with delegated credentials and with passfile",
    )
    _delegation_fails(
        node,
        "TABLE tf2;",
        prefer_connstr,
        "postgres_fdw does not work with delegated credentials and with passfile",
    )


def _include_realm_scenarios(node, port):
    """include_realm=0 hba: delegated encrypted connections + krb_realm check."""
    pathlib.Path(node.datadir, "pg_ident.conf").write_text("", encoding="utf-8")
    _reset_hba(
        node,
        [
            "    local all test2 scram-sha-256",
            "\thost all all {}/32 gss include_realm=0".format(HOSTADDR),
        ],
    )

    _test_access(
        node,
        "test1",
        STAT_DELEGATED,
        0,
        "gssdelegation=1",
        "succeeds with include_realm=0 and defaults",
        log_msgs=_auth_msgs("yes", "yes"),
    )

    _test_query(
        node,
        "test1",
        "SELECT * FROM dblink('user=test1 dbname={db} host={host} hostaddr={addr} port={port} password=1234','select 1') as t1(c1 int);".format(
            db=DBNAME, host=HOST, addr=HOSTADDR, port=port
        ),
        r"^1$",
        "gssencmode=require gssdelegation=1",
        "dblink works encrypted",
    )
    _test_query(
        node,
        "test1",
        "TABLE tf1;",
        r"^1$",
        "gssencmode=require gssdelegation=1",
        "postgres_fdw works encrypted",
    )

    # Reset pg_hba.conf, and cause a usermap failure with an authentication
    # that has passed.
    _reset_hba(
        node,
        [
            "    local all test2 scram-sha-256",
            "\thost all all {}/32 gss include_realm=0 krb_realm=EXAMPLE.ORG".format(
                HOSTADDR
            ),
        ],
    )

    _test_access(
        node,
        "test1",
        "SELECT true",
        2,
        "",
        "fails with wrong krb_realm, but still authenticates",
        log_msgs=[
            'connection authenticated: identity="test1@{}" method=gss'.format(REALM)
        ],
    )
