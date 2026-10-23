# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/interfaces/libpq/t/001_uri.pl.

Drives the libpq_uri_regress helper with a long table of connection URIs and
checks, for each, the program's stdout (the parsed connection options), stderr
(libpq's URI parse error, if any) and exit status. A non-empty expected stderr
means the URI is invalid and the program is expected to exit non-zero.

Some entries override environment variables for the duration of the test; the
three sslmode entries set PGSSLROOTCERT=system to exercise the system-CA path,
where the default sslmode becomes verify-full.
"""

from pypg import PgBin

# pylint: disable=line-too-long
# The URI strings and libpq's verbatim error messages cannot be wrapped without
# changing the assertions, so long lines are tolerated in this module.

# List of URI tests. For each test the first element is the input string, the
# second the expected stdout and the third the expected stderr. An optional
# fourth element is a dict of environment variables to override for the test.
_TESTS = [
    (
        r"postgresql://uri-user:secret@host:12345/db",
        r"user='uri-user' password='secret' dbname='db' host='host' port='12345' (inet)",
        r"",
    ),
    (
        r"postgresql://uri-user@host:12345/db",
        r"user='uri-user' dbname='db' host='host' port='12345' (inet)",
        r"",
    ),
    (
        r"postgresql://uri-user@host/db",
        r"user='uri-user' dbname='db' host='host' (inet)",
        r"",
    ),
    (
        r"postgresql://host:12345/db",
        r"dbname='db' host='host' port='12345' (inet)",
        r"",
    ),
    (r"postgresql://host/db", r"dbname='db' host='host' (inet)", r""),
    (
        r"postgresql://uri-user@host:12345/",
        r"user='uri-user' host='host' port='12345' (inet)",
        r"",
    ),
    (
        r"postgresql://uri-user@host/",
        r"user='uri-user' host='host' (inet)",
        r"",
    ),
    (r"postgresql://uri-user@", r"user='uri-user' (local)", r""),
    (r"postgresql://host:12345/", r"host='host' port='12345' (inet)", r""),
    (r"postgresql://host:12345", r"host='host' port='12345' (inet)", r""),
    (r"postgresql://host/db", r"dbname='db' host='host' (inet)", r""),
    (r"postgresql://host/", r"host='host' (inet)", r""),
    (r"postgresql://host", r"host='host' (inet)", r""),
    (r"postgresql://", r"(local)", r""),
    (
        r"postgresql://?hostaddr=127.0.0.1",
        r"hostaddr='127.0.0.1' (inet)",
        r"",
    ),
    (
        r"postgresql://example.com?hostaddr=63.1.2.4",
        r"host='example.com' hostaddr='63.1.2.4' (inet)",
        r"",
    ),
    (r"postgresql://%68ost/", r"host='host' (inet)", r""),
    (
        r"postgresql://host/db?user=uri-user",
        r"user='uri-user' dbname='db' host='host' (inet)",
        r"",
    ),
    (
        r"postgresql://host/db?user=uri-user&port=12345",
        r"user='uri-user' dbname='db' host='host' port='12345' (inet)",
        r"",
    ),
    (
        r"postgresql://host/db?u%73er=someotheruser&port=12345",
        r"user='someotheruser' dbname='db' host='host' port='12345' (inet)",
        r"",
    ),
    (
        r"postgresql://host/db?u%7aer=someotheruser&port=12345",
        r"",
        r'libpq_uri_regress: invalid URI query parameter: "uzer"',
    ),
    (
        r"postgresql://host:12345?user=uri-user",
        r"user='uri-user' host='host' port='12345' (inet)",
        r"",
    ),
    (
        r"postgresql://host?user=uri-user",
        r"user='uri-user' host='host' (inet)",
        r"",
    ),
    (
        # Leading and trailing spaces, works.
        r"postgresql://host?  user = uri-user & port  = 12345 ",
        r"user='uri-user' host='host' port='12345' (inet)",
        r"",
    ),
    (
        # Trailing data in parameter.
        r"postgresql://host?  user user  =  uri  & port = 12345 12 ",
        r"",
        r'libpq_uri_regress: unexpected spaces found in "  user user  ", use percent-encoded spaces (%20) instead',
    ),
    (
        # Trailing data in value.
        r"postgresql://host?  user  =  uri-user  & port = 12345 12 ",
        r"",
        r'libpq_uri_regress: unexpected spaces found in " 12345 12 ", use percent-encoded spaces (%20) instead',
    ),
    (r"postgresql://host?", r"host='host' (inet)", r""),
    (
        r"postgresql://[::1]:12345/db",
        r"dbname='db' host='::1' port='12345' (inet)",
        r"",
    ),
    (r"postgresql://[::1]/db", r"dbname='db' host='::1' (inet)", r""),
    (
        r"postgresql://[2001:db8::1234]/",
        r"host='2001:db8::1234' (inet)",
        r"",
    ),
    (
        r"postgresql://[200z:db8::1234]/",
        r"host='200z:db8::1234' (inet)",
        r"",
    ),
    (r"postgresql://[::1]", r"host='::1' (inet)", r""),
    (r"postgres://", r"(local)", r""),
    (r"postgres:///", r"(local)", r""),
    (r"postgres:///db", r"dbname='db' (local)", r""),
    (
        r"postgres://uri-user@/db",
        r"user='uri-user' dbname='db' (local)",
        r"",
    ),
    (
        r"postgres://?host=/path/to/socket/dir",
        r"host='/path/to/socket/dir' (local)",
        r"",
    ),
    (
        r"postgresql://host?uzer=",
        r"",
        r'libpq_uri_regress: invalid URI query parameter: "uzer"',
    ),
    (
        r"postgre://",
        r"",
        r'libpq_uri_regress: missing "=" after "postgre://" in connection info string',
    ),
    (
        r"postgres://[::1",
        r"",
        r'libpq_uri_regress: end of string reached when looking for matching "]" in IPv6 host address in URI: "postgres://[::1"',
    ),
    (
        r"postgres://[]",
        r"",
        r'libpq_uri_regress: IPv6 host address may not be empty in URI: "postgres://[]"',
    ),
    (
        r"postgres://[::1]z",
        r"",
        r'libpq_uri_regress: unexpected character "z" at position 17 in URI (expected ":" or "/"): "postgres://[::1]z"',
    ),
    (
        r"postgresql://host?zzz",
        r"",
        r'libpq_uri_regress: missing key/value separator "=" in URI query parameter: "zzz"',
    ),
    (
        r"postgresql://host?value1&value2",
        r"",
        r'libpq_uri_regress: missing key/value separator "=" in URI query parameter: "value1"',
    ),
    (
        r"postgresql://host?key=key=value",
        r"",
        r'libpq_uri_regress: extra key/value separator "=" in URI query parameter: "key"',
    ),
    (
        r"postgres://host?dbname=%XXfoo",
        r"",
        r'libpq_uri_regress: invalid percent-encoded token: "%XXfoo"',
    ),
    (
        r"postgresql://a%00b",
        r"",
        r'libpq_uri_regress: forbidden value %00 in percent-encoded value: "a%00b"',
    ),
    (
        r"postgresql://%zz",
        r"",
        r'libpq_uri_regress: invalid percent-encoded token: "%zz"',
    ),
    (
        r"postgresql://%1",
        r"",
        r'libpq_uri_regress: invalid percent-encoded token: "%1"',
    ),
    (
        r"postgresql://%",
        r"",
        r'libpq_uri_regress: invalid percent-encoded token: "%"',
    ),
    (r"postgres://@host", r"host='host' (inet)", r""),
    (r"postgres://host:/", r"host='host' (inet)", r""),
    (r"postgres://:12345/", r"port='12345' (local)", r""),
    (
        r"postgres://otheruser@?host=/no/such/directory",
        r"user='otheruser' host='/no/such/directory' (local)",
        r"",
    ),
    (
        r"postgres://otheruser@/?host=/no/such/directory",
        r"user='otheruser' host='/no/such/directory' (local)",
        r"",
    ),
    (
        r"postgres://otheruser@:12345?host=/no/such/socket/path",
        r"user='otheruser' host='/no/such/socket/path' port='12345' (local)",
        r"",
    ),
    (
        r"postgres://otheruser@:12345/db?host=/path/to/socket",
        r"user='otheruser' dbname='db' host='/path/to/socket' port='12345' (local)",
        r"",
    ),
    (
        r"postgres://:12345/db?host=/path/to/socket",
        r"dbname='db' host='/path/to/socket' port='12345' (local)",
        r"",
    ),
    (
        r"postgres://:12345?host=/path/to/socket",
        r"host='/path/to/socket' port='12345' (local)",
        r"",
    ),
    (
        r"postgres://%2Fvar%2Flib%2Fpostgresql/dbname",
        r"dbname='dbname' host='/var/lib/postgresql' (local)",
        r"",
    ),
    # Usually the default sslmode is 'prefer' (for libraries with SSL) or
    # 'disable' (for those without). This default changes to 'verify-full' if
    # the system CA store is in use.
    (
        r"postgresql://host?sslmode=disable",
        r"host='host' sslmode='disable' (inet)",
        r"",
        {"PGSSLROOTCERT": "system"},
    ),
    (
        r"postgresql://host?sslmode=prefer",
        r"host='host' sslmode='prefer' (inet)",
        r"",
        {"PGSSLROOTCERT": "system"},
    ),
    (
        r"postgresql://host?sslmode=verify-full",
        r"host='host' (inet)",
        r"",
        {"PGSSLROOTCERT": "system"},
    ),
]


def _run_uri(pg_bin, uri, envvars):
    """Run libpq_uri_regress for uri with envvars overridden; return result.

    The helper is built but not installed, so it lives on PATH (the meson test
    harness prepends the build's test directory). Mirrors the IPC::Run::run call
    in the Perl original, chomping a single trailing newline off each stream.
    """
    result = pg_bin.run_command(["libpq_uri_regress", uri], extra_env=envvars)
    return result


def test_001_uri(pg_bin: PgBin):
    """Each URI parses to the expected options or fails with the expected error."""
    for entry in _TESTS:
        uri, expected_stdout, expected_stderr = entry[0], entry[1], entry[2]
        envvars = entry[3] if len(entry) > 3 else {}
        result = _run_uri(pg_bin, uri, envvars)

        expected_exit = 0 if expected_stderr == "" else 1
        actual_exit = 0 if result.exit_code == 0 else 1
        assert actual_exit == expected_exit, "{}: exit status".format(uri)
        assert result.stdout == expected_stdout, "{}: stdout".format(uri)
        assert result.stderr == expected_stderr, "{}: stderr".format(uri)
