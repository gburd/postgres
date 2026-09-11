# Copyright (c) 2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/test_misc/t/012_ddlutils.pl.

Tests for pg_get_database_ddl(), pg_get_tablespace_ddl(), and
pg_get_role_ddl().  These are TAP tests rather than plain regression tests
because they create databases and tablespaces, which are heavyweight
operations that should run only once rather than being repeated with every
invocation of the core regression suite.
"""

import re


def _ddl_filter(text):
    """Strip locale/collation details from DDL output so that results are
    stable across platforms (mirrors the Perl ddl_filter sub)."""
    text = re.sub(
        r"\s*\bLOCALE_PROVIDER\b\s*=\s*(?:'[^']*'|\"[^\"]*\"|\S+)",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\s*LC_COLLATE\s*=\s*(['\"])[^'\"]*\1", "", text, flags=re.IGNORECASE
    )
    text = re.sub(r"\s*LC_CTYPE\s*=\s*(['\"])[^'\"]*\1", "", text, flags=re.IGNORECASE)
    text = re.sub(
        r"\s*\S*LOCALE\S*\s*=?\s*(['\"])[^'\"]*\1", "", text, flags=re.IGNORECASE
    )
    text = re.sub(
        r"\s*\S*COLLATION\S*\s*=?\s*(['\"])[^'\"]*\1", "", text, flags=re.IGNORECASE
    )
    return text


def test_012_ddlutils(create_pg):
    """pg_get_role_ddl/pg_get_database_ddl/pg_get_tablespace_ddl behavior."""
    node = create_pg("main", start=False)
    # Force UTC so that timestamptz values (e.g. VALID UNTIL) render the same
    # way regardless of the host's local timezone.
    node.append_conf("timezone = 'UTC'\n")
    node.start()

    def like(text, pattern, _msg):
        assert re.search(pattern, text), "{}\noutput:\n{}".format(_msg, text)

    def unlike(text, pattern, _msg):
        assert not re.search(pattern, text), "{}\noutput:\n{}".format(_msg, text)

    ######################################################################
    # pg_get_role_ddl tests
    ######################################################################

    # Basic role
    node.safe_psql("CREATE ROLE regress_role_ddl_test1")
    result = node.safe_psql("SELECT * FROM pg_get_role_ddl('regress_role_ddl_test1')")
    like(
        result,
        r"CREATE ROLE regress_role_ddl_test1 .* NOLOGIN",
        "basic role DDL",
    )

    # Role with multiple privileges
    node.safe_psql(
        """
\tCREATE ROLE regress_role_ddl_test2
\t  LOGIN SUPERUSER CREATEDB CREATEROLE
\t  CONNECTION LIMIT 5
\t  VALID UNTIL '2030-12-31 23:59:59+00'"""
    )
    result = node.safe_psql("SELECT * FROM pg_get_role_ddl('regress_role_ddl_test2')")
    like(result, r"SUPERUSER", "role with SUPERUSER")
    like(result, r"CREATEDB", "role with CREATEDB")
    like(result, r"CONNECTION LIMIT 5", "role with CONNECTION LIMIT")
    like(result, r"VALID UNTIL '2030-12-31", "role with VALID UNTIL")

    # Role with configuration parameters
    node.safe_psql(
        """
\tALTER ROLE regress_role_ddl_test1 SET work_mem TO '256MB';
\tALTER ROLE regress_role_ddl_test1 SET search_path TO myschema, public"""
    )
    result = node.safe_psql("SELECT * FROM pg_get_role_ddl('regress_role_ddl_test1')")
    like(result, r"SET work_mem TO '256MB'", "role with work_mem setting")
    like(result, r"SET search_path TO", "role with search_path setting")

    # Role with database-specific configuration (needs a real database)
    node.safe_psql(
        """
\tCREATE DATABASE regression_ddlutils_test
\t  TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C';
\tALTER ROLE regress_role_ddl_test2
\t  IN DATABASE regression_ddlutils_test SET work_mem TO '128MB'"""
    )
    result = node.safe_psql("SELECT * FROM pg_get_role_ddl('regress_role_ddl_test2')")
    like(
        result,
        r"IN DATABASE regression_ddlutils_test SET work_mem TO '128MB'",
        "role with database-specific setting",
    )

    # Role with special characters (requires quoting)
    node.safe_psql('CREATE ROLE "regress_role-with-dash"')
    result = node.safe_psql("SELECT * FROM pg_get_role_ddl('regress_role-with-dash')")
    like(result, r'"regress_role-with-dash"', "role name requiring quoting")

    # Pretty-printed output
    result = node.safe_psql(
        "SELECT * FROM pg_get_role_ddl('regress_role_ddl_test2', 'pretty', 'true')"
    )
    like(result, r"\n\s+SUPERUSER", "role pretty-print indents attributes")

    # Role with memberships
    node.safe_psql(
        """
\tCREATE ROLE regress_role_ddl_grantor CREATEROLE;
\tCREATE ROLE regress_role_ddl_group1;
\tCREATE ROLE regress_role_ddl_group2;
\tCREATE ROLE regress_role_ddl_member;
\tGRANT regress_role_ddl_group1 TO regress_role_ddl_grantor WITH ADMIN TRUE;
\tGRANT regress_role_ddl_group2 TO regress_role_ddl_grantor WITH ADMIN TRUE;
\tSET ROLE regress_role_ddl_grantor;
\tGRANT regress_role_ddl_group1 TO regress_role_ddl_member
\t  WITH INHERIT TRUE, SET FALSE;
\tGRANT regress_role_ddl_group2 TO regress_role_ddl_member
\t  WITH ADMIN TRUE;
\tRESET ROLE"""
    )
    result = node.safe_psql("SELECT * FROM pg_get_role_ddl('regress_role_ddl_member')")
    like(
        result,
        r"GRANT regress_role_ddl_group1 TO regress_role_ddl_member",
        "role with memberships includes GRANT",
    )
    like(result, r"SET FALSE", "membership includes SET FALSE")
    like(result, r"ADMIN TRUE", "membership includes ADMIN TRUE")

    # Memberships suppressed
    result = node.safe_psql(
        "SELECT * FROM pg_get_role_ddl('regress_role_ddl_member', 'memberships', 'false')"
    )
    unlike(result, r"GRANT", "memberships suppressed")

    # Non-existent role (should error)
    res = node.psql_capture("SELECT * FROM pg_get_role_ddl(9999999::oid)")
    assert res.exit_code != 0, "non-existent role errors"
    like(res.stderr, r"does not exist", "non-existent role error message")

    # NULL input (should return no rows)
    result = node.safe_psql("SELECT count(*) FROM pg_get_role_ddl(NULL)")
    assert result == "0", "NULL role returns no rows"

    # Permission check: revoke SELECT on pg_authid
    node.safe_psql(
        """
\tCREATE ROLE regress_role_ddl_noaccess;
\tREVOKE SELECT ON pg_authid FROM PUBLIC"""
    )
    res = node.psql_capture(
        "SET ROLE regress_role_ddl_noaccess;\n"
        "\t  SELECT * FROM pg_get_role_ddl('regress_role_ddl_test1')"
    )
    assert res.exit_code != 0, "role DDL denied without pg_authid access"
    node.safe_psql(
        """
\tGRANT SELECT ON pg_authid TO PUBLIC"""
    )

    ######################################################################
    # pg_get_database_ddl tests
    ######################################################################

    # Set up: the test database was already created above for role tests.
    node.safe_psql(
        """
\tALTER DATABASE regression_ddlutils_test OWNER TO regress_role_ddl_test2;
\tALTER DATABASE regression_ddlutils_test CONNECTION LIMIT 123;
\tALTER DATABASE regression_ddlutils_test SET random_page_cost = 2.0;
\tALTER ROLE regress_role_ddl_test2
\t  IN DATABASE regression_ddlutils_test SET random_page_cost = 1.1"""
    )

    # Non-existent database
    res = node.psql_capture(
        "SELECT * FROM pg_get_database_ddl('regression_no_such_db')"
    )
    assert res.exit_code != 0, "non-existent database errors"

    # NULL input
    result = node.safe_psql("SELECT count(*) FROM pg_get_database_ddl(NULL)")
    assert result == "0", "NULL database returns no rows"

    # Invalid option
    res = node.psql_capture(
        "SELECT * FROM pg_get_database_ddl('regression_ddlutils_test', 'owner', 'invalid')"
    )
    assert res.exit_code != 0, "invalid boolean option errors"
    like(res.stderr, r"invalid value", "invalid option error message")

    # Duplicate option
    res = node.psql_capture(
        "SELECT * FROM pg_get_database_ddl('regression_ddlutils_test',\n"
        "\t  'owner', 'false', 'owner', 'true')"
    )
    assert res.exit_code != 0, "duplicate option errors"

    # Basic output (without locale details)
    result = _ddl_filter(
        node.safe_psql(
            "SELECT pg_get_database_ddl\n"
            "\t  FROM pg_get_database_ddl('regression_ddlutils_test')"
        )
    )
    like(
        result,
        r"CREATE DATABASE regression_ddlutils_test",
        "database DDL includes CREATE",
    )
    like(result, r"TEMPLATE = template0", "database DDL includes TEMPLATE")
    like(result, r"ENCODING = 'UTF8'", "database DDL includes ENCODING")
    like(
        result,
        r"OWNER TO regress_role_ddl_test2",
        "database DDL includes OWNER",
    )
    like(result, r"CONNECTION LIMIT = 123", "database DDL includes CONNLIMIT")
    like(
        result,
        r"SET random_page_cost TO '2.0'",
        "database DDL includes GUC setting",
    )

    # Pretty-printed output
    result = _ddl_filter(
        node.safe_psql(
            "SELECT pg_get_database_ddl\n"
            "\t  FROM pg_get_database_ddl('regression_ddlutils_test',\n"
            "\t    'pretty', 'true', 'tablespace', 'false')"
        )
    )
    like(result, r"\n\s+WITH TEMPLATE", "database DDL pretty-prints WITH")

    # Permission check
    node.safe_psql(
        """
\tREVOKE CONNECT ON DATABASE regression_ddlutils_test FROM PUBLIC"""
    )
    res = node.psql_capture(
        "SET ROLE regress_role_ddl_noaccess;\n"
        "\t  SELECT * FROM pg_get_database_ddl('regression_ddlutils_test')"
    )
    assert res.exit_code != 0, "database DDL denied without CONNECT"
    node.safe_psql(
        """
\tGRANT CONNECT ON DATABASE regression_ddlutils_test TO PUBLIC"""
    )

    ######################################################################
    # pg_get_tablespace_ddl tests
    ######################################################################

    # Non-existent tablespace by name
    res = node.psql_capture(
        "SELECT * FROM pg_get_tablespace_ddl('regress_nonexistent_tblsp')"
    )
    assert res.exit_code != 0, "non-existent tablespace errors"

    # Non-existent tablespace by OID
    res = node.psql_capture("SELECT * FROM pg_get_tablespace_ddl(0::oid)")
    assert res.exit_code != 0, "non-existent tablespace OID errors"

    # NULL input (name and OID variants)
    result = node.safe_psql("SELECT count(*) FROM pg_get_tablespace_ddl(NULL::name)")
    assert result == "0", "NULL tablespace name returns no rows"
    result = node.safe_psql("SELECT count(*) FROM pg_get_tablespace_ddl(NULL::oid)")
    assert result == "0", "NULL tablespace OID returns no rows"

    # Tablespace name requiring quoting
    node.safe_psql(
        """
\tSET allow_in_place_tablespaces = true;
\tCREATE TABLESPACE "regress_ tblsp" OWNER regress_role_ddl_test1
\t  LOCATION ''"""
    )
    result = node.safe_psql("SELECT * FROM pg_get_tablespace_ddl('regress_ tblsp')")
    like(result, r'"regress_ tblsp"', "tablespace name is quoted")

    # Rename and add options; reuse this tablespace for the remaining tests
    node.safe_psql(
        """
\tALTER TABLESPACE "regress_ tblsp" RENAME TO regress_allopt_tblsp;
\tALTER TABLESPACE regress_allopt_tblsp
\t  SET (seq_page_cost = '1.5', random_page_cost = '1.1234567890',
\t       effective_io_concurrency = '17', maintenance_io_concurrency = '18')"""
    )

    # Tablespace with multiple options
    result = node.safe_psql(
        "SELECT * FROM pg_get_tablespace_ddl('regress_allopt_tblsp')"
    )
    like(
        result,
        r"CREATE TABLESPACE regress_allopt_tblsp",
        "tablespace DDL includes CREATE",
    )
    like(
        result,
        r"OWNER regress_role_ddl_test1",
        "tablespace DDL includes OWNER",
    )
    like(result, r"seq_page_cost='1.5'", "tablespace DDL includes options")

    # Pretty-printed output
    result = node.safe_psql(
        "SELECT * FROM pg_get_tablespace_ddl('regress_allopt_tblsp',\n"
        "\t  'pretty', 'true')"
    )
    like(result, r"\n\s+OWNER", "tablespace DDL pretty-prints OWNER")

    # Owner suppressed
    result = node.safe_psql(
        "SELECT * FROM pg_get_tablespace_ddl('regress_allopt_tblsp',\n"
        "\t  'owner', 'false')"
    )
    unlike(result, r"OWNER", "tablespace DDL owner suppressed")

    # Lookup by OID
    result = node.safe_psql(
        """
\tSELECT pg_get_tablespace_ddl
\tFROM pg_get_tablespace_ddl(
\t  (SELECT oid FROM pg_tablespace
\t   WHERE spcname = 'regress_allopt_tblsp'))"""
    )
    like(
        result,
        r"CREATE TABLESPACE regress_allopt_tblsp",
        "tablespace DDL by OID",
    )

    # Permission check
    node.safe_psql("REVOKE SELECT ON pg_tablespace FROM PUBLIC")
    res = node.psql_capture(
        "SET ROLE regress_role_ddl_noaccess;\n"
        "\t  SELECT * FROM pg_get_tablespace_ddl('regress_allopt_tblsp')"
    )
    assert res.exit_code != 0, "tablespace DDL denied without pg_tablespace access"
    node.safe_psql(
        """
\tGRANT SELECT ON pg_tablespace TO PUBLIC"""
    )

    node.stop()
