# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/scripts/t/070_dropuser.pl."""


def test_dropuser(pg_bin, create_pg):
    """dropuser drops a role and fails for a nonexistent one."""
    pg_bin.program_help_ok("dropuser")
    pg_bin.program_version_ok("dropuser")
    pg_bin.program_options_handling_ok("dropuser")

    node = create_pg("main")

    node.safe_psql("CREATE ROLE regress_foobar1")
    node.issues_sql_like(
        ["dropuser", "regress_foobar1"],
        r"statement: DROP ROLE regress_foobar1",
        "SQL DROP ROLE run",
    )

    node.command_fails_like(
        ["dropuser", "regress_nonexistent"],
        r'role "regress_nonexistent" does not exist',
        "fails with nonexistent user",
    )
