# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/interfaces/ecpg/preproc/t/001_ecpg_err_warn_msg.pl.

ecpg preprocessor error/warning messages: ecpg with no arguments fails, and compiling t/err_warn_msg.pgc fails with the expected diagnostic set.
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_001_ecpg_err_warn_msg(pg_bin):
    """ecpg preprocessor error/warning messages."""
    pg_bin.program_help_ok("ecpg")
    pg_bin.program_version_ok("ecpg")
    pg_bin.program_options_handling_ok("ecpg")
    pg_bin.command_fails(["ecpg"], "ecpg without arguments fails")
    pg_bin.command_checks_all(
        ["ecpg", "t/err_warn_msg.pgc"],
        3,
        [r""""""],
        [
            r"""ERROR: AT option not allowed in CONNECT statement""",
            r"""ERROR: AT option not allowed in DISCONNECT statement""",
            r"""ERROR: AT option not allowed in SET CONNECTION statement""",
            r"""ERROR: AT option not allowed in TYPE statement""",
            r"""ERROR: AT option not allowed in WHENEVER statement""",
            r"""ERROR: AT option not allowed in VAR statement""",
            r"""WARNING: COPY FROM STDIN is not implemented""",
            r"""ERROR: using variable "cursor_var" in different declare statements is not supported""",
            r"""ERROR: cursor "duplicate_cursor" is already defined""",
            r"""ERROR: SHOW ALL is not implemented""",
            r"""WARNING: no longer supported LIMIT""",
            r"""WARNING: cursor "duplicate_cursor" has been declared but not opened""",
            r"""WARNING: cursor "duplicate_cursor" has been declared but not opened""",
            r"""WARNING: cursor ":cursor_var" has been declared but not opened""",
            r"""WARNING: cursor ":cursor_var" has been declared but not opened""",
        ],
        "ecpg with errors and warnings",
    )
