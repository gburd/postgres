# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/interfaces/ecpg/preproc/t/002_ecpg_err_warn_msg_informix.pl.

ecpg preprocessor error/warning messages in INFORMIX mode: compiling t/err_warn_msg_informix.pgc fails with the expected diagnostic set.
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_002_ecpg_err_warn_msg_informix(pg_bin):
    """ecpg preprocessor error/warning messages in INFORMIX mode."""
    pg_bin.command_checks_all(
        ["ecpg", "-C", "INFORMIX", "t/err_warn_msg_informix.pgc"],
        3,
        [r""""""],
        [
            r"""ERROR: AT option not allowed in CLOSE DATABASE statement""",
            r"""ERROR: "database" cannot be used as cursor name in INFORMIX mode""",
        ],
        "ecpg in INFORMIX mode with errors and warnings",
    )
