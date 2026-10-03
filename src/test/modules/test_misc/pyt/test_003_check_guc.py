# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_misc/t/003_check_guc.pl.

postgresql.conf.sample must stay in sync with guc_tables.c: every in-sample GUC
appears in the file (and vice versa), no GUC marked NOT_IN_SAMPLE appears in the
file, and the file has no tab characters.
"""

import os
import re


def test_003_check_guc(create_pg):
    """postgresql.conf.sample lists exactly the in-sample GUCs, no tabs."""
    node = create_pg("main")
    all_params = (
        node.safe_psql(
            "SELECT name FROM pg_settings\n"
            "WHERE NOT 'NOT_IN_SAMPLE' = ANY (pg_settings_get_flags(name)) AND\n"
            "name <> 'config_file' AND category <> 'Customized Options'\n"
            "ORDER BY 1"
        )
        .lower()
        .split("\n")
    )
    not_in_sample = set(
        node.safe_psql(
            "SELECT name FROM pg_settings\n"
            "WHERE 'NOT_IN_SAMPLE' = ANY (pg_settings_get_flags(name))\nORDER BY 1"
        )
        .lower()
        .split("\n")
    )
    share_dir = node.config_data("--sharedir")
    sample_file = os.path.join(share_dir, "postgresql.conf.sample")
    gucs_in_file = []
    lines_with_tabs = []
    ignore = {"include", "include_dir", "include_if_exists"}
    with open(sample_file, encoding="utf-8") as fh:
        for line_num, line in enumerate(fh, start=1):
            if "\t" in line:
                lines_with_tabs.append(line_num)
            match = re.match(r"^#([_a-zA-Z0-9]+) = .*", line)
            if match:
                name = match.group(1).lower()
                if name not in ignore:
                    gucs_in_file.append(name)
                continue
            assert not re.match(
                r"^\s*[^#\s]", line
            ), "{} missing initial # in postgresql.conf.sample".format(line)
    gucs_set = set(gucs_in_file)
    all_set = set(all_params)
    assert [
        p for p in all_params if p not in gucs_set
    ] == [], "no parameters missing from postgresql.conf.sample"
    assert [
        p for p in gucs_in_file if p not in all_set
    ] == [], "no parameters missing from guc_tables.c"
    assert [
        p for p in gucs_in_file if p in not_in_sample
    ] == [], "no parameters marked as NOT_IN_SAMPLE in postgresql.conf.sample"
    assert not lines_with_tabs, "no lines with tabs in postgresql.conf.sample"
