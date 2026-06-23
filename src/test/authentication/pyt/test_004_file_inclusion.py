# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/authentication/t/004_file_inclusion.pl.

Tests include/include_if_exists/include_dir directives in HBA and ident files.
The HBA and ident entry points are relocated (via ALTER SYSTEM) into
subdirectories, then a structure of files referencing each other through
include directives is generated. After a restart the test compares the contents
of pg_hba_file_rules and pg_ident_file_mappings against the rule/map text built
up alongside the files, verifying line numbers, rule numbers, relative-path
resolution, include_if_exists for missing/present files, include_dir ordering,
and the @file database-name expansion. Requires Unix-domain sockets.
"""

import os
import sys

import pytest


class _Counters:
    """Tracks per-file line numbers and the global hba/ident rule counters.

    Mirrors the %line_counters hash of the Perl original: line_counters[file]
    is the next line number written to that file, while hba_rule/ident_rule are
    the global pg_hba_file_rules.rule_number / pg_ident_file_mappings.map_number
    counters that advance only for non-include entries.
    """

    def __init__(self):
        self.hba_rule = 0
        self.ident_rule = 0
        self.files = {}

    def next_fileline(self, filename):
        self.files[filename] = self.files.get(filename, 0) + 1
        return self.files[filename]

    def next_hba_rule(self):
        self.hba_rule += 1
        return self.hba_rule

    def next_ident_rule(self):
        self.ident_rule += 1
        return self.ident_rule


def _append_conf(node, filename, entry):
    """Append entry (plus newline) to filename, relative to the data dir.

    Creates parent directories as needed, mirroring the way the Perl test relies
    on directories it has already mkdir'd.
    """
    path = node.datadir / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(entry + "\n")


def _basename(filename):
    return os.path.basename(filename)


def _add_hba_line(node, counters, filename, entry):
    """Append entry to an HBA file and return its expected pg_hba_file_rules row.

    Maintains the file-line and global rule counters. An "include" directive
    generates no catalog row (and does not advance the rule counter), so it
    returns the empty string.
    """
    _append_conf(node, filename, entry)
    base_filename = _basename(filename)
    fileline = counters.next_fileline(filename)

    if entry.startswith("include"):
        return ""

    globline = counters.next_hba_rule()
    tokens = entry.split(" ")
    tokens[1] = "{" + tokens[1] + "}"  # database
    tokens[2] = "{" + tokens[2] + "}"  # user_name
    tokens.append("")  # options
    tokens.append("")  # error

    line = ""
    if globline > 1:
        line += "\n"
    line += "{}|{}|{}|".format(globline, base_filename, fileline)
    line += "|".join(tokens)
    return line


def _add_ident_line(node, counters, filename, entry):
    """Append entry to an ident file and return its pg_ident_file_mappings row.

    Like _add_hba_line, but for the ident map catalog; include directives
    generate no row.
    """
    base_filename = _basename(filename)
    _append_conf(node, filename, entry)
    fileline = counters.next_fileline(filename)

    if entry.startswith("include"):
        return ""

    globline = counters.next_ident_rule()
    tokens = entry.split(" ")
    tokens.append("")  # error

    line = ""
    if globline > 1:
        line += "\n"
    line += "{}|{}|{}|".format(globline, base_filename, fileline)
    line += "|".join(tokens)
    return line


def _build_hba_structure(node, counters, hba_file):
    """Generate the HBA file structure with include directives.

    Returns the expected concatenated pg_hba_file_rules contents.
    """
    expected = ""

    for sub in ("subdir1", "hba_inc", "hba_inc_if", "hba_pos"):
        (node.datadir / sub).mkdir(parents=True, exist_ok=True)

    # First, make sure that we will always be able to connect.
    expected += _add_hba_line(node, counters, hba_file, "local all all trust")

    # "include". As hba_file lives in subdir1, pg_hba_pre.conf is at the root of
    # the data directory.
    expected += _add_hba_line(node, counters, hba_file, "include ../pg_hba_pre.conf")
    expected += _add_hba_line(node, counters, "pg_hba_pre.conf", "local pre all reject")
    expected += _add_hba_line(node, counters, hba_file, "local all all reject")
    _add_hba_line(node, counters, hba_file, "include ../hba_pos/pg_hba_pos.conf")
    expected += _add_hba_line(
        node, counters, "hba_pos/pg_hba_pos.conf", "local pos all reject"
    )
    # A relative include path is resolved from the base location of the file it
    # is loaded from.
    expected += _add_hba_line(
        node, counters, "hba_pos/pg_hba_pos.conf", "include pg_hba_pos2.conf"
    )
    expected += _add_hba_line(
        node, counters, "hba_pos/pg_hba_pos2.conf", "local pos2 all reject"
    )
    expected += _add_hba_line(
        node, counters, "hba_pos/pg_hba_pos2.conf", "local pos3 all reject"
    )

    # include_if_exists: missing file, no catalog entries.
    expected += _add_hba_line(
        node, counters, hba_file, "include_if_exists ../hba_inc_if/none"
    )
    # File with some contents loaded.
    expected += _add_hba_line(
        node, counters, hba_file, "include_if_exists ../hba_inc_if/some"
    )
    expected += _add_hba_line(
        node, counters, "hba_inc_if/some", "local if_some all reject"
    )

    # include_dir
    expected += _add_hba_line(node, counters, hba_file, "include_dir ../hba_inc")
    expected += _add_hba_line(
        node, counters, "hba_inc/01_z.conf", "local dir_z all reject"
    )
    expected += _add_hba_line(
        node, counters, "hba_inc/02_a.conf", "local dir_a all reject"
    )
    # Garbage file not suffixed by .conf, so it is ignored.
    _append_conf(node, "hba_inc/garbageconf", "should not be included")

    # Authentication file expanded in an existing entry for database names.
    # As it is expanded, ignore the output generated.
    _add_hba_line(node, counters, hba_file, "local @../dbnames.conf all reject")
    _append_conf(node, "dbnames.conf", "db1")
    _append_conf(node, "dbnames.conf", "db3")
    expected += (
        "\n"
        + str(counters.hba_rule)
        + "|"
        + _basename(hba_file)
        + "|"
        + str(counters.files[hba_file])
        + "|local|{db1,db3}|{all}|reject||"
    )
    return expected


def _build_ident_structure(node, counters, ident_file):
    """Generate the ident file structure with include directives.

    Returns the expected concatenated pg_ident_file_mappings contents.
    """
    expected = ""

    for sub in ("subdir2", "ident_inc", "ident_inc_if", "ident_pos"):
        (node.datadir / sub).mkdir(parents=True, exist_ok=True)

    # include. pg_ident_pre.conf is at the root of the data directory.
    expected += _add_ident_line(
        node, counters, ident_file, "include ../pg_ident_pre.conf"
    )
    expected += _add_ident_line(node, counters, "pg_ident_pre.conf", "pre foo bar")
    expected += _add_ident_line(node, counters, ident_file, "test a b")
    expected += _add_ident_line(
        node, counters, ident_file, "include ../ident_pos/pg_ident_pos.conf"
    )
    expected += _add_ident_line(
        node, counters, "ident_pos/pg_ident_pos.conf", "pos foo bar"
    )
    # A relative include path is resolved from the base location of the file it
    # is loaded from.
    expected += _add_ident_line(
        node, counters, "ident_pos/pg_ident_pos.conf", "include pg_ident_pos2.conf"
    )
    expected += _add_ident_line(
        node, counters, "ident_pos/pg_ident_pos2.conf", "pos2 foo bar"
    )
    expected += _add_ident_line(
        node, counters, "ident_pos/pg_ident_pos2.conf", "pos3 foo bar"
    )

    # include_if_exists: missing file, no catalog entries.
    expected += _add_ident_line(
        node, counters, ident_file, "include_if_exists ../ident_inc_if/none"
    )
    # File with some contents loaded.
    expected += _add_ident_line(
        node, counters, ident_file, "include_if_exists ../ident_inc_if/some"
    )
    expected += _add_ident_line(node, counters, "ident_inc_if/some", "if_some foo bar")

    # include_dir
    expected += _add_ident_line(node, counters, ident_file, "include_dir ../ident_inc")
    expected += _add_ident_line(node, counters, "ident_inc/01_z.conf", "dir_z foo bar")
    expected += _add_ident_line(node, counters, "ident_inc/02_a.conf", "dir_a foo bar")
    # Garbage file not suffixed by .conf, so it is ignored.
    _append_conf(node, "ident_inc/garbageconf", "should not be included")

    return expected


_HBA_QUERY = """SELECT rule_number,
  regexp_replace(file_name, '.*/', ''),
  line_number,
  type,
  database,
  user_name,
  auth_method,
  options,
  error
 FROM pg_hba_file_rules ORDER BY rule_number;"""

_IDENT_QUERY = """SELECT map_number,
  regexp_replace(file_name, '.*/', ''),
  line_number,
  map_name,
  sys_name,
  pg_username,
  error
 FROM pg_ident_file_mappings ORDER BY map_number"""


@pytest.mark.skipif(sys.platform == "win32", reason="needs Unix-domain sockets")
def test_004_file_inclusion(create_pg):
    """HBA/ident include directives reflect correctly in the catalog views."""
    # Locations for the entry points of the HBA and ident files.
    hba_file = "subdir1/pg_hba_custom.conf"
    ident_file = "subdir2/pg_ident_custom.conf"

    node = create_pg("primary")
    data_dir = node.datadir
    counters = _Counters()

    # Customise main auth file names.
    node.safe_psql("ALTER SYSTEM SET hba_file = '{}/{}'".format(data_dir, hba_file))
    node.safe_psql("ALTER SYSTEM SET ident_file = '{}/{}'".format(data_dir, ident_file))

    # Remove the original ones; this node links to non-default ones now.
    (data_dir / "pg_hba.conf").unlink(missing_ok=True)
    (data_dir / "pg_ident.conf").unlink(missing_ok=True)

    hba_expected = _build_hba_structure(node, counters, hba_file)
    ident_expected = _build_ident_structure(node, counters, ident_file)

    node.restart()

    contents = node.safe_psql(_HBA_QUERY)
    assert contents == hba_expected, "check contents of pg_hba_file_rules"

    contents = node.safe_psql(_IDENT_QUERY)
    assert contents == ident_expected, "check contents of pg_ident_file_mappings"
