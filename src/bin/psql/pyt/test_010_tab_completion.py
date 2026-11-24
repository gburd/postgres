# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/psql/t/010_tab_completion.pl.

Drives an interactive (PTY) psql session and checks that readline tab
completion responds as expected: command/keyword completion, table and
schema-qualified name completion (with quoting and case folding), filename and
enum/timezone label completion, GUC and psql-variable completion, and the
FOR PORTION OF column completions for DELETE/UPDATE.

Like the Perl original this does nothing unless the build is --with-readline
(the with_readline env var is 'yes'), is not disabled via SKIP_READLINE_TESTS,
and a PTY is available (not Windows).
"""

import os
import re
import sys
import tempfile

import pytest


def _check_completion(handle, send, pattern, annotation):
    """Send input, wait for the accumulated terminal output to match pattern."""
    regex = pattern if hasattr(pattern, "search") else re.compile(pattern)
    out = handle.query_until(regex, send)
    assert regex.search(out) and not handle.timed_out, annotation


def _clear_query(handle):
    """Clear the query buffer to start over (won't work inside a string)."""
    _check_completion(
        handle,
        "\\r\n",
        re.compile(r"Query buffer reset.*postgres=# ", re.S),
        "\\r works",
    )


def _clear_line(handle):
    """Clear the current line to start over (works in an incomplete literal)."""
    _check_completion(handle, "\025\n", r"postgres=# ", "control-U works")


def _basic_completions(handle):
    """Command, table-name, case-folding and quoted-name completions."""
    # check basic command completion: SEL<tab> produces SELECT<space>
    _check_completion(handle, "SEL\t", r"SELECT ", "complete SEL<tab> to SELECT")

    _clear_query(handle)

    # check case variation is honored
    _check_completion(handle, "sel\t", r"select ", "complete sel<tab> to select")

    # check basic table name completion
    _check_completion(handle, "* from t\t", r"\* from tab1 ", "complete t<tab> to tab1")

    _clear_query(handle)

    # check table name completion with multiple alternatives
    # note: readline might print a bell before the completion
    _check_completion(
        handle,
        "select * from my\t",
        r"select \* from my\a?tab",
        "complete my<tab> to mytab when there are multiple choices",
    )

    # some versions of readline/libedit require two tabs here, some only need one
    _check_completion(
        handle,
        "\t\t",
        r"mytab123 +mytab246",
        "offer multiple table choices",
    )

    _check_completion(
        handle, "2\t", r"246 ", "finish completion of one of multiple table choices"
    )

    _clear_query(handle)


def _quoted_completions(handle):
    """Quoted, mixed-case, case-folded and schema-qualified name completions."""
    # check handling of quoted names
    _check_completion(
        handle,
        'select * from "my\t',
        r'select \* from "my\a?tab',
        'complete "my<tab> to "mytab when there are multiple choices',
    )

    _check_completion(
        handle,
        "\t\t",
        r'"mytab123" +"mytab246"',
        "offer multiple quoted table choices",
    )

    _check_completion(
        handle,
        "2\t",
        r'246" ',
        "finish completion of one of multiple quoted table choices",
    )

    _clear_query(handle)

    # check handling of mixed-case names
    _check_completion(
        handle, 'select * from "mi\t', r'"mixedName" ', "complete a mixed-case name"
    )

    _clear_query(handle)

    # check case folding
    _check_completion(
        handle, "select * from TAB\t", r"tab1 ", "automatically fold case"
    )

    _clear_query(handle)

    # check case-sensitive keyword replacement
    # note: various versions of readline/libedit handle backspacing
    # differently, so just check that the replacement comes out correctly
    _check_completion(handle, "\\DRD\t", r"drds ", "complete \\DRD<tab> to \\drds")

    _clear_query(handle)

    # check completion of a schema-qualified name
    _check_completion(
        handle, "select * from pub\t", r"public\.", "complete schema when relevant"
    )

    _check_completion(handle, "tab\t", r"tab1 ", "complete schema-qualified name")

    _clear_query(handle)

    _check_completion(
        handle,
        "select * from PUBLIC.t\t",
        r"public\.tab1 ",
        "automatically fold case in schema-qualified name",
    )

    _clear_query(handle)


def _refname_completions(handle):
    """Completions that interpret referenced (constraint/qualified) names."""
    # check interpretation of referenced names
    _check_completion(
        handle,
        "alter table tab1 drop constraint t\t",
        r"tab1_pkey ",
        "complete index name for referenced table",
    )

    _clear_query(handle)

    _check_completion(
        handle,
        "alter table TAB1 drop constraint t\t",
        r"tab1_pkey ",
        "complete index name for referenced table, with downcasing",
    )

    _clear_query(handle)

    _check_completion(
        handle,
        'alter table public."tab1" drop constraint t\t',
        r"tab1_pkey ",
        "complete index name for referenced table, with schema and quoting",
    )

    _clear_query(handle)

    # check variant where we're completing a qualified name from a refname
    # (this one also checks successful completion in a multiline command)
    _check_completion(
        handle,
        "comment on constraint tab1_pkey \n on public.\t",
        r"public\.tab1",
        "complete qualified name from object reference",
    )

    _clear_query(handle)


def _filename_completions(handle):
    """Filename completions for \\lo_import and COPY (quoted)."""
    # check filename completion
    _check_completion(
        handle,
        "\\lo_import tab_comp_dir/some\t",
        r"tab_comp_dir/somefile ",
        "filename completion with one possibility",
    )

    _clear_query(handle)

    # note: readline might print a bell before the completion
    _check_completion(
        handle,
        "\\lo_import tab_comp_dir/af\t",
        r"tab_comp_dir/af\a?ile",
        "filename completion with multiple possibilities",
    )

    # here we are inside a string literal 'afile*', so must use clear_line().
    _clear_line(handle)

    # COPY requires quoting
    _check_completion(
        handle,
        "COPY foo FROM tab_comp_dir/some\t",
        r"'tab_comp_dir/somefile' ",
        "quoted filename completion with one possibility",
    )

    _clear_query(handle)

    _check_completion(
        handle,
        "COPY foo FROM tab_comp_dir/af\t",
        r"'tab_comp_dir/afile",
        "quoted filename completion with multiple possibilities",
    )

    # some versions of readline/libedit require two tabs here, some only need
    # one; also, some will offer the whole path name and some just the file
    # name; the quotes might appear, too
    _check_completion(
        handle,
        "\t\t",
        r"afile123'? +'?(tab_comp_dir/)?afile456",
        "offer multiple file choices",
    )

    _clear_line(handle)


def _enum_tz_completions(handle):
    """Enum label and timezone name completions."""
    # check enum label completion
    # some versions of readline/libedit require two tabs here, some only need
    # one; also, some versions will offer quotes, some will not
    _check_completion(
        handle,
        "ALTER TYPE enum1 RENAME VALUE 'ba\t\t",
        r"'?bar'? +'?baz'?",
        "offer multiple enum choices",
    )

    _clear_line(handle)

    # enum labels are case sensitive, so this should complete BLACK immediately
    _check_completion(
        handle,
        "ALTER TYPE enum1 RENAME VALUE 'B\t",
        r"BLACK",
        "enum labels are case sensitive",
    )

    _clear_line(handle)

    # check timezone name completion
    _check_completion(
        handle, "SET timezone TO am\t", r"'America/", "offer partial timezone name"
    )

    _check_completion(handle, "new_\t", r"New_York", "complete partial timezone name")

    _clear_line(handle)


def _keyword_case_completions(handle):
    """Keyword offered with object names obeys COMP_KEYWORD_CASE; plus more."""
    # check completion of a keyword offered in addition to object names;
    # such a keyword should obey COMP_KEYWORD_CASE
    for case, in_, out in (
        ("lower", "CO", "column"),
        ("upper", "co", "COLUMN"),
        ("preserve-lower", "co", "column"),
        ("preserve-upper", "CO", "COLUMN"),
    ):
        _check_completion(
            handle,
            "\\set COMP_KEYWORD_CASE {case}\n".format(case=case),
            r"postgres=#",
            "set completion case to '{case}'".format(case=case),
        )
        _check_completion(
            handle,
            "alter table tab1 rename {in_}\t\t\t".format(in_=in_),
            out,
            "offer keyword {out} for input {in_}<TAB>, "
            "COMP_KEYWORD_CASE = {case}".format(out=out, in_=in_, case=case),
        )
        _clear_query(handle)

    # alternate path where keyword comes from SchemaQuery
    _check_completion(
        handle,
        "DROP TYPE big\t",
        r"DROP TYPE bigint ",
        "offer keyword from SchemaQuery",
    )

    _clear_query(handle)

    # check create_command_generator
    _check_completion(
        handle, "CREATE TY\t", r"CREATE TYPE ", "check create_command_generator"
    )

    _clear_query(handle)

    # check words_after_create infrastructure
    _check_completion(
        handle,
        "CREATE TABLE mytab\t\t",
        r"mytab123 +mytab246",
        "check words_after_create",
    )

    _clear_query(handle)

    # check VersionedQuery infrastructure
    _check_completion(
        handle,
        "DROP PUBLIC\t \t\t",
        r"DROP PUBLICATION\s+some_publication ",
        "check VersionedQuery",
    )

    _clear_query(handle)

    # hits ends_with() and logic for completing in multi-line queries
    _check_completion(
        handle, "analyze (\n\t\t", r"VERBOSE", "check ANALYZE (VERBOSE ..."
    )

    _clear_query(handle)


def _guc_var_completions(handle):
    """GUC name/value and psql-variable completions."""
    # check completions for GUCs
    _check_completion(
        handle, "set interval\t\t", r"intervalstyle TO", "complete a GUC name"
    )
    _check_completion(handle, " iso\t", r"iso_8601 ", "complete a GUC enum value")

    _clear_query(handle)

    # same, for qualified GUC names
    _check_completion(
        handle,
        "DO $$begin end$$ LANGUAGE plpgsql;\n",
        r"postgres=# ",
        "load plpgsql extension",
    )

    _check_completion(
        handle, "set plpg\t", r"plpg\a?sql\.", "complete prefix of a GUC name"
    )
    _check_completion(
        handle, "var\t\t", r"variable_conflict TO", "complete a qualified GUC name"
    )
    _check_completion(
        handle, " USE_C\t", r"use_column", "complete a qualified GUC enum value"
    )

    _clear_query(handle)

    # check completions for psql variables
    _check_completion(
        handle, "\\set VERB\t", r"VERBOSITY ", "complete a psql variable name"
    )
    _check_completion(handle, "def\t", r"default ", "complete a psql variable value")

    _clear_query(handle)

    _check_completion(
        handle,
        "\\echo :VERB\t",
        r":VERBOSITY ",
        "complete an interpolated psql variable name",
    )

    _clear_query(handle)

    # check completion for psql variable test
    _check_completion(
        handle,
        "\\echo :{?VERB\t",
        r":\{\?VERBOSITY} ",
        "complete a psql variable test",
    )

    _clear_query(handle)

    # check no-completions code path
    _check_completion(handle, "blarg \t\t", r"", "check completion failure path")

    _clear_query(handle)

    # check COPY FROM with DEFAULT option
    _check_completion(
        handle,
        "COPY foo FROM stdin WITH ( DEF\t)",
        r"DEFAULT ",
        "COPY FROM with DEFAULT completion",
    )

    _clear_line(handle)


def _portion_of_completions(handle, verb, table_in):
    """Tab completion for DELETE/UPDATE ... FOR PORTION OF."""
    _check_completion(
        handle,
        table_in,
        r"FOR ",
        "complete {verb} <table> F<tab> to FOR".format(verb=verb),
    )

    _check_completion(handle, "P\t", r"PORTION ", "complete FOR P<tab> to PORTION")

    _check_completion(handle, "O\t", r"OF ", "complete PORTION O<tab> to OF")

    _check_completion(
        handle, "v\t", r"valid_at ", "complete FOR PORTION OF offers column names"
    )

    _check_completion(
        handle, "FR\t", r"FROM ", "complete FOR PORTION OF <col> FR<tab> to FROM"
    )

    _clear_query(handle)


def _setup_objects(node):
    """Create the database objects the completion checks reference."""
    node.safe_psql(
        "CREATE TABLE tab1 (c1 int primary key constraint foo not null, c2 text);\n"
        "CREATE TABLE mytab123 (f1 int, f2 text);\n"
        "CREATE TABLE mytab246 (f1 int, f2 text);\n"
        'CREATE TABLE "mixedName" (f1 int, f2 text);\n'
        "CREATE TYPE enum1 AS ENUM ('foo', 'bar', 'baz', 'BLACK');\n"
        "CREATE PUBLICATION some_publication;\n"
        "CREATE TABLE fpo_test (id int4range, valid_at daterange, name text);\n"
    )


def _make_junk_files():
    """Create the tab_comp_dir junk files for filename completion testing."""
    os.makedirs("tab_comp_dir", exist_ok=True)
    with open("tab_comp_dir/somefile", "w", encoding="utf-8") as fh:
        fh.write("some stuff\n")
    with open("tab_comp_dir/afile123", "w", encoding="utf-8") as fh:
        fh.write("more stuff\n")
    with open("tab_comp_dir/afile456", "w", encoding="utf-8") as fh:
        fh.write("other stuff\n")


def test_010_tab_completion(create_pg, monkeypatch):
    """psql readline tab completion responds as expected (skips w/o readline)."""
    # Do nothing unless the build is --with-readline.
    if os.environ.get("with_readline") != "yes":
        pytest.skip("readline is not supported by this build")
    # Also, skip if user has set environment variable to command that. This is
    # mainly intended to allow working around some of the more broken versions
    # of libedit --- some users might find them acceptable even if they won't
    # pass these tests.
    if os.environ.get("SKIP_READLINE_TESTS"):
        pytest.skip("SKIP_READLINE_TESTS is set")
    # If we don't have a PTY, forget it (the Perl IO::Pty requirement).
    if sys.platform == "win32":
        pytest.skip("a PTY is needed to run this test")

    # start a new server
    node = create_pg("main")

    # set up a few database objects
    _setup_objects(node)

    # In a VPATH build, we'll be started in the source directory, but we want
    # to run in the build directory so that we can use relative paths to access
    # the tab_comp_dir subdirectory; otherwise the output from filename
    # completion tests is too variable.
    testdatadir = os.environ.get("TESTDATADIR")
    monkeypatch.chdir(testdatadir if testdatadir else tempfile.mkdtemp())

    # Create some junk files for filename completion testing.
    _make_junk_files()

    # Arrange to capture, not discard, the interactive session's history
    # output. Put it in the test log directory, so that buildfarm runs capture
    # the result for possible debugging purposes.
    logdir = os.environ.get("TESTLOGDIR")
    if logdir:
        historyfile = os.path.join(logdir, "010_psql_history.txt")
    else:
        historyfile = os.path.join(tempfile.mkdtemp(), "010_psql_history.txt")

    # fire up an interactive psql session and configure it such that each query
    # restarts the timer
    handle = node.interactive_psql("postgres", history_file=historyfile)
    handle.set_query_timer_restart()

    _basic_completions(handle)
    _quoted_completions(handle)
    _refname_completions(handle)
    _filename_completions(handle)
    _enum_tz_completions(handle)
    _keyword_case_completions(handle)
    _guc_var_completions(handle)

    # check tab completion for DELETE ... FOR PORTION OF
    _portion_of_completions(handle, "DELETE FROM", "DELETE FROM fpo_test F\t")
    # check tab completion for UPDATE ... FOR PORTION OF
    _portion_of_completions(handle, "UPDATE", "UPDATE fpo_test F\t")

    # send psql an explicit \q to shut it down, else pty won't close properly
    handle.quit()

    # done
    node.stop()
