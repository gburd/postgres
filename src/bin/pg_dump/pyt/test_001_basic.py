# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/bin/pg_dump/t/001_basic.pl.

pg_dump/pg_restore command-line option validation: invalid options, mutually exclusive options, compression specs, and required-argument errors.
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_001_basic(pg_bin):
    """pg_dump/pg_restore command-line option validation."""
    pg_bin.program_help_ok("pg_dump")
    pg_bin.program_version_ok("pg_dump")
    pg_bin.program_options_handling_ok("pg_dump")
    pg_bin.program_help_ok("pg_restore")
    pg_bin.program_version_ok("pg_restore")
    pg_bin.program_options_handling_ok("pg_restore")
    pg_bin.program_help_ok("pg_dumpall")
    pg_bin.program_version_ok("pg_dumpall")
    pg_bin.program_options_handling_ok("pg_dumpall")
    pg_bin.command_fails_like(
        ["pg_dump", "qqq", "abc"],
        r"""pg_dump:\ error:\ too\ many\ command\-line\ arguments\ \(first\ is\ "abc"\)""",
        "pg_dump: too many command-line arguments",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "qqq", "abc"],
        r"""pg_restore:\ error:\ too\ many\ command\-line\ arguments\ \(first\ is\ "abc"\)""",
        "pg_restore: too many command-line arguments",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "qqq", "abc"],
        r"""pg_dumpall:\ error:\ too\ many\ command\-line\ arguments\ \(first\ is\ "qqq"\)""",
        "pg_dumpall: too many command-line arguments",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "-s", "-a"],
        r"""pg_dump:\ error:\ options\ \-a/\-\-data\-only\ and\ \-s/\-\-schema\-only\ cannot\ be\ used\ together""",
        "pg_dump: options -a/--data-only and -s/--schema-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "-s", "--statistics-only"],
        r"""pg_dump:\ error:\ options\ \-s/\-\-schema\-only\ and\ \-\-statistics\-only\ cannot\ be\ used\ together""",
        "pg_dump: error: options -s/--schema-only and --statistics-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "-a", "--statistics-only"],
        r"""pg_dump:\ error:\ options\ \-a/\-\-data\-only\ and\ \-\-statistics\-only\ cannot\ be\ used\ together""",
        "pg_dump: error: options -a/--data-only and --statistics-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "-s", "--include-foreign-data=xxx"],
        r"""pg_dump:\ error:\ options\ \-\-include\-foreign\-data\ and\ \-s/\-\-schema\-only\ cannot\ be\ used\ together""",
        "pg_dump: options --include-foreign-data and -s/--schema-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "--statistics-only", "--no-statistics"],
        r"""pg_dump:\ error:\ options\ \-\-statistics\-only\ and\ \-\-no\-statistics\ cannot\ be\ used\ together""",
        "pg_dump: options --statistics-only and --no-statistics cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "-j2", "--include-foreign-data=xxx"],
        r"""pg_dump:\ error:\ option\ \-\-include\-foreign\-data\ is\ not\ supported\ with\ parallel\ backup""",
        "pg_dump: option --include-foreign-data is not supported with parallel backup",
    )
    pg_bin.command_fails_like(
        ["pg_restore"],
        r"""pg_restore:\ error:\ one\ of\ \-d/\-\-dbname\ and\ \-f/\-\-file\ must\ be\ specified""",
        "pg_restore: error: one of -d/--dbname and -f/--file must be specified",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "-s", "-a", "-f -"],
        r"""pg_restore:\ error:\ options\ \-a/\-\-data\-only\ and\ \-s/\-\-schema\-only\ cannot\ be\ used\ together""",
        "pg_restore: options -a/--data-only and -s/--schema-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "-d", "xxx", "-f", "xxx"],
        r"""pg_restore:\ error:\ options\ \-d/\-\-dbname\ and\ \-f/\-\-file\ cannot\ be\ used\ together""",
        "pg_restore: options -d/--dbname and -f/--file cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "-c", "-a"],
        r"""pg_dump:\ error:\ options\ \-c/\-\-clean\ and\ \-a/\-\-data\-only\ cannot\ be\ used\ together""",
        "pg_dump: options -c/--clean and -a/--data-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "-c", "-a"],
        r"""pg_dumpall:\ error:\ options\ \-c/\-\-clean\ and\ \-a/\-\-data\-only\ cannot\ be\ used\ together""",
        "pg_dumpall: options -c/--clean and -a/--data-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "-c", "-a", "-f -"],
        r"""pg_restore:\ error:\ options\ \-c/\-\-clean\ and\ \-a/\-\-data\-only\ cannot\ be\ used\ together""",
        "pg_restore: options -c/--clean and -a/--data-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "--if-exists"],
        r"""pg_dump:\ error:\ option\ \-\-if\-exists\ requires\ option\ \-c/\-\-clean""",
        "pg_dump: option --if-exists requires option -c/--clean",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "-j3"],
        r"""pg_dump:\ error:\ parallel\ backup\ only\ supported\ by\ the\ directory\ format""",
        "pg_dump: parallel backup only supported by the directory format",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "-j", "-1 "],
        r"""pg_dump:\ error:\ \-j/\-\-jobs\ must\ be\ in\ range""",
        "pg_dump: -j/--jobs must be in range",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "-F", "garbage"],
        r"""pg_dump:\ error:\ invalid\ output\ format""",
        "pg_dump: invalid output format",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "-j", "-1", "-f -"],
        r"""pg_restore:\ error:\ \-j/\-\-jobs\ must\ be\ in\ range""",
        "pg_restore: -j/--jobs must be in range",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "--single-transaction", "-j3", "-f -"],
        r"""pg_restore:\ error:\ cannot\ specify\ both\ \-\-single\-transaction\ and\ multiple\ jobs""",
        "pg_restore: cannot specify both --single-transaction and multiple jobs",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "--compress", "garbage"],
        r"""pg_dump:\ error:\ unrecognized\ compression\ algorithm""",
        "pg_dump: invalid --compress",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "--compress", "none:1"],
        r"""pg_dump:\ error:\ invalid\ compression\ specification:\ compression\ algorithm\ "none"\ does\ not\ accept\ a\ compression\ level""",
        'pg_dump: invalid compression specification: compression algorithm "none" does not accept a compression level',
    )
    if pg_bin.check_pg_config("#define HAVE_LIBZ 1"):
        pg_bin.command_fails_like(
            ["pg_dump", "-Z", "15"],
            r"""pg_dump:\ error:\ invalid\ compression\ specification:\ compression\ algorithm\ "gzip"\ expects\ a\ compression\ level\ between\ 1\ and\ 9\ \(default\ at\ \-1\)""",
            "pg_dump: invalid compression specification: must be in range",
        )
        pg_bin.command_fails_like(
            ["pg_dump", "--compress", "1", "--format", "tar"],
            r"""pg_dump:\ error:\ compression\ is\ not\ supported\ by\ tar\ archive\ format""",
            "pg_dump: compression is not supported by tar archive format",
        )
        pg_bin.command_fails_like(
            ["pg_dump", "-Z", "gzip:nonInt"],
            r'''pg_dump:\ error:\ invalid\ compression\ specification:\ unrecognized\ compression\ option:\ "nonInt"''',
            "pg_dump: invalid compression specification: must be an integer",
        )
    else:
        pg_bin.command_fails_like(
            ["pg_dump", "--format", "tar", "-j3"],
            r"""pg_dump:\ error:\ parallel\ backup\ only\ supported\ by\ the\ directory\ format""",
            "pg_dump: warning: parallel backup not supported by tar format",
        )
        pg_bin.command_fails_like(
            ["pg_dump", "-Z", "gzip:nonInt", "--format", "tar", "-j2"],
            r"""pg_dump:\ error:\ invalid\ compression\ specification:\ unrecognized\ compression\ option""",
            "pg_dump: invalid compression specification: must be an integer",
        )
    pg_bin.command_fails_like(
        ["pg_dump", "--extra-float-digits", "-16"],
        r"""pg_dump:\ error:\ \-\-extra\-float\-digits\ must\ be\ in\ range""",
        "pg_dump: --extra-float-digits must be in range",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "--rows-per-insert", "0"],
        r"""pg_dump:\ error:\ \-\-rows\-per\-insert\ must\ be\ in\ range""",
        "pg_dump: --rows-per-insert must be in range",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "--if-exists", "-f -"],
        r"""pg_restore:\ error:\ option\ \-\-if\-exists\ requires\ option\ \-c/\-\-clean""",
        "pg_restore: option --if-exists requires option -c/--clean",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "-f -", "-F", "garbage"],
        r"""pg_restore:\ error:\ unrecognized\ archive\ format\ "garbage";""",
        "pg_restore: unrecognized archive format",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "-f -", "-F", ""],
        r"""pg_restore:\ error:\ unrecognized\ archive\ format\ "";""",
        "pg_restore: empty archive format",
    )
    pg_bin.command_fails_like(
        ["pg_dump", "--on-conflict-do-nothing"],
        r"""pg_dump: error: option --on-conflict-do-nothing requires option --inserts, --rows-per-insert, or --column-inserts""",
        "pg_dump: --on-conflict-do-nothing requires --inserts, --rows-per-insert, --column-inserts",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "-g", "-r"],
        r"""pg_dumpall:\ error:\ options\ \-g/\-\-globals\-only\ and\ \-r/\-\-roles\-only\ cannot\ be\ used\ together""",
        "pg_dumpall: options -g/--globals-only and -r/--roles-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "-g", "-t"],
        r"""pg_dumpall:\ error:\ options\ \-g/\-\-globals\-only\ and\ \-t/\-\-tablespaces\-only\ cannot\ be\ used\ together""",
        "pg_dumpall: options -g/--globals-only and -t/--tablespaces-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "-r", "-t"],
        r"""pg_dumpall:\ error:\ options\ \-r/\-\-roles\-only\ and\ \-t/\-\-tablespaces\-only\ cannot\ be\ used\ together""",
        "pg_dumpall: options -r/--roles-only and -t/--tablespaces-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "--if-exists"],
        r"""pg_dumpall:\ error:\ option\ \-\-if\-exists\ requires\ option\ \-c/\-\-clean""",
        "pg_dumpall: option --if-exists requires option -c/--clean",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "-C", "-1", "-f -"],
        r"""pg_restore:\ error:\ options\ \-C/\-\-create\ and\ \-1/\-\-single\-transaction\ cannot\ be\ used\ together""",
        "pg_restore: options -C\\/--create and -1\\/--single-transaction cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "--exclude-database=foo", "--globals-only"],
        r"""pg_dumpall:\ error:\ options\ \-\-exclude\-database\ and\ \-g/\-\-globals\-only\ cannot\ be\ used\ together""",
        "pg_dumpall: options --exclude-database and -g/--globals-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "-a", "--no-data"],
        r"""pg_dumpall:\ error:\ options\ \-a/\-\-data\-only\ and\ \-\-no\-data\ cannot\ be\ used\ together""",
        "pg_dumpall: options -a\\/--data-only and --no-data cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "-s", "--no-schema"],
        r"""pg_dumpall:\ error:\ options\ \-s/\-\-schema\-only\ and\ \-\-no\-schema\ cannot\ be\ used\ together""",
        "pg_dumpall: options -s\\/--schema-only and --no-schema cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "--statistics-only", "--no-statistics"],
        r"""pg_dumpall:\ error:\ options\ \-\-statistics\-only\ and\ \-\-no\-statistics\ cannot\ be\ used\ together""",
        "pg_dumpall: options --statistics-only and --no-statistics cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "--statistics", "--no-statistics"],
        r"""pg_dumpall:\ error:\ options\ \-\-statistics\ and\ \-\-no\-statistics\ cannot\ be\ used\ together""",
        "pg_dumpall: options --statistics-only and --no-statistics cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "--statistics", "--tablespaces-only"],
        r"""pg_dumpall:\ error:\ options\ \-\-statistics\ and\ \-t/\-\-tablespaces\-only\ cannot\ be\ used\ together""",
        "pg_dumpall: options --statistics and -t\\/--tablespaces-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "--format", "x"],
        r"""pg_dumpall:\ error:\ unrecognized\ output\ format\ "x";""",
        "pg_dumpall: unrecognized output format",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "--format", "d", "--restrict-key=uu", "-f dumpfile"],
        r"""pg_dumpall:\ error:\ option\ \-\-restrict\-key\ can\ only\ be\ used\ with\ \-\-format=plain""",
        "pg_dumpall: --restrict-key can only be used with plain dump format",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "--format", "d", "--globals-only", "--clean", "-f", "dumpfile"],
        r"""pg_dumpall:\ error:\ options\ \-\-clean\ and\ \-g/\-\-globals\-only\ cannot\ be\ used\ together\ in\ non\-text\ dump""",
        "pg_dumpall: --clean and -g/--globals-only cannot be used together in non-text dump",
    )
    pg_bin.command_fails_like(
        ["pg_dumpall", "--format", "d"],
        r"""pg_dumpall:\ error:\ option\ \-F/\-\-format=d\|c\|t\ requires\ option\ \-f/\-\-file""",
        "pg_dumpall: non-plain format requires --file option",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "--exclude-database=foo", "--globals-only", "-d", "xxx"],
        r"""pg_restore:\ error:\ options\ \-\-exclude\-database\ and\ \-g/\-\-globals\-only\ cannot\ be\ used\ together""",
        "pg_restore: options --exclude-database and -g/--globals-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "--data-only", "--globals-only", "-d", "xxx"],
        r"""pg_restore:\ error:\ options\ \-a/\-\-data\-only\ and\ \-g/\-\-globals\-only\ cannot\ be\ used\ together""",
        "pg_restore: error: options -a/--data-only and -g/--globals-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "--schema-only", "--globals-only", "-d", "xxx"],
        r"""pg_restore:\ error:\ options\ \-g/\-\-globals\-only\ and\ \-s/\-\-schema\-only\ cannot\ be\ used\ together""",
        "pg_restore: error: options -g/--globals-only and -s/--schema-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "--statistics-only", "--globals-only", "-d", "xxx"],
        r"""pg_restore:\ error:\ options\ \-g/\-\-globals\-only\ and\ \-\-statistics\-only\ cannot\ be\ used\ together""",
        "pg_restore: error: options -g/--globals-only and --statistics-only cannot be used together",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "--exclude-database=foo", "-d", "xxx", "dumpdir"],
        r"""pg_restore:\ error:\ option\ \-\-exclude\-database\ can\ be\ used\ only\ when\ restoring\ an\ archive\ created\ by\ pg_dumpall""",
        "When option --exclude-database is used in pg_restore with dump of pg_dump",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "--globals-only", "-d", "xxx", "dumpdir"],
        r"""pg_restore:\ error:\ option\ \-g/\-\-globals\-only\ can\ be\ used\ only\ when\ restoring\ an\ archive\ created\ by\ pg_dumpall""",
        "When option --globals-only is used in pg_restore with the dump of pg_dump",
    )
    pg_bin.command_fails_like(
        ["pg_restore", "--globals-only", "--no-globals", "-d", "xxx", "dumpdir"],
        r"""pg_restore:\ error:\ options\ \-g/\-\-globals\-only\ and\ \-\-no\-globals\ cannot\ be\ used\ together""",
        "options --no-globals and --globals-only cannot be used together",
    )
