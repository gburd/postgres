# Copyright (c) 2022-2026, PostgreSQL Global Development Group

ecpg_sources = files(
  '../ecpglib/typename.c',
  'c_keywords.c',
  'descriptor.c',
  'ecpg.c',
  'ecpg_keywords.c',
  'keywords.c',
  'output.c',
  'parser.c',
  'pgc.c',
  'type.c',
  'util.c',
  'variable.c',
)

# Phase 5: Lime-generated lexer for pgc.c (replaces the legacy
# hand-rolled state machine).  pgc.c becomes a parser-driver shim
# around PgcLexFeedBytes.  The generated lexer .c is warning-clean as of
# Lime v1.5.1 and is compiled directly into ecpg below (after the preproc
# grammar target it depends on for preproc.h).
pgc_lex_gen = custom_target('pgc_lex',
  input: 'pgc.lex',
  output: ['pgc_lex.c', 'pgc_lex.h'],
  command: lime_lex_cmd,
)
generated_sources += pgc_lex_gen.to_list()

# The hand-rolled pgc.c driver includes the generated lexer header.
# The generated lexer .c itself is compiled into ecpg below (after the
# preproc grammar target exists, since pgc_lex.c includes preproc.h).
ecpg_sources += pgc_lex_gen[1]

ecpg_files = [
  'ecpg.addons',
  'ecpg.header',
  'ecpg.tokens',
  'ecpg.trailer',
  'ecpg.type',
]

# Phase 2k.3: ecpg's parse.pl previously read the bison-format
# gram.y directly.  With Phase 2k.2 the authoritative grammar is
# gram.lime; we run lime_to_bison_gram.py to derive a bison-format
# scaffold that parse.pl can consume unchanged.  The derived gram.y
# is a build artefact -- not committed.
derived_gram_y = custom_target('derived_gram.y',
  input: '../../../backend/parser/gram.lime',
  output: ['derived_gram.y'],
  command: [
    python, files('../../../tools/lime_to_bison_gram.py'),
    '@INPUT@', '@OUTPUT@',
  ],
)
generated_sources += derived_gram_y

preproc_y = custom_target('preproc.y',
  input: [
    derived_gram_y,
    ecpg_files,
  ],
  output: ['preproc.y'],
  command: [
    perl, files('parse.pl'),
    '--srcdir', '@CURRENT_SOURCE_DIR@',
    '--parser', '@INPUT0@',
    '--output', '@OUTPUT0@',
  ],
)
generated_sources += preproc_y

# Phase 3 final: convert preproc.y to a Lime grammar via
# lime_convert_gram.py and compile with lime, retiring bison from
# the tree entirely.  Lime v0.2.0 ships every primitive needed:
#   P0-NEW-6: YYLLOC_DEFAULT honored on every reduce
#   P0-NEW-7: pre-action ordering (action @$ writes survive)
#   P0-NEW-8: Parse_drain (ecpg's lex-time vs reduce-time output
#             ordering matches Bison's pull-mode timing)
#
# --no-driver: parser.c provides its own base_yyparse driver that
#   calls Parse_drain after each push.
# --yyerror-fn=base_yyerror: ecpg's signature differs from backend.
# --empty-loc='""': ecpg's YYLTYPE is `const char *`; empty rules
#   default to "".
preproc_lime = custom_target('preproc.lime',
  input: preproc_y,
  output: ['preproc.lime'],
  command: [
    python, files('../../../tools/lime_convert_gram.py'),
    '--no-driver',
    '--yyerror-fn', 'base_yyerror',
    '--empty-loc', '""',
    '@INPUT@', '@OUTPUT@',
  ],
)
generated_sources += preproc_lime

preproc = custom_target('preproc.c',
  input: preproc_lime,
  kwargs: lime_kw,
)
generated_sources += preproc.to_list()

# Now that the preproc grammar target exists (it emits preproc.h, which
# pgc.lex includes), compile both generated translation units into ecpg.
# Both the Lime-generated grammar (preproc.c) and lexer (pgc_lex.c) are
# warning-clean as of Lime v1.5.x, so they need no isolation -- the same
# outcome as upstream's bison/flex output.
#
# The hand-rolled ecpg sources include the generated grammar header; the
# generated lexer (pgc_lex.c) and grammar (preproc.c) are compiled here.
# (pgc_lex.c includes preproc.h, hence the dependency on the preproc
# grammar target existing first.)
ecpg_sources += [preproc, pgc_lex_gen]

c_kwlist = custom_target('c_kwlist_d.h',
  input: ['c_kwlist.h'],
  output: ['c_kwlist_d.h'],
  depend_files: gen_kwlist_deps,
  command: [gen_kwlist_cmd, '--varname', 'ScanCKeywords', '--no-case-fold'],
)
generated_sources += c_kwlist
ecpg_sources += c_kwlist

ecpg_kwlist = custom_target('ecpg_kwlist_d.h',
  input: ['ecpg_kwlist.h'],
  output: ['ecpg_kwlist_d.h'],
  depend_files: gen_kwlist_deps,
  command: [gen_kwlist_cmd, '--varname', 'ScanECPGKeywords'],
)
generated_sources += ecpg_kwlist
ecpg_sources += ecpg_kwlist

if host_system == 'windows'
  ecpg_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'ecpg',
    '--FILEDESC', 'ecpg - embedded SQL precompiler for C',])
endif

ecpg_exe = executable('ecpg',
  ecpg_sources,
  include_directories: ['.', ecpg_inc, postgres_inc, libpq_inc],
  c_pch: pch_postgres_fe_h,
  dependencies: [frontend_code],
  kwargs: default_bin_args,
)
ecpg_targets += ecpg_exe

subdir('po', if_found: libintl)

tests += {
  'name': 'ecpg',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_ecpg_err_warn_msg.pl',
      't/002_ecpg_err_warn_msg_informix.pl',
    ],
    'deps': [ecpg_exe],
  },
}
