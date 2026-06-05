# Copyright (c) 2022-2026, PostgreSQL Global Development Group

backend_sources += files(
  'slot.c',
  'slotfuncs.c',
  'syncrep.c',
  'walreceiver.c',
  'walreceiverfuncs.c',
  'walsender.c',
)

# see ../parser/meson.build
repl_parser_sources = []

repl_parser_sources += files('repl_scanner.c')
repl_parser_sources += files('syncrep_scanner.c')

# Lime-generated grammars and scanners.  All are warning-clean as of
# Lime v1.5.x, so -- like upstream's bison/flex output -- they are
# compiled directly into repl_parser below rather than isolated.

repl_gram = custom_target('repl_gram',
  input: 'repl_gram.lime',
  kwargs: lime_kw,
)
generated_sources += repl_gram.to_list()

syncrep_gram = custom_target('syncrep_gram',
  input: 'syncrep_gram.lime',
  kwargs: lime_kw,
)
generated_sources += syncrep_gram.to_list()

# syncrep_scanner.c / repl_scanner.c are now parser-driver shims around
# the generated *LexFeedBytes entry points.
syncrep_scanner = custom_target('syncrep_scanner_lex',
  input: 'syncrep_scanner.lex',
  output: ['syncrep_scanner_lex.c', 'syncrep_scanner_lex.h'],
  command: lime_lex_cmd,
)
generated_sources += syncrep_scanner

repl_scanner = custom_target('repl_scanner_lex',
  input: 'repl_scanner.lex',
  output: ['repl_scanner_lex.c', 'repl_scanner_lex.h'],
  command: lime_lex_cmd,
)
generated_sources += repl_scanner

repl_parser = static_library('repl_parser',
  repl_parser_sources,
  repl_gram,
  syncrep_gram,
  repl_scanner,
  syncrep_scanner,
  dependencies: [backend_code],
  include_directories: include_directories('.'),
  kwargs: internal_lib_args,
)
backend_link_with += repl_parser

subdir('logical')
