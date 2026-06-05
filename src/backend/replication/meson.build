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

# Lime-generated parser code is emitted without static qualifiers on the
# driver-internal helpers (the xxAlloc/xxFree/xxInit functions) and with
# mixed declarations/code in its template.  Compile each generated file
# in its own library so we can relax just those two warnings; the hand-
# rolled scanners and all other backend code keep the project defaults.
#
# (One `static_library` per generated .c is noisier than a single shared
# one but keeps the c_args override local to the output that needs it.)

repl_gram = custom_target('repl_gram',
  input: 'repl_gram.lime',
  kwargs: lime_kw,
)
generated_sources += repl_gram.to_list()

repl_gram_lib = static_library('repl_gram',
  repl_gram,
  dependencies: [backend_code],
  include_directories: include_directories('.'),
  c_args: ['-Wno-missing-prototypes'] + cflags_no_decl_after_statement,
  kwargs: internal_lib_args,
)
backend_link_with += repl_gram_lib

syncrep_gram = custom_target('syncrep_gram',
  input: 'syncrep_gram.lime',
  kwargs: lime_kw,
)
generated_sources += syncrep_gram.to_list()

syncrep_gram_lib = static_library('syncrep_gram',
  syncrep_gram,
  dependencies: [backend_code],
  include_directories: include_directories('.'),
  c_args: ['-Wno-missing-prototypes'] + cflags_no_decl_after_statement,
  kwargs: internal_lib_args,
)
backend_link_with += syncrep_gram_lib

# Lime-generated lexer (replaces syncrep_scanner.c's hand-rolled
# state machine).  syncrep_scanner.c is now a thin parser-driver
# shim around SyncRepLexFeedBytes.
syncrep_scanner = custom_target('syncrep_scanner_lex',
  input: 'syncrep_scanner.lex',
  output: ['syncrep_scanner_lex.c', 'syncrep_scanner_lex.h'],
  command: lime_lex_cmd,
)
generated_sources += syncrep_scanner

syncrep_scanner_lib = static_library('syncrep_scanner_lex',
  syncrep_scanner,
  dependencies: [backend_code],
  include_directories: include_directories('.'),
  c_args: ['-Wno-missing-prototypes', '-Wno-unused-variable'] +
          cflags_no_decl_after_statement,
  kwargs: internal_lib_args,
)
backend_link_with += syncrep_scanner_lib

# Lime-generated lexer for repl_scanner (replaces hand-rolled
# tokenizer).  repl_scanner.c is now a parser-driver shim.
repl_scanner = custom_target('repl_scanner_lex',
  input: 'repl_scanner.lex',
  output: ['repl_scanner_lex.c', 'repl_scanner_lex.h'],
  command: lime_lex_cmd,
)
generated_sources += repl_scanner

repl_scanner_lib = static_library('repl_scanner_lex',
  repl_scanner,
  dependencies: [backend_code],
  include_directories: include_directories('.'),
  c_args: ['-Wno-missing-prototypes', '-Wno-unused-variable'] +
          cflags_no_decl_after_statement,
  kwargs: internal_lib_args,
)
backend_link_with += repl_scanner_lib

repl_parser = static_library('repl_parser',
  repl_parser_sources,
  dependencies: [backend_code],
  include_directories: include_directories('.'),
  kwargs: internal_lib_args,
)
backend_link_with += repl_parser

subdir('logical')
