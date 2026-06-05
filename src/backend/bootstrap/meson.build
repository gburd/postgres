# Copyright (c) 2022-2026, PostgreSQL Global Development Group

backend_sources += files(
  'bootstrap.c')

# see ../parser/meson.build
boot_parser_sources = []

boot_parser_sources += files('bootscanner.c')

# Lime-generated lexer.  Replaces the former hand-rolled tokenizer
# in bootscanner.c.  Lime v0.2.1's `-X` mode emits
# bootscanner_lex.c + bootscanner_lex.h next to each other; the
# driver in bootscanner.c includes the .h for the BootLex* API.
bootscanner_lex = custom_target('bootscanner_lex',
  input: 'bootscanner.lex',
  output: ['bootscanner_lex.c', 'bootscanner_lex.h'],
  command: lime_lex_cmd,
)
boot_parser_sources += bootscanner_lex

# Lime-generated parser code is emitted without static qualifiers on the
# driver-internal helpers and with mixed declarations/code in its template.
# Compile the generated file in its own library so we can relax just those
# two warnings; the hand-rolled scanner and all other backend code keep the
# project defaults.  (Mirrors the pattern in ../replication/meson.build.)

bootparse = custom_target('bootparse',
  input: 'bootparse.lime',
  kwargs: lime_kw,
)
generated_sources += bootparse.to_list()

boot_gram_lib = static_library('boot_gram',
  bootparse,
  dependencies: [backend_code],
  include_directories: include_directories('.'),
  c_args: ['-Wno-missing-prototypes', '-Wno-unused-variable'] + cflags_no_decl_after_statement,
  kwargs: internal_lib_args,
)
backend_link_with += boot_gram_lib

boot_parser = static_library('boot_parser',
  boot_parser_sources,
  dependencies: [backend_code],
  include_directories: include_directories('.'),
  c_args: ['-Wno-missing-prototypes', '-Wno-unused-variable'] + cflags_no_decl_after_statement,
  kwargs: internal_lib_args,
)
backend_link_with += boot_parser
