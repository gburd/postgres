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
boot_parser_sources += bootscanner_lex.to_list()

# Lime-generated grammar and scanner.  Both are warning-clean as of Lime
# v1.5.x, so -- like upstream's bison/flex output -- they are compiled
# directly into boot_parser below rather than isolated.

bootparse = custom_target('bootparse',
  input: 'bootparse.lime',
  kwargs: lime_kw,
)
generated_sources += bootparse.to_list()

boot_parser = static_library('boot_parser',
  boot_parser_sources,
  bootparse,
  dependencies: [backend_code],
  include_directories: include_directories('.'),
  kwargs: internal_lib_args,
)
backend_link_with += boot_parser
