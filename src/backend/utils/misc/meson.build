# Copyright (c) 2022-2026, PostgreSQL Global Development Group

backend_sources += files(
  'conffiles.c',
  'guc.c',
  'guc_funcs.c',
  'guc_tables.c',
  'help_config.c',
  'injection_point.c',
  'pg_config.c',
  'pg_controldata.c',
  'pg_rusage.c',
  'ps_status.c',
  'queryenvironment.c',
  'rls.c',
  'sampling.c',
  'stack_depth.c',
  'superuser.c',
  'timeout.c',
  'tzparser.c',
)

# Lime-generated lexer for the postgresql.conf grammar (replaces the
# hand-rolled tokenizer that used to live in guc-file.c).  guc-file.c
# now consumes Lime-emitted tokens via a parser-driver shim; both
# files compile in their own static_library so the per-subdirectory
# include path resolves the generated header correctly (.h lives in
# the build dir, not the source dir).
guc_file_lex = custom_target('guc_file_lex',
  input: 'guc_file.lex',
  output: ['guc_file_lex.c', 'guc_file_lex.h'],
  command: lime_lex_cmd,
)
generated_sources += guc_file_lex.to_list()

guc_file_lib = static_library('guc_file',
  files('guc-file.c'),
  guc_file_lex,
  dependencies: [backend_code],
  include_directories: include_directories('.'),
  kwargs: internal_lib_args,
)
backend_link_with += guc_file_lib

install_data('postgresql.conf.sample',
  install_dir: dir_data,
)
