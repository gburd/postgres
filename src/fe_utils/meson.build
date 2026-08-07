# Copyright (c) 2022-2026, PostgreSQL Global Development Group

fe_utils_sources = files(
  'archive.c',
  'astreamer_file.c',
  'astreamer_gzip.c',
  'astreamer_lz4.c',
  'astreamer_tar.c',
  'astreamer_zstd.c',
  'cancel.c',
  'conditional.c',
  'connect_utils.c',
  'mbprint.c',
  'option_utils.c',
  'parallel_slot.c',
  'print.c',
  'query_utils.c',
  'recovery_gen.c',
  'simple_list.c',
  'string_utils.c',
  'version.c',
)

# Lime-generated SQL-side lexer (replaces the hand-rolled state machine
# in psqlscan.c, pre-Phase 5).  psqlscan.c becomes a parser-driver shim
# around PsqlLexFeedBytes plus the buffer-stack management helpers
# shared with psqlscanslash.c and pgbench's exprscan.c.
psqlscan_lex = custom_target('psqlscan_lex',
  input: 'psqlscan.lex',
  output: ['psqlscan_lex.c', 'psqlscan_lex.h'],
  command: lime_lex_cmd,
)
generated_sources += psqlscan_lex.to_list()

# Wrap the generated .c plus psqlscan.c into one static_library so
# include_directories('.') sees the build dir for this subdir (where
# psqlscan_lex.h is emitted).  Lime v1.5.1 emits warning-clean scanner
# code, so no warning relaxation is needed here.
psqlscan_lib = static_library('psqlscan',
  ['psqlscan.c'],
  psqlscan_lex,
  c_pch: pch_postgres_fe_h,
  include_directories: [postgres_inc, libpq_inc, include_directories('.')],
  dependencies: frontend_common_code,
  kwargs: internal_lib_args,
)

fe_utils = static_library('libpgfeutils',
  fe_utils_sources,
  c_pch: pch_postgres_fe_h,
  include_directories: [postgres_inc, libpq_inc],
  c_args: host_system == 'windows' ? ['-DFD_SETSIZE=1024'] : [],
  dependencies: frontend_common_code,
  link_whole: [psqlscan_lib],
  kwargs: default_lib_args + {
            'install': install_internal_static_lib,
          },
)
