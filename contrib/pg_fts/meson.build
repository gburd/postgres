# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_fts_sources = files(
  'pg_fts_analyze.c',
  'pg_fts_tsanalyze.c',
  'pg_fts_doc.c',
  'pg_fts_query.c',
  'pg_fts_rank.c',
  'pg_fts_am.c',
  'pg_fts_match.c',
)

if host_system == 'windows'
  pg_fts_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_fts',
    '--FILEDESC', 'pg_fts - next-generation full-text search',])
endif

pg_fts = shared_module('pg_fts',
  pg_fts_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_fts

install_data(
  'pg_fts.control',
  'pg_fts--1.0.sql',
  'pg_fts--1.0--1.1.sql',
  'pg_fts--1.1--1.2.sql',
  'pg_fts--1.2--1.3.sql',
  'pg_fts--1.3--1.4.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_fts',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_fts',
    ],
  },
}
