# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_fts_sources = files(
  'pg_fts_analyze.c',
  'pg_fts_tsanalyze.c',
  'pg_fts_doc.c',
  'pg_fts_query.c',
  'pg_fts_rank.c',
  'pg_fts_am.c',
  'pg_fts_aux.c',
  'pg_fts_migrate.c',
  'pg_fts_trgm.c',
  'pg_fts_match.c',
)

# Vendored sparsemap: compiled separately with relaxed warnings (it uses C99
# mixed declarations) and its public symbols namespaced to __pg_bm25_* (see
# pg_fts_sm.h / vendor/sm.c) so a second copy in another extension cannot
# collide at load time.
pg_fts_sparsemap = static_library('pg_fts_sparsemap',
  files('vendor/sm.c'),
  c_args: cflags_mod + ['-Wno-declaration-after-statement'],
  include_directories: postgres_inc,
  kwargs: default_lib_args,
)

if host_system == 'windows'
  pg_fts_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_fts',
    '--FILEDESC', 'pg_fts - next-generation full-text search',])
endif

pg_fts = shared_module('pg_fts',
  pg_fts_sources,
  link_with: pg_fts_sparsemap,
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
  'pg_fts--1.4--1.5.sql',
  'pg_fts--1.5--1.6.sql',
  'pg_fts--1.6--1.7.sql',
  'pg_fts--1.7--1.8.sql',
  'pg_fts--1.8--1.9.sql',
  'pg_fts--1.9--1.10.sql',
  'pg_fts--1.10--1.11.sql',
  'pg_fts--1.11--1.12.sql',
  'pg_fts--1.12--1.13.sql',
  'pg_fts--1.13--1.14.sql',
  'pg_fts--1.14--1.15.sql',
  'pg_fts--1.15--1.16.sql',
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
