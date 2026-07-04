# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_bp_clir_sources = files(
  'pg_bp_clir.c',
)

if host_system == 'windows'
  pg_bp_clir_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_bp_clir',
    '--FILEDESC', 'pg_bp_clir - lock-light Clock-LIRS buffer pool replacement strategy',])
endif

pg_bp_clir = shared_module('pg_bp_clir',
  pg_bp_clir_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_bp_clir

install_data(
  'pg_bp_clir--1.0.sql',
  'pg_bp_clir.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_bp_clir',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_bp_clir',
    ],
  },
}
