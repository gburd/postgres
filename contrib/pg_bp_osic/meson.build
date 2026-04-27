# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_bp_osic_sources = files(
  'pg_bp_osic.c',
)

if host_system == 'windows'
  pg_bp_osic_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_bp_osic',
    '--FILEDESC', 'pg_bp_osic - OSIC buffer pool replacement strategy',])
endif

pg_bp_osic = shared_module('pg_bp_osic',
  pg_bp_osic_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_bp_osic

install_data(
  'pg_bp_osic--1.0.sql',
  'pg_bp_osic.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_bp_osic',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_bp_osic',
    ],
  },
}
