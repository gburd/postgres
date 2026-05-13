# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_bp_lirs_sources = files(
  'pg_bp_lirs.c',
)

if host_system == 'windows'
  pg_bp_lirs_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_bp_lirs',
    '--FILEDESC', 'pg_bp_lirs - LIRS-2 buffer pool replacement strategy',])
endif

pg_bp_lirs = shared_module('pg_bp_lirs',
  pg_bp_lirs_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_bp_lirs

install_data(
  'pg_bp_lirs--1.0.sql',
  'pg_bp_lirs.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_bp_lirs',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_bp_lirs',
    ],
  },
}
