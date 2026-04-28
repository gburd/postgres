# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_bp_rcrd_sources = files(
  'pg_bp_rcrd.c',
)

if host_system == 'windows'
  pg_bp_rcrd_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_bp_rcrd',
    '--FILEDESC', 'pg_bp_rcrd - RCRD (LIRS2) buffer pool replacement strategy',])
endif

pg_bp_rcrd = shared_module('pg_bp_rcrd',
  pg_bp_rcrd_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_bp_rcrd

install_data(
  'pg_bp_rcrd--1.0.sql',
  'pg_bp_rcrd.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_bp_rcrd',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_bp_rcrd',
    ],
  },
}
