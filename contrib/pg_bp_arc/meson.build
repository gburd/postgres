# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_bp_arc_sources = files(
  'pg_bp_arc.c',
)

if host_system == 'windows'
  pg_bp_arc_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_bp_arc',
    '--FILEDESC', 'pg_bp_arc - ARC buffer pool replacement strategy',])
endif

pg_bp_arc = shared_module('pg_bp_arc',
  pg_bp_arc_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_bp_arc

install_data(
  'pg_bp_arc--1.0.sql',
  'pg_bp_arc.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_bp_arc',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_bp_arc',
    ],
  },
}
