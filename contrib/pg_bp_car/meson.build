# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_bp_car_sources = files(
  'pg_bp_car.c',
)

if host_system == 'windows'
  pg_bp_car_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_bp_car',
    '--FILEDESC', 'pg_bp_car - CAR buffer pool replacement strategy',])
endif

pg_bp_car = shared_module('pg_bp_car',
  pg_bp_car_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_bp_car

install_data(
  'pg_bp_car--1.0.sql',
  'pg_bp_car.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_bp_car',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_bp_car',
    ],
  },
}
