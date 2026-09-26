# Copyright (c) 2026, PostgreSQL Global Development Group

test_sbm_sources = files(
  'test_sbm.c',
)

if host_system == 'windows'
  test_sbm_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_sbm',
    '--FILEDESC', 'test_sbm - test code for src/backend/lib/sbm.c',])
endif

test_sbm = shared_module('test_sbm',
  test_sbm_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_sbm

test_install_data += files(
  'test_sbm.control',
  'test_sbm--1.0.sql',
)

tests += {
  'name': 'test_sbm',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_sbm',
    ],
  },
}
