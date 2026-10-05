# Copyright (c) 2022-2026, PostgreSQL Global Development Group

test_idxundo_sources = files(
  'test_idxundo.c',
)

if host_system == 'windows'
  test_idxundo_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_idxundo',
    '--FILEDESC', 'test_idxundo - exercise index UNDO',])
endif

test_idxundo = shared_module('test_idxundo',
  test_idxundo_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_idxundo

test_install_data += files(
  'test_idxundo.control',
  'test_idxundo--1.0.sql',
)

tests += {
  'name': 'test_idxundo',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_idxundo',
    ],
    # The end-to-end tests read the index back and verify it with amcheck.
    'regress_args': ['--load-extension=amcheck'],
  },
  'tap': {
    'tests': [
      't/001_idxundo_crash.pl',
    ],
  },
}
