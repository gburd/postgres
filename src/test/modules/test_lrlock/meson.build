# Copyright (c) 2026, PostgreSQL Global Development Group

test_lrlock_sources = files(
  'test_lrlock.c',
)

if host_system == 'windows'
  test_lrlock_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_lrlock',
    '--FILEDESC', 'test_lrlock - test code for the left-right lock primitive',])
endif

test_lrlock = shared_module('test_lrlock',
  test_lrlock_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_lrlock

test_install_data += files(
  'test_lrlock.control',
  'test_lrlock--1.0.sql',
)

tests += {
  'name': 'test_lrlock',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_lrlock',
    ],
    'regress_args': ['--temp-config', files('test_lrlock.conf')],
    'runningcheck': false,
  },
}
