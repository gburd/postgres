# Copyright (c) 2026, PostgreSQL Global Development Group

test_seqlock_sources = files(
  'test_seqlock.c',
)

if host_system == 'windows'
  test_seqlock_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_seqlock',
    '--FILEDESC', 'test_seqlock - test code for src/include/storage/seqlock.h',])
endif

test_seqlock = shared_module('test_seqlock',
  test_seqlock_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_seqlock

test_install_data += files(
  'test_seqlock.control',
  'test_seqlock--1.0.sql',
)

tests += {
  'name': 'test_seqlock',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_seqlock',
    ],
  },
}
