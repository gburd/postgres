# Copyright (c) 2026, PostgreSQL Global Development Group

test_recno_posting_sources = files(
  'test_recno_posting.c',
)

if host_system == 'windows'
  test_recno_posting_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_recno_posting',
    '--FILEDESC', 'test_recno_posting - test code for the RECNO posting codec',])
endif

test_recno_posting = shared_module('test_recno_posting',
  test_recno_posting_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_recno_posting

test_install_data += files(
  'test_recno_posting.control',
  'test_recno_posting--1.0.sql',
)

tests += {
  'name': 'test_recno_posting',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_recno_posting',
    ],
  },
}
