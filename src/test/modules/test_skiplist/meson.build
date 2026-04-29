# Copyright (c) 2024-2026, PostgreSQL Global Development Group

test_skiplist_sources = files(
  'test_skiplist.c',
)

if host_system == 'windows'
  test_skiplist_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_skiplist',
    '--FILEDESC', 'test_skiplist - test code for src/include/lib/skiplist.h',])
endif

test_skiplist = shared_module('test_skiplist',
  test_skiplist_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_skiplist

test_install_data += files(
  'test_skiplist.control',
  'test_skiplist--1.0.sql',
)

tests += {
  'name': 'test_skiplist',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_skiplist',
    ],
  },
}
