# Copyright (c) 2026, PostgreSQL Global Development Group

test_index_impl_sources = files(
  'test_index_impl.c',
)

if host_system == 'windows'
  test_index_impl_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_index_impl',
    '--FILEDESC', 'test_index_impl - index access methods implementing btree indexes',])
endif

test_index_impl = shared_module('test_index_impl',
  test_index_impl_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_index_impl

test_install_data += files(
  'test_index_impl.control',
  'test_index_impl--1.0.sql',
)

tests += {
  'name': 'test_index_impl',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_index_impl',
    ],
  },
  'tap': {
    'tests': [
      't/001_dump_upgrade.pl',
    ],
  },
}
