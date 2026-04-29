# Copyright (c) 2024-2026, PostgreSQL Global Development Group

test_sparsemap_sources = files(
  'test_sparsemap.c',
)

if host_system == 'windows'
  test_sparsemap_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_sparsemap',
    '--FILEDESC', 'test_sparsemap - test code for src/include/lib/sparsemap.h',])
endif

test_sparsemap = shared_module('test_sparsemap',
  test_sparsemap_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_sparsemap

test_install_data += files(
  'test_sparsemap.control',
  'test_sparsemap--1.0.sql',
)

tests += {
  'name': 'test_sparsemap',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_sparsemap',
    ],
  },
}
