# Copyright (c) 2026, PostgreSQL Global Development Group

bark_multikey_sources = files(
  'bark_multikey.c',
)

if host_system == 'windows'
  bark_multikey_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'bark_multikey',
    '--FILEDESC', 'bark_multikey - test code for BARK multikey operator classes',])
endif

bark_multikey = shared_module('bark_multikey',
  bark_multikey_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += bark_multikey

test_install_data += files(
  'bark_multikey.control',
  'bark_multikey--1.0.sql',
)

tests += {
  'name': 'bark_multikey',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'bark_multikey',
    ],
  },
}
