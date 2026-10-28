# Copyright (c) 2026, PostgreSQL Global Development Group

bark_wildcard_sources = files(
  'bark_wildcard.c',
)

if host_system == 'windows'
  bark_wildcard_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'bark_wildcard',
    '--FILEDESC', 'bark_wildcard - a wildcard operator class for BARK over jsonb',])
endif

bark_wildcard = shared_module('bark_wildcard',
  bark_wildcard_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += bark_wildcard

test_install_data += files(
  'bark_wildcard.control',
  'bark_wildcard--1.0.sql',
)

tests += {
  'name': 'bark_wildcard',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'bark_wildcard',
      'bark_wildcard_scan',
    ],
    # Paths and strings sort by the database's collation.
    'regress_args': ['--no-locale'],
  },
}
