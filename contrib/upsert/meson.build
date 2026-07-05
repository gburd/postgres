# Copyright (c) 2026, PostgreSQL Global Development Group
#
# upsert: UPSERT statement as a parser extension, lowered to
# INSERT ... ON CONFLICT (...) DO UPDATE.

upsert_sources = files('upsert.c')

if host_system == 'windows'
  upsert_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'upsert',
    '--FILEDESC', 'upsert - UPSERT statement as a parser extension',])
endif

upsert = shared_module('upsert',
  upsert_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += upsert

install_data(
  'upsert.control',
  'upsert--1.0.sql',
  install_dir: contrib_data_args['install_dir'],
)

tests += {
  'name': 'upsert',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_upsert.pl',
    ],
  },
}
