# Copyright (c) 2026, PostgreSQL Global Development Group
#
# QUEL: Postquel revival as a parser extension.

quel_sources = files('quel.c', 'quel_grammar.c', 'quel_rangetab.c')

if host_system == 'windows'
  quel_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'quel',
    '--FILEDESC', 'quel - QUEL query language as a parser extension',])
endif

quel = shared_module('quel',
  quel_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += quel

install_data(
  'quel.control',
  'quel--1.0.sql',
  install_dir: contrib_data_args['install_dir'],
)

tests += {
  'name': 'quel',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_quel.pl',
    ],
  },
}
