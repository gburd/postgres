# Copyright (c) 2022-2026, PostgreSQL Global Development Group

seg_sources = files(
  'seg.c',
  'segparse_driver.c',
)

# Lime-generated lexer (replaces segscan.l) and parser (replaces
# segparse.y).  Both are warning-clean as of Lime v1.5.x, so -- like
# upstream's flex/bison output -- they are compiled directly into the
# module rather than isolated.
seg_scan = custom_target('segscan_lex',
  input: 'segscan.lex',
  output: ['segscan_lex.c', 'segscan_lex.h'],
  command: lime_lex_cmd,
)
generated_sources += seg_scan.to_list()

seg_parse = custom_target('segparse',
  input: 'segparse.lime',
  kwargs: lime_kw,
)
generated_sources += seg_parse.to_list()

seg_sources += [seg_scan, seg_parse]

if host_system == 'windows'
  seg_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'seg',
    '--FILEDESC', 'seg - line segment data type',])
endif

seg = shared_module('seg',
  seg_sources,
  include_directories: include_directories('.'),
  kwargs: contrib_mod_args,
)
contrib_targets += seg

install_data(
  'seg.control',
  'seg--1.0--1.1.sql',
  'seg--1.1--1.2.sql',
  'seg--1.1.sql',
  'seg--1.2--1.3.sql',
  'seg--1.3--1.4.sql',
  kwargs: contrib_data_args,
)

install_headers(
  'segdata.h',
  install_dir: dir_include_extension / 'seg',
)

tests += {
  'name': 'seg',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'security',
      'seg',
      'partition',
    ],
  },
}
