# Copyright (c) 2022-2026, PostgreSQL Global Development Group

cube_sources = files(
  'cube.c',
  'cubeparse_driver.c',
)

# Lime-generated lexer (replaces cubescan.l) and parser (replaces
# cubeparse.y).  Both are warning-clean as of Lime v1.5.x, so -- like
# upstream's flex/bison output -- they are compiled directly into the
# module rather than isolated.
cube_scan = custom_target('cubescan_lex',
  input: 'cubescan.lex',
  output: ['cubescan_lex.c', 'cubescan_lex.h'],
  command: lime_lex_cmd,
)
generated_sources += cube_scan

cube_parse = custom_target('cubeparse',
  input: 'cubeparse.lime',
  kwargs: lime_kw,
)
generated_sources += cube_parse.to_list()

# The generated lexer and parser are warning-clean as of Lime v1.5.x, so
# both are compiled directly into the module.
cube_sources += [cube_scan, cube_parse]

if host_system == 'windows'
  cube_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'cube',
    '--FILEDESC', 'cube - multidimensional cube data type',])
endif

cube = shared_module('cube',
  cube_sources,
  include_directories: include_directories('.'),
  kwargs: contrib_mod_args,
)
contrib_targets += cube

install_data(
  'cube.control',
  'cube--1.0--1.1.sql',
  'cube--1.1--1.2.sql',
  'cube--1.2.sql',
  'cube--1.2--1.3.sql',
  'cube--1.3--1.4.sql',
  'cube--1.4--1.5.sql',
  kwargs: contrib_data_args,
)

install_headers(
  'cubedata.h',
  install_dir: dir_include_extension / 'cube',
)

tests += {
  'name': 'cube',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'cube',
      'cube_sci',
    ],
  },
}
