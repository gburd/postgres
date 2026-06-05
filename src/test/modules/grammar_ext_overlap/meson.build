# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Multi-extension overlap torture test.  Five extensions simulate
# the user's flagship use case: DuckDB + MySQL + MongoDB + pg_infer
# + QUEL all loaded in the same backend, contributing different
# keywords and rules to the same `stmt` non-terminal.

ovl_kw = pg_test_mod_args

foreach ext_name : [
  'duckdb_compat',
  'mysql_compat',
  'mongo_jsonb',
  'pg_infer',
  'quel_lite',
]
  src = 'ext_' + ext_name + '.c'
  if host_system == 'windows'
    rc = rc_lib_gen.process(win32ver_rc, extra_args: [
      '--NAME', 'grammar_ext_overlap_' + ext_name,
      '--FILEDESC', 'grammar_ext_overlap ' + ext_name + ' - multi-ext torture',])
    so = shared_module('grammar_ext_overlap_' + ext_name,
      [src, rc], kwargs: ovl_kw)
  else
    so = shared_module('grammar_ext_overlap_' + ext_name,
      [src], kwargs: ovl_kw)
  endif
  test_install_libs += so
endforeach

tests += {
  'name': 'grammar_ext_overlap',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_overlap.pl',
    ],
  },
}
