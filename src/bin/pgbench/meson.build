# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pgbench_sources = files(
  'pgbench.c',
)

exprparse = custom_target('exprparse',
  input: 'exprparse.lime',
  kwargs: lime_kw,
)
generated_sources += exprparse.to_list()

# Lime template emits some unused-variable warnings; isolate the
# generated .c in its own static_library so the relaxation doesn't
# leak into pgbench.c.
exprparse_lib = static_library('exprparse',
  exprparse,
  include_directories: include_directories('.'),
  c_args: ['-Wno-missing-prototypes', '-Wno-unused-variable']
          + cflags_no_decl_after_statement,
  dependencies: [frontend_code, libpq],
  kwargs: internal_lib_args,
)

# Lime-generated lexer (replaces the EXPR-state half of the
# hand-rolled tokenizer in exprscan.c).  exprscan.c remains as the
# parser-driver shim plus the INITIAL-state word splitter and the
# PgBenchExpr constructors.
exprscan_lex = custom_target('exprscan_lex',
  input: 'exprscan.lex',
  output: ['exprscan_lex.c', 'exprscan_lex.h'],
  command: lime_lex_cmd,
)
generated_sources += exprscan_lex.to_list()

exprscan_lex_lib = static_library('exprscan_lex',
  exprscan_lex,
  include_directories: include_directories('.'),
  c_args: ['-Wno-missing-prototypes', '-Wno-unused-variable']
          + cflags_no_decl_after_statement,
  dependencies: [frontend_code, libpq],
  kwargs: internal_lib_args,
)

# exprscan.c needs the generated exprscan_lex.h; wrap it in its own
# static_library so include_directories sees the build dir for this
# subdir.
exprscan_lib = static_library('exprscan',
  ['exprscan.c'],
  exprscan_lex,
  include_directories: include_directories('.'),
  dependencies: [frontend_code, libpq],
  kwargs: internal_lib_args,
)

if host_system == 'windows'
  pgbench_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pgbench',
    '--FILEDESC', 'pgbench - a simple program for running benchmark tests'])
endif

pgbench = executable('pgbench',
  pgbench_sources,
  dependencies: [frontend_code, libpq, thread_dep],
  link_with: [exprparse_lib, exprscan_lex_lib, exprscan_lib],
  include_directories: include_directories('.'),
  c_pch: pch_postgres_fe_h,
  c_args: host_system == 'windows' ? ['-DFD_SETSIZE=1024'] : [],
  kwargs: default_bin_args,
)
bin_targets += pgbench

tests += {
  'name': 'pgbench',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_pgbench_with_server.pl',
      't/002_pgbench_no_server.pl',
    ],
  },
}
