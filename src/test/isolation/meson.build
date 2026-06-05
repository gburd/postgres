# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pg_regress_c helpfully provided by regress/meson.build

isolation_sources = pg_regress_c + files(
  'isolation_main.c',
)

isolationtester_sources = files(
  'isolationtester.c',
  'specscanner.c',
)

# Generated parser.  Compiled into its own static_library so the two
# warnings that the Lime template trips (missing-prototypes and
# declaration-after-statement) can be relaxed for the generated .c
# only -- every other translation unit keeps the project defaults.
# See ../../backend/replication/meson.build for the template.
spec_gram = custom_target('specparse',
  input: 'specparse.lime',
  kwargs: lime_kw,
)
generated_sources += spec_gram.to_list()

spec_gram_lib = static_library('spec_gram',
  spec_gram,
  dependencies: [frontend_code],
  include_directories: include_directories('.'),
  c_args: ['-Wno-missing-prototypes'] + cflags_no_decl_after_statement,
  kwargs: internal_lib_args,
)

# Lime-generated lexer (replaces specscanner.c's hand-rolled tokenizer).
spec_scanner_lex = custom_target('specscanner_lex',
  input: 'specscanner.lex',
  output: ['specscanner_lex.c', 'specscanner_lex.h'],
  command: lime_lex_cmd,
)
generated_sources += spec_scanner_lex

spec_scanner_lex_lib = static_library('spec_scanner_lex',
  spec_scanner_lex,
  dependencies: [frontend_code],
  include_directories: include_directories('.'),
  c_args: ['-Wno-missing-prototypes', '-Wno-unused-variable'] +
          cflags_no_decl_after_statement,
  kwargs: internal_lib_args,
)

if host_system == 'windows'
  isolation_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_isolation_regress',
    '--FILEDESC', 'pg_isolation_regress - multi-client test driver',])
endif

pg_isolation_regress = executable('pg_isolation_regress',
  isolation_sources,
  c_args: pg_regress_cflags,
  include_directories: pg_regress_inc,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args + {
    'install_dir': dir_pgxs / 'src/test/isolation',
  },
)
bin_targets += pg_isolation_regress


if host_system == 'windows'
  isolationtester_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'isolationtester',
    '--FILEDESC', 'isolationtester - multi-client test driver',])
endif

isolationtester = executable('isolationtester',
  isolationtester_sources,
  include_directories: include_directories('.'),
  link_with: [spec_gram_lib, spec_scanner_lex_lib],
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args + {
    'install_dir': dir_pgxs / 'src/test/isolation',
  },
)
bin_targets += isolationtester

tests += {
  'name': 'isolation',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'isolation': {
    'schedule': files('isolation_schedule'),
    'test_kwargs': {
      'priority': 40,
      'timeout': 1000,
    },
    'dbname': 'isolation_regression',
  },
}
