# Copyright (c) 2026, PostgreSQL Global Development Group

parser_microbench_sources = files('parser_microbench.c')

if host_system == 'windows'
  parser_microbench_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'parser_microbench',
    '--FILEDESC', 'parser_microbench - direct raw_parser() timing',])
endif

parser_microbench = shared_module('parser_microbench',
  parser_microbench_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += parser_microbench
