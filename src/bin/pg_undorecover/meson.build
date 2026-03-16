# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_undorecover_sources = files(
  'pg_undorecover.c',
)

if host_system == 'windows'
  pg_undorecover_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_undorecover',
    '--FILEDESC', 'pg_undorecover - recover data from UNDO logs',])
endif

pg_undorecover = executable('pg_undorecover',
  pg_undorecover_sources,
  dependencies: [frontend_code],
  kwargs: default_bin_args,
)
bin_targets += pg_undorecover
