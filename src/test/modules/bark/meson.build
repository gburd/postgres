# Copyright (c) 2022-2026, PostgreSQL Global Development Group

if not get_option('injection_points')
  subdir_done()
endif

tests += {
  'name': 'bark',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'bark_incomplete_splits',
    ],
    'regress_args': ['--temp-config', files('../injection_points/extra.conf')],
    # The injection points are run in the same backend, so disable
    # parallelism to keep the local-attach semantics deterministic.
    'runningcheck': false,
  },
}
