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
      'bark_split_moveright',
      'bark_walinspect',
      'bark_validate',
      'bark_bottomup',
    ],
    'regress_args': ['--temp-config', files('../injection_points/extra.conf')],
    # The injection points are run in the same backend, so disable
    # parallelism to keep the local-attach semantics deterministic.
    'runningcheck': false,
  },
  'isolation': {
    'specs': [
      'bark_concurrent_split',
      'bark_vacuum_split',
      'bark_deleted_page',
      'bark_nnd_upsert',
      'bark_recently_dead',
      'bark_broken_hot_chain',
      'bark_lock_right',
      'bark_root_split_race',
      'bark_walk_left',
    ],
    'regress_args': ['--temp-config', files('../injection_points/extra.conf')],
    # Injection points are cluster-wide.
    'runningcheck': false,
  },
  'tap': {
    'tests': [
      't/001_empty_index_race.pl',
      't/002_standby.pl',
      't/003_split_recovery.pl',
      't/004_insert_fpi_recovery.pl',
      't/005_insert_done_recovery.pl',
      't/006_split_fpi_recovery.pl',
      't/007_split_done_recovery.pl',
      't/008_redo_dropped_index.pl',
      't/009_validate_single_user.pl',
      't/010_bottomup_wal_level.pl',
    ],
  },
}
