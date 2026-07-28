# Copyright (c) 2026, PostgreSQL Global Development Group

tests += {
  'name': 'test_bufmgr',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_bufmgr_eviction_reload',
      'test_bufmgr_dirty_persistence',
      'test_bufmgr_scan_no_retain',
      'test_bufmgr_truncate_stale',
      'test_bufmgr_drop_recreate_stale',
      'test_bufmgr_local_eviction',
      'test_bufmgr_churn',
    ],
    'regress_args': ['--temp-config', files('test_bufmgr_small.conf')],
  },
  'isolation': {
    'specs': [
      'test_bufmgr_pinned_safety',
    ],
    'regress_args': ['--temp-config', files('test_bufmgr_small.conf')],
  },
}
