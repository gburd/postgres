# Copyright (c) 2026, PostgreSQL Global Development Group

# Admission/promotion needs no special config -- in fact it needs the
# opposite: enough headroom that querying pg_buffercache itself doesn't
# perturb the one or two pages being tracked. Only the eviction test
# below needs shared_buffers pinned down.
tests += {
  'name': 'test_bufmgr',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_bufmgr_admission',
    ],
  },
}

tests += {
  'name': 'test_bufmgr_eviction',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_bufmgr_eviction',
    ],
    'regress_args': ['--temp-config', files('test_bufmgr.conf')],
  },
}
