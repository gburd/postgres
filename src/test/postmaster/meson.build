# Copyright (c) 2022-2026, PostgreSQL Global Development Group

tests += {
  'name': 'postmaster',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'pytest': {
    'tests': [
      'pyt/test_001_basic.py',
      'pyt/test_002_connection_limits.py',
      'pyt/test_003_start_stop.py',
      'pyt/test_004_negotiate.py',
    ],
  },
  'tap': {
    'tests': [
      't/001_basic.pl',
      't/002_connection_limits.pl',
      't/003_start_stop.pl',
      't/004_negotiate.pl',
    ],
  },
}
