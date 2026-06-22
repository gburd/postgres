# Copyright (c) 2022-2026, PostgreSQL Global Development Group

tests += {
  'name': 'recno',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_basic_operations.pl',
      't/002_crash_recovery.pl',
      't/003_replication.pl',
      't/004_concurrent_access.pl',
      't/005_wal_consistency.pl',
      't/006_escrow_crash.pl',
    ],
  },
}
