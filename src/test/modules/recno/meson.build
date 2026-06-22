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
      't/006_inplace_torn_read.pl',
      't/007_inplace_rollback_reclaim.pl',
      't/008_inplace_grow_rollback_reclaim.pl',
      't/009_epq_torn_read.pl',
      't/010_vacuum_reloc_rollback.pl',
      't/011_cas_inplace_torn_read.pl',
      't/012_inplace_index_dup_keytid.pl',
      't/013_index_fetch_all_dead_sticky.pl',
    ],
  },
}
