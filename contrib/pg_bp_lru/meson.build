# Copyright (c) 2022-2026, PostgreSQL Global Development Group

pg_bp_lru_sources = files(
  'pg_bp_lru.c',
)

if host_system == 'windows'
  pg_bp_lru_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_bp_lru',
    '--FILEDESC', 'pg_bp_lru - LRU buffer pool replacement strategy',])
endif

pg_bp_lru = shared_module('pg_bp_lru',
  pg_bp_lru_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_bp_lru

install_data(
  'pg_bp_lru--1.0.sql',
  'pg_bp_lru.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_bp_lru',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_bp_lru',
    ],
  },
  'tap': {
    'env': {
      'enable_injection_points': get_option('injection_points') ? 'yes' : 'no',
    },
    'tests': [
      't/001_drop_concurrency.pl',
      't/002_reservation_pool.pl',
      't/003_aio_reservation_pool.pl',
      't/004_huge_pages_pool.pl',
      't/005_numa_interleave_pool.pl',
      't/006_algorithm_swap.pl',
      't/007_disjoint_fragmentation.pl',
      't/008_resize_under_load.pl',
      't/009_swap_reader_race.pl',
      't/010_create_drop_race.pl',
    ],
  },
}
