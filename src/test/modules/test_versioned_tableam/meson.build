# Copyright (c) 2026, PostgreSQL Global Development Group

test_versioned_tableam_sources = files(
  'test_versioned_tableam.c',
)

if host_system == 'windows'
  test_versioned_tableam_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_versioned_tableam',
    '--FILEDESC', 'test_versioned_tableam - test version-tag plumbing',])
endif

test_versioned_tableam = shared_module('test_versioned_tableam',
  test_versioned_tableam_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_versioned_tableam

test_install_data += files(
  'test_versioned_tableam.control',
  'test_versioned_tableam--1.0.sql',
)

tests += {
  'name': 'test_versioned_tableam',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'versioned',
    ],
    'regress_args': ['--temp-config', files('versioned.conf')],
    'runningcheck': false,
  },
  'isolation': {
    'specs': [
      'cic_versioned',
      'cic_bark',
    ],
    'regress_args': ['--temp-config', files('versioned.conf')],
    'runningcheck': false,
  },
  'tap': {
    'env': {
       'enable_injection_points': get_option('injection_points') ? 'yes' : 'no',
    },
    'tests': [
      't/001_apply.pl',
      't/002_bark_tags.pl',
      't/003_bark_readers.pl',
      't/004_bark_vacuum.pl',
      't/005_bark_more.pl',
      't/006_bark_corrupt.pl',
    ],
  },
}
