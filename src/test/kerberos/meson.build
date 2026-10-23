# Copyright (c) 2022-2026, PostgreSQL Global Development Group

tests += {
  'name': 'kerberos',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'test_kwargs': {'priority': 40}, # kerberos tests are slow, start early
    'tests': [
      't/001_auth.pl',
    ],
    'env': {
      'with_gssapi': gssapi.found() ? 'yes' : 'no',
      'with_krb_srvnam': 'postgres',
    },
  },
  'pytest': {
    'test_kwargs': {'priority': 40}, # kerberos tests are slow, start early
    'tests': [
      'pyt/test_001_auth.py',
    ],
    'env': {
      'with_gssapi': gssapi.found() ? 'yes' : 'no',
      'with_krb_srvnam': 'postgres',
      'enable_injection_points': get_option('injection_points') ? 'yes' : 'no',
    },
  },
}
