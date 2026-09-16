# Copyright (c) 2022-2026, PostgreSQL Global Development Group

tests += {
  'name': 'ldap',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_auth.pl',
      't/002_bindpasswd.pl',
      't/003_ldap_connection_param_lookup.pl',
    ],
    'env': {
      'with_ldap': ldap.found() ? 'yes' : 'no',
    },
  },
  'pytest': {
    'tests': [
      'pyt/test_001_auth.py',
      'pyt/test_002_bindpasswd.py',
      'pyt/test_003_ldap_connection_param_lookup.py',
    ],
    'env': {
      'with_ldap': ldap.found() ? 'yes' : 'no',
    },
  },
}
