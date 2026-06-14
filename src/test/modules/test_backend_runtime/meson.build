# Copyright (c) 2026, PostgreSQL Global Development Group

test_backend_runtime_sources = files(
  'test_backend_runtime.c',
  'test_backend_runtime_backend.c',
  'test_backend_runtime_connection.c',
  'test_backend_runtime_execution.c',
  'test_backend_runtime_session.c',
)

if host_system == 'windows'
  test_backend_runtime_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_backend_runtime',
    '--FILEDESC', 'test_backend_runtime - test code for backend runtime scaffolding',])
endif

test_backend_runtime = shared_module('test_backend_runtime',
  test_backend_runtime_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_backend_runtime

test_backend_runtime_threaded = shared_module('test_backend_runtime_threaded',
  files('test_backend_runtime_threaded.c'),
  kwargs: pg_test_mod_args,
)
test_install_libs += test_backend_runtime_threaded

test_install_data += files(
  'test_backend_runtime.control',
  'test_backend_runtime--1.0.sql',
  'test_backend_runtime_threaded.control',
  'test_backend_runtime_threaded--1.0.sql',
)

tests += {
  'name': 'test_backend_runtime',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_backend_runtime',
    ],
  },
  'tap': {
    'tests': [
      't/001_threaded_runtime.pl',
    ],
  },
}
