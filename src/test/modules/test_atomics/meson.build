test_atomics_sources = files(
  'test_atomics.c',
)

if host_system == 'windows'
  test_atomics_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_atomics',
    '--FILEDESC', 'test_atomics - test module for atomic operations',])
endif

test_atomics = shared_module('test_atomics',
  test_atomics_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_atomics

test_install_data += files(
  'test_atomics.control',
  'test_atomics--1.0.sql',
)

tests += {
  'name': 'test_atomics',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_atomics',
    ],
  },
}
