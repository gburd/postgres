# Copyright (c) 2026, PostgreSQL Global Development Group

test_backend_runtime_sources = files(
  'test_backend_runtime.c',
  'test_backend_runtime_adoption.c',
  'test_backend_runtime_backend.c',
  'test_backend_runtime_backend_core.c',
  'test_backend_runtime_backend_interrupt.c',
  'test_backend_runtime_backend_subsystems.c',
  'test_backend_runtime_carrier.c',
  'test_backend_runtime_connection.c',
  'test_backend_runtime_dsm.c',
  'test_backend_runtime_execution.c',
  'test_backend_runtime_exit.c',
  'test_backend_runtime_pmchild.c',
  'test_backend_runtime_session.c',
  'test_backend_runtime_session_cache.c',
  'test_backend_runtime_session_guc.c',
  'test_backend_runtime_session_guc_core.c',
  'test_backend_runtime_session_guc_planner.c',
  'test_backend_runtime_thread.c',
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
      't/002_threaded_bgworker_crash.pl',
      't/003_milestone_w_core_smoke.pl',
      't/004_phase13_wait_completion.pl',
      't/005_phase14_protocol_scheduler.pl',
      't/006_phase14_protocol_postmaster_death.pl',
      't/007_phase15_pooled_protocol_mode.pl',
      't/008_phase15_pooled_protocol_postmaster_death.pl',
      't/009_phase15_pooled_deep_waits_pinned.pl',
      't/010_phase16_pooled_crash_recovery.pl',
      't/011_phase16_pooled_plan_cache_teardown.pl',
      't/012_phase16_pooled_plperl_affine.pl',
      't/013_phase19_process_fallback_crash.pl',
      't/014_phase_c_d_forced_migration.pl',
    ],
  },
}
