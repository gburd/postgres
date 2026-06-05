# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Smoke test for Lime's in-process compile API (Phase 4 Track B
# Phase 2 foundation).

if not lime_runtime_dep.found() or not lime_compiler_dep.found()
  subdir_done()
endif

lime_in_process_smoke_sources = files('lime_in_process_smoke.c')

if host_system == 'windows'
  lime_in_process_smoke_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'lime_in_process_smoke',
    '--FILEDESC', 'lime_in_process_smoke - in-process compile API smoke test',])
endif

lime_smoke_kw = pg_test_mod_args + {
  'dependencies': pg_test_mod_args.get('dependencies', []) + [lime_runtime_dep, lime_compiler_dep],
  # liblime_compiler.a's lime_compile_grammar_in_process overrides
  # liblime_parser.a's weak no-op stub.  Standard linkage drops the
  # .a file entirely if no other-strong-symbol reference pulls it
  # in -- which means the weak stub stays resolved and we silently
  # fall back to subprocess.  Force the compiler archive to be
  # link-whole'd so its strong symbol overrides the weak.
  'link_args': pg_test_mod_args.get('link_args', []) + [
    '-Wl,--push-state',
    '-Wl,--whole-archive',
    '-llime_compiler',
    '-Wl,--no-whole-archive',
    '-Wl,--pop-state',
  ],
}

lime_in_process_smoke = shared_module('lime_in_process_smoke',
  lime_in_process_smoke_sources,
  kwargs: lime_smoke_kw,
)
test_install_libs += lime_in_process_smoke

tests += {
  'name': 'lime_in_process_smoke',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_smoke.pl',
    ],
  },
}
