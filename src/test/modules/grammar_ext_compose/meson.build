# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Build matrix for grammar_ext_compose: six small extension .so files
# exercising the parser_extension.h API in different combinations.
# A single TAP test (t/001_compose.pl) loads them in mixed
# shared_preload_libraries configurations and asserts the rebuild
# pipeline + cache keys + reduce dispatch + base-grammar preservation.

# Each extension is its own .so so the test harness can load arbitrary
# subsets via shared_preload_libraries without recompiling.

ext_compose_kw = pg_test_mod_args + {
  # nothing extra; pg_test_mod_args already includes the parser API headers
}

# alpha:    K_GRAMMAR_ALPHA + K_GRAMMAR_BRAVO + 2 stmt-orphan rules
# beta:     K_GRAMMAR_CHARLIE + K_GRAMMAR_DELTA + 1 stmt-orphan rule + 1 prec
# echo:     K_GRAMMAR_ALPHA again, SAME lexeme "grammar_alpha" -- should be no-op
# foxtrot:  K_GRAMMAR_ALPHA again, DIFFERENT lexeme -- should fail register()
# golf:     uses tokens declared by alpha (cross-extension dep ordering)
# hotel:    sets precedence on K_GRAMMAR_BRAVO that conflicts with foxtrot
foreach ext_name : ['alpha', 'beta', 'echo', 'foxtrot', 'golf', 'hotel']
  src = 'compose_ext_' + ext_name + '.c'
  if host_system == 'windows'
    rc = rc_lib_gen.process(win32ver_rc, extra_args: [
      '--NAME', 'grammar_ext_compose_' + ext_name,
      '--FILEDESC', 'grammar_ext_compose ' + ext_name + ' - parser ext torture',])
    so = shared_module('grammar_ext_compose_' + ext_name,
      [src, rc],
      kwargs: ext_compose_kw,
    )
  else
    so = shared_module('grammar_ext_compose_' + ext_name,
      [src],
      kwargs: ext_compose_kw,
    )
  endif
  test_install_libs += so
endforeach

tests += {
  'name': 'grammar_ext_compose',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_compose.pl',
    ],
  },
}
