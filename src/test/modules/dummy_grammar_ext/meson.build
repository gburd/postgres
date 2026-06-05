# Copyright (c) 2022-2026, PostgreSQL Global Development Group

dummy_grammar_ext_sources = files(
  'dummy_grammar_ext.c',
)

if host_system == 'windows'
  dummy_grammar_ext_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'dummy_grammar_ext',
    '--FILEDESC', 'dummy_grammar_ext - grammar extension API smoke test',])
endif

dummy_grammar_ext = shared_module('dummy_grammar_ext',
  dummy_grammar_ext_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += dummy_grammar_ext
