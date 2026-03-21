# Copyright (c) 2022-2026, PostgreSQL Global Development Group

test_undo_tam_sources = files(
  'test_undo_tam.c',
)

if host_system == 'windows'
  test_undo_tam_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_undo_tam',
    '--FILEDESC', 'test_undo_tam - test table AM using per-relation UNDO',])
endif

test_undo_tam = shared_module('test_undo_tam',
  test_undo_tam_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_undo_tam

test_install_data += files(
  'test_undo_tam.control',
  'test_undo_tam--1.0.sql',
)
