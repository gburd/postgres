# src/test/modules/test_backend_runtime/Makefile

MODULE_big = test_backend_runtime
MODULES = test_backend_runtime_threaded
OBJS = \
	$(WIN32RES) \
	test_backend_runtime_adoption.o \
	test_backend_runtime_backend.o \
	test_backend_runtime_backend_core.o \
	test_backend_runtime_backend_interrupt.o \
	test_backend_runtime_backend_subsystems.o \
	test_backend_runtime_carrier.o \
	test_backend_runtime_connection.o \
	test_backend_runtime_dsm.o \
	test_backend_runtime_execution.o \
	test_backend_runtime_exit.o \
	test_backend_runtime_pmchild.o \
	test_backend_runtime_session.o \
	test_backend_runtime_session_cache.o \
	test_backend_runtime_session_guc.o \
	test_backend_runtime_session_guc_core.o \
	test_backend_runtime_session_guc_planner.o \
	test_backend_runtime_thread.o \
	test_backend_runtime.o
PGFILEDESC = "test_backend_runtime - test code for backend runtime scaffolding"

EXTENSION = test_backend_runtime test_backend_runtime_threaded
DATA = test_backend_runtime--1.0.sql test_backend_runtime_threaded--1.0.sql

REGRESS = test_backend_runtime

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_backend_runtime
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
