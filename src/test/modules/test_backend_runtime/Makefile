# src/test/modules/test_backend_runtime/Makefile

MODULE_big = test_backend_runtime
MODULES = test_backend_runtime_threaded
OBJS = \
	$(WIN32RES) \
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
