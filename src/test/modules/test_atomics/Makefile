# src/test/modules/test_atomics/Makefile

MODULE_big = test_atomics
OBJS = \
	$(WIN32RES) \
	test_atomics.o
PGFILEDESC = "test_atomics - test module for atomic operations"

EXTENSION = test_atomics
DATA = test_atomics--1.0.sql

REGRESS = test_atomics

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_atomics
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
