# src/test/modules/test_sparsemap/Makefile

MODULE_big = test_sparsemap
OBJS = \
	$(WIN32RES) \
	test_sparsemap.o
PGFILEDESC = "test_sparsemap - test code for src/include/lib/sparsemap.h"

EXTENSION = test_sparsemap
DATA = test_sparsemap--1.0.sql

REGRESS = test_sparsemap

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_sparsemap
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
