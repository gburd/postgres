# src/test/modules/test_skiplist/Makefile

MODULE_big = test_skiplist
OBJS = \
	$(WIN32RES) \
	test_skiplist.o
PGFILEDESC = "test_skiplist - test code for src/include/lib/skiplist.h"

EXTENSION = test_skiplist
DATA = test_skiplist--1.0.sql

REGRESS = test_skiplist

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_skiplist
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
