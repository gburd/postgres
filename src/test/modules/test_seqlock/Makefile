# src/test/modules/test_seqlock/Makefile

MODULE_big = test_seqlock
OBJS = \
	$(WIN32RES) \
	test_seqlock.o
PGFILEDESC = "test_seqlock - test code for src/include/storage/seqlock.h"

EXTENSION = test_seqlock
DATA = test_seqlock--1.0.sql

REGRESS = test_seqlock

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_seqlock
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
