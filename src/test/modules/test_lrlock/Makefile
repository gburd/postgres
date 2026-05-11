# src/test/modules/test_lrlock/Makefile

MODULE_big = test_lrlock
OBJS = \
	$(WIN32RES) \
	test_lrlock.o
PGFILEDESC = "test_lrlock - test code for the left-right lock primitive"

EXTENSION = test_lrlock
DATA = test_lrlock--1.0.sql

REGRESS_OPTS = --temp-config $(top_srcdir)/src/test/modules/test_lrlock/test_lrlock.conf
REGRESS = test_lrlock
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_lrlock
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
