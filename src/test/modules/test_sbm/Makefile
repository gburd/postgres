# src/test/modules/test_sbm/Makefile

MODULE_big = test_sbm
OBJS = \
	$(WIN32RES) \
	test_sbm.o
PGFILEDESC = "test_sbm - test code for the sparse bitmap set (sbm) ADT"

EXTENSION = test_sbm
DATA = test_sbm--1.0.sql

REGRESS = test_sbm

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_sbm
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
