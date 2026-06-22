# src/test/modules/test_recno_posting/Makefile

MODULE_big = test_recno_posting
OBJS = \
	$(WIN32RES) \
	test_recno_posting.o
PGFILEDESC = "test_recno_posting - test code for the RECNO posting codec"

EXTENSION = test_recno_posting
DATA = test_recno_posting--1.0.sql

REGRESS = test_recno_posting

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_recno_posting
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
