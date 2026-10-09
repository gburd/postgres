# src/test/modules/bark_multikey/Makefile

MODULE_big = bark_multikey
OBJS = \
	$(WIN32RES) \
	bark_multikey.o
PGFILEDESC = "bark_multikey - test code for BARK multikey operator classes"

EXTENSION = bark_multikey
DATA = bark_multikey--1.0.sql

REGRESS = bark_multikey

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/bark_multikey
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
