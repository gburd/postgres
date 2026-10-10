# src/test/modules/bark_multikey/Makefile

MODULE_big = bark_multikey
OBJS = \
	$(WIN32RES) \
	bark_multikey.o
PGFILEDESC = "bark_multikey - test code for BARK multikey operator classes"

EXTENSION = bark_multikey
DATA = bark_multikey--1.0.sql

REGRESS = bark_multikey bark_multikey_scan bark_multikey_unique
ISOLATION = bark_multikey_concurrent bark_multikey_unique_concurrent
EXTRA_INSTALL = contrib/amcheck src/test/modules/injection_points
TAP_TESTS = 1

export enable_injection_points

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
