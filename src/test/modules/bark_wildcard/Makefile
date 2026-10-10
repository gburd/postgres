# src/test/modules/bark_wildcard/Makefile

MODULE_big = bark_wildcard
OBJS = \
	$(WIN32RES) \
	bark_wildcard.o
PGFILEDESC = "bark_wildcard - a wildcard operator class for BARK over jsonb"

EXTENSION = bark_wildcard
DATA = bark_wildcard--1.0.sql

REGRESS = bark_wildcard bark_wildcard_scan
EXTRA_INSTALL = contrib/amcheck contrib/pageinspect
# Paths and strings sort by the database's collation; keep the output stable.
NO_LOCALE = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/bark_wildcard
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
