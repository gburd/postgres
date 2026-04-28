# contrib/pg_bp_rcrd/Makefile

MODULE_big = pg_bp_rcrd
OBJS = \
	$(WIN32RES) \
	pg_bp_rcrd.o

EXTENSION = pg_bp_rcrd
DATA = pg_bp_rcrd--1.0.sql
PGFILEDESC = "pg_bp_rcrd - buffer pool replacement strategy"

REGRESS = pg_bp_rcrd

NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_bp_rcrd
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
