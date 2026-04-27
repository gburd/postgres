# contrib/pg_bp_arc/Makefile

MODULE_big = pg_bp_arc
OBJS = \
	$(WIN32RES) \
	pg_bp_arc.o

EXTENSION = pg_bp_arc
DATA = pg_bp_arc--1.0.sql
PGFILEDESC = "pg_bp_arc - buffer pool replacement strategy"

REGRESS = pg_bp_arc

NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_bp_arc
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
