# contrib/pg_bp_lirs/Makefile

MODULE_big = pg_bp_lirs
OBJS = \
	$(WIN32RES) \
	pg_bp_lirs.o

EXTENSION = pg_bp_lirs
DATA = pg_bp_lirs--1.0.sql
PGFILEDESC = "pg_bp_lirs - buffer pool replacement strategy"

REGRESS = pg_bp_lirs

NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_bp_lirs
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
