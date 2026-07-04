# contrib/pg_bp_clir/Makefile

MODULE_big = pg_bp_clir
OBJS = \
	$(WIN32RES) \
	pg_bp_clir.o

EXTENSION = pg_bp_clir
DATA = pg_bp_clir--1.0.sql
PGFILEDESC = "pg_bp_clir - lock-light Clock-LIRS buffer pool replacement strategy"

REGRESS = pg_bp_clir

NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_bp_clir
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
