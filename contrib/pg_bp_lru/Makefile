# contrib/pg_bp_lru/Makefile

MODULE_big = pg_bp_lru
OBJS = \
	$(WIN32RES) \
	pg_bp_lru.o

EXTENSION = pg_bp_lru
DATA = pg_bp_lru--1.0.sql
PGFILEDESC = "pg_bp_lru - buffer pool replacement strategy"

REGRESS = pg_bp_lru

NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_bp_lru
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
