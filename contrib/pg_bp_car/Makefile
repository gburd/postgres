# contrib/pg_bp_car/Makefile

MODULE_big = pg_bp_car
OBJS = \
	$(WIN32RES) \
	pg_bp_car.o

EXTENSION = pg_bp_car
DATA = pg_bp_car--1.0.sql
PGFILEDESC = "pg_bp_car - buffer pool replacement strategy"

REGRESS = pg_bp_car

NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_bp_car
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
