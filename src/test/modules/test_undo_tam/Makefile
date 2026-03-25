# src/test/modules/test_undo_tam/Makefile

MODULE_big = test_undo_tam
OBJS = \
	$(WIN32RES) \
	test_undo_tam.o
PGFILEDESC = "test_undo_tam - test table AM using per-relation UNDO"

EXTENSION = test_undo_tam
DATA = test_undo_tam--1.0.sql

REGRESS = relundo

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_undo_tam
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
