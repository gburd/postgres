# src/test/modules/test_index_impl/Makefile

MODULES = test_index_impl

EXTENSION = test_index_impl
DATA = test_index_impl--1.0.sql
PGFILEDESC = "test_index_impl - index access methods implementing btree indexes"

EXTRA_INSTALL = contrib/amcheck contrib/pageinspect contrib/pgstattuple

REGRESS = test_index_impl
TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_index_impl
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
