# src/test/modules/test_versioned_tableam/Makefile

MODULES = test_versioned_tableam

EXTENSION = test_versioned_tableam
DATA = test_versioned_tableam--1.0.sql
PGFILEDESC = "test_versioned_tableam - test version-tag plumbing"

EXTRA_INSTALL = contrib/amcheck contrib/pageinspect

REGRESS = versioned
ISOLATION = cic_versioned
TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_versioned_tableam
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
