# src/test/modules/test_versioned_tableam/Makefile

MODULES = test_versioned_tableam

EXTENSION = test_versioned_tableam
DATA = test_versioned_tableam--1.0.sql
PGFILEDESC = "test_versioned_tableam - test version-tag plumbing"

EXTRA_INSTALL = contrib/amcheck contrib/pageinspect src/test/modules/bark_multikey contrib/pg_walinspect \
	src/test/modules/injection_points

REGRESS = versioned bark_mk_versioned
REGRESS_OPTS = --temp-config $(top_srcdir)/src/test/modules/test_versioned_tableam/versioned.conf
ISOLATION = cic_versioned cic_bark
ISOLATION_OPTS = --temp-config $(top_srcdir)/src/test/modules/test_versioned_tableam/versioned.conf
TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_versioned_tableam
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
# the TAP tests and bark_lower_bound need injection points
export enable_injection_points
ifeq ($(enable_injection_points),yes)
ISOLATION += bark_lower_bound
endif
include $(top_srcdir)/contrib/contrib-global.mk
endif
