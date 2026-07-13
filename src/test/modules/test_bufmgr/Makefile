# src/test/modules/test_bufmgr/Makefile

EXTRA_INSTALL = contrib/pg_buffercache

# NOTE: the classic pg_regress harness runs everything in REGRESS through
# one pg_regress invocation against one temp instance, so REGRESS_OPTS
# applies uniformly -- unlike the meson build (see meson.build), which
# gives test_bufmgr_eviction its own instance with a tiny shared_buffers
# and leaves test_bufmgr_admission on the normal-sized default. Under
# `make check`/`installcheck` both tests share the tiny pool below; that
# is a known limitation of this fallback path, not the meson path.
REGRESS_OPTS = --temp-config $(top_srcdir)/src/test/modules/test_bufmgr/test_bufmgr.conf
REGRESS = test_bufmgr_admission test_bufmgr_eviction

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_bufmgr
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
