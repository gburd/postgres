# src/test/modules/test_bufmgr/Makefile

EXTRA_INSTALL = contrib/pg_buffercache contrib/pg_prewarm

# NOTE: the classic pg_regress harness runs everything in REGRESS through one
# pg_regress invocation against one temp instance, so a single --temp-config
# applies uniformly, unlike the meson build (see meson.build), which gives
# the core tests and clock-sweep tests a small shared_buffers.
REGRESS_OPTS = --temp-config $(top_srcdir)/src/test/modules/test_bufmgr/test_bufmgr_small.conf
REGRESS = test_bufmgr_eviction_reload \
	test_bufmgr_dirty_persistence \
	test_bufmgr_scan_no_retain \
	test_bufmgr_truncate_stale \
	test_bufmgr_drop_recreate_stale \
	test_bufmgr_local_eviction \
	test_bufmgr_churn

ISOLATION = test_bufmgr_pinned_safety
ISOLATION_OPTS = --temp-config $(top_srcdir)/src/test/modules/test_bufmgr/test_bufmgr_small.conf

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
