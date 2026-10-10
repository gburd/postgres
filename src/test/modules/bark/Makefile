# src/test/modules/bark/Makefile

EXTRA_INSTALL = src/test/modules/injection_points src/test/modules/bark_multikey \
	contrib/amcheck contrib/pg_buffercache contrib/pageinspect \
	contrib/pg_freespacemap

REGRESS = bark_incomplete_splits bark_split_moveright
ISOLATION = bark_concurrent_split bark_vacuum_split bark_deleted_page \
	bark_nnd_upsert bark_recently_dead bark_broken_hot_chain \
	bark_lock_right bark_root_split_race
TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/bark
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

# This test needs injection points, which the parent Makefile only descends
# into when they are enabled.  Guard here too so the buildfarm misc-check step
# (which ignores that condition) does not try to run it.
ifeq ($(enable_injection_points),yes)
include $(top_srcdir)/contrib/contrib-global.mk
else
check:
	@echo "injection points are disabled in this build"
endif

endif
