# contrib/pg_fts/Makefile

MODULE_big = pg_fts
OBJS = \
	$(WIN32RES) \
	pg_fts_analyze.o \
	pg_fts_tsanalyze.o \
	pg_fts_doc.o \
	pg_fts_query.o \
	pg_fts_rank.o \
	pg_fts_am.o \
	pg_fts_match.o

EXTENSION = pg_fts
DATA = pg_fts--1.0.sql pg_fts--1.0--1.1.sql pg_fts--1.1--1.2.sql \
	pg_fts--1.2--1.3.sql \
	pg_fts--1.3--1.4.sql
PGFILEDESC = "pg_fts - next-generation full-text search"

REGRESS = pg_fts

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_fts
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
