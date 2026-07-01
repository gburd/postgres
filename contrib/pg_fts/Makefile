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
	pg_fts_aux.o \
	pg_fts_migrate.o \
	pg_fts_match.o

EXTENSION = pg_fts
DATA = pg_fts--1.0.sql pg_fts--1.0--1.1.sql pg_fts--1.1--1.2.sql \
	pg_fts--1.2--1.3.sql \
	pg_fts--1.3--1.4.sql \
	pg_fts--1.4--1.5.sql \
	pg_fts--1.5--1.6.sql \
	pg_fts--1.6--1.7.sql \
	pg_fts--1.7--1.8.sql \
	pg_fts--1.8--1.9.sql \
	pg_fts--1.9--1.10.sql \
	pg_fts--1.10--1.11.sql \
	pg_fts--1.11--1.12.sql \
	pg_fts--1.12--1.13.sql
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
