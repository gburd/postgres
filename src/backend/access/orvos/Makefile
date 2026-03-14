#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/orvos
#
# IDENTIFICATION
#    src/backend/access/orvos/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/orvos
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = orvos_btree.o orvos_tiditem.o orvos_tidpage.o \
       orvos_attitem.o orvos_attpage.o \
       orvos_compression.o orvos_dict.o orvos_fsst.o orvos_simple8b.o \
       orvos_handler.o \
       orvos_meta.o orvos_undolog.o orvos_undorec.o \
       orvos_overflow.o orvos_visibility.o orvos_inspect.o \
       orvos_freepagemap.o orvos_tupslot.o orvos_wal.o orvos_planner.o \
       orvos_stats.o

include $(top_srcdir)/src/backend/common.mk
