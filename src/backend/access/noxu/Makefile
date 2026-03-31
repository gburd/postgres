#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/noxu
#
# IDENTIFICATION
#    src/backend/access/noxu/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/noxu
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = noxu_array.o noxu_btree.o noxu_tiditem.o noxu_tidpage.o \
       noxu_attitem.o noxu_attpage.o \
       noxu_compression.o noxu_dict.o noxu_fsst.o noxu_simple8b.o noxu_uuid.o \
       noxu_handler.o \
       noxu_meta.o \
       noxu_overflow.o noxu_visibility.o noxu_inspect.o \
       noxu_freepagemap.o noxu_tupslot.o noxu_undostubs.o noxu_wal.o noxu_planner.o \
       noxu_rollback.o noxu_stats.o

include $(top_srcdir)/src/backend/common.mk
