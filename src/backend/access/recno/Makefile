#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/recno
#
# IDENTIFICATION
#    src/backend/access/recno/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/recno
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	recno_dirtymap.o \
	recno_handler.o \
	recno_tuple.o \
	recno_mvcc.o \
	recno_overflow.o \
	recno_compress.o \
	recno_stats.o \
	recno_xlog.o \
	recno_fsm.o \
	recno_operations.o \
	recno_lock.o \
	recno_slot.o \
	recno_vm.o \
	recno_undo.o \
	recno_relundo.o \
	recno_escrow.o \
	recno_pvs.o \
	recno_posting.o \
	recno_dict.o

include $(top_srcdir)/src/backend/common.mk