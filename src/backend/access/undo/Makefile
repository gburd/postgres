#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/undo
#
# IDENTIFICATION
#    src/backend/access/undo/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/undo
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	atm.o \
	logical_revert_worker.o \
	relundo.o \
	relundo_apply.o \
	relundo_discard.o \
	relundo_page.o \
	relundo_worker.o \
	relundo_xlog.o \
	slog.o \
	undo.o \
	undo_bufmgr.o \
	undo_flush.o \
	undo_xlog.o \
	undoapply.o \
	undoinsert.o \
	undolog.o \
	undorecord.o \
	undormgr.o \
	undostats.o \
	undoworker.o \
	xactundo.o

include $(top_srcdir)/src/backend/common.mk
