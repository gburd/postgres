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
	relundo.o \
	relundo_discard.o \
	relundo_page.o \
	relundo_xlog.o \
	undo.o \
	undo_bufmgr.o \
	undo_xlog.o \
	undoapply.o \
	undoinsert.o \
	undolog.o \
	undorecord.o \
	undostats.o \
	undoworker.o \
	xactundo.o

include $(top_srcdir)/src/backend/common.mk
