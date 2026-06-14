#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/buffer
#
# IDENTIFICATION
#    src/backend/storage/buffer/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/buffer
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	backend_runtime_buffer.o \
	buf_init.o \
	buf_table.o \
	bufmgr.o \
	freelist.o \
	localbuf.o

include $(top_srcdir)/src/backend/common.mk
