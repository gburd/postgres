#
# Makefile for the rmgr descriptor routines
#
# src/backend/access/rmgrdesc/Makefile
#

subdir = src/backend/access/rmgrdesc
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	atmdesc.o \
	brindesc.o \
	clogdesc.o \
	committsdesc.o \
	dbasedesc.o \
	genericdesc.o \
	gindesc.o \
	gistdesc.o \
	hashdesc.o \
	heapdesc.o \
	logicalmsgdesc.o \
	mxactdesc.o \
	nbtdesc.o \
	relmapdesc.o \
	relundodesc.o \
	replorigindesc.o \
	rmgrdesc_utils.o \
	seqdesc.o \
	smgrdesc.o \
	spgdesc.o \
	standbydesc.o \
	tblspcdesc.o \
	undodesc.o \
	xactdesc.o \
	xlogdesc.o

endif

ifdef USE_FLUX
OBJS += fluxdesc.o
endif

include $(top_srcdir)/src/backend/common.mk
