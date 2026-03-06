#
# Makefile for the access methods module
#
# src/backend/access/Makefile
#

subdir = src/backend/access
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS	    = \
	brin \
	common \
	gin \
	gist \
	hash \
	heap \
	index \
	nbtree \
	rmgrdesc \
	spgist \
	sequence \
	table \
	tablesample \
	transam

# Orvos table AM (optional, enabled by default)
ifeq ($(with_orvos),yes)
SUBDIRS += orvos
endif

include $(top_srcdir)/src/backend/common.mk
