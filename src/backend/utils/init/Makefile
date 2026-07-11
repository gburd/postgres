#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/init
#
# IDENTIFICATION
#    src/backend/utils/init/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/utils/init
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	backend_runtime.o \
	backend_runtime_backend.o \
	backend_runtime_execution.o \
	backend_runtime_session.o \
	backend_runtime_teardown.o \
	globals.o \
	miscinit.o \
	postinit.o \
	usercontext.o

backend_runtime.o: backend_runtime_carrier_buckets.def \
	backend_runtime_connection_buckets.def \
	backend_runtime_execution_buckets.def \
	backend_runtime_session_buckets.def \
	backend_runtime_session_reset_buckets.def \
	$(top_srcdir)/src/include/utils/backend_runtime_hot_buckets.def \
	$(top_srcdir)/src/include/utils/backend_runtime_hot_fields.def

backend_runtime_backend.o: backend_runtime_backend_buckets.def

backend_runtime_execution.o: backend_runtime_execution_buckets.def

backend_runtime_teardown.o: backend_runtime_backend_buckets.def \
	backend_runtime_execution_buckets.def \
	backend_runtime_session_reset_buckets.def

include $(top_srcdir)/src/backend/common.mk
