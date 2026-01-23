#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for foreign
#
# IDENTIFICATION
#    src/backend/foreign/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/foreign
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = \
	conn_multiplexer.o \
	foreign.o

include $(top_srcdir)/src/backend/common.mk
