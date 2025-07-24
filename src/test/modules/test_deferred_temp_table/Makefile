# src/test/modules/test_deferred_temp_table/Makefile

PGFILEDESC = "test_deferred_temp_table - test module for deferred temp table placement"

REGRESS = test_deferred_temp_table

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_deferred_temp_table
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif