# src/test/modules/test_sharding/Makefile

EXTRA_INSTALL=contrib/postgres_fdw

subdir = src/test/modules/test_sharding
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)

clean distclean maintainer-clean:
	rm -rf tmp_check
