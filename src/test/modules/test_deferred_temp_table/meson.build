test_deferred_temp_table_sources = []

if host_system == 'windows'
  test_deferred_temp_table_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_deferred_temp_table',
    '--FILEDESC', 'test_deferred_temp_table - test module for deferred temp table placement',])
endif

test_deferred_temp_table = shared_module('test_deferred_temp_table',
  test_deferred_temp_table_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_deferred_temp_table

test_install_data += files(
  'test_deferred_temp_table.control',
)

tests += {
  'name': 'test_deferred_temp_table',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_deferred_temp_table',
    ],
  },
}