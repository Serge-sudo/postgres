# Copyright (c) 2025, PostgreSQL Global Development Group

# Test deferred temp table placement feature basic functionality
use strict;
use warnings FATAL => 'all';
use Test::More;

# Test plan
plan tests => 4;

# Test 1: Verify GUC parameter exists
my $exists_result = qx{echo "SELECT COUNT(*) FROM pg_settings WHERE name = 'deferred_temp_table_placement';" | /tmp/pg-install/usr/local/pgsql/bin/postgres --single -D /tmp/test_db template1 2>/dev/null | grep '= "1"'};
chomp($exists_result);
ok($exists_result ne '', 'deferred_temp_table_placement GUC parameter exists');

# Test 2: Verify GUC parameter default value
my $default_result = qx{echo "SHOW deferred_temp_table_placement;" | /tmp/pg-install/usr/local/pgsql/bin/postgres --single -D /tmp/test_db template1 2>/dev/null | grep '= "off"'};
chomp($default_result);
ok($default_result ne '', 'deferred_temp_table_placement defaults to off');

# Test 3: Verify basic temp table operations work (with feature off)
my $temp_table_result = qx{echo "CREATE TEMP TABLE test_basic (id int); INSERT INTO test_basic VALUES (1), (2), (3); SELECT COUNT(*) FROM test_basic;" | /tmp/pg-install/usr/local/pgsql/bin/postgres --single -D /tmp/test_db template1 2>/dev/null | grep '= "3"'};
chomp($temp_table_result);
ok($temp_table_result ne '', 'Basic temp table operations work');

# Test 4: Verify GUC parameter properties
my $context_result = qx{echo "SELECT context FROM pg_settings WHERE name = 'deferred_temp_table_placement';" | /tmp/pg-install/usr/local/pgsql/bin/postgres --single -D /tmp/test_db template1 2>/dev/null | grep 'postmaster'};
chomp($context_result);
ok($context_result ne '', 'deferred_temp_table_placement has correct context (postmaster)');

done_testing();

# Note: More comprehensive testing would require starting PostgreSQL with the
# feature enabled, which needs a proper test cluster. This basic test ensures
# the feature compiles correctly and doesn't break basic operations.