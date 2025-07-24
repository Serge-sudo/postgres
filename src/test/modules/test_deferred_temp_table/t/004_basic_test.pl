# Copyright (c) 2025, PostgreSQL Global Development Group

# Test deferred temp table placement feature basic functionality
use strict;
use warnings FATAL => 'all';
use Test::More;

# Test plan - focus on what we can validate
plan tests => 3;

# Test 1: Verify GUC parameter exists and can be accessed
my $result = qx{echo "SELECT COUNT(*) FROM pg_settings WHERE name = 'deferred_temp_table_placement';" | /tmp/pg-install/usr/local/pgsql/bin/postgres --single -D /tmp/test_db template1 2>/dev/null | grep '= "1"'};
chomp($result);
ok($result ne '', 'deferred_temp_table_placement GUC parameter exists');

# Test 2: Verify GUC parameter can be enabled
my $setting_result = qx{echo "ALTER SYSTEM SET deferred_temp_table_placement = on; SELECT setting FROM pg_settings WHERE name = 'deferred_temp_table_placement';" | /tmp/pg-install/usr/local/pgsql/bin/postgres --single -D /tmp/test_db template1 2>/dev/null | grep '= "on"'};
chomp($setting_result);
ok($setting_result ne '', 'deferred_temp_table_placement can be set to on');

# Test 3: Verify the feature doesn't break basic temp table operations
my $temp_table_result = qx{echo "CREATE TEMP TABLE test_basic (id int); INSERT INTO test_basic VALUES (1), (2), (3); SELECT COUNT(*) FROM test_basic;" | /tmp/pg-install/usr/local/pgsql/bin/postgres --single -D /tmp/test_db template1 2>/dev/null | grep '= "3"'};
chomp($temp_table_result);
ok($temp_table_result ne '', 'Basic temp table operations work with feature enabled');

done_testing();