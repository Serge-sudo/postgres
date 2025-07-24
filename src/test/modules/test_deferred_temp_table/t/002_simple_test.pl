# Copyright (c) 2025, PostgreSQL Global Development Group

# Simple test for deferred temp table placement feature
use strict;
use warnings FATAL => 'all';
use Test::More;

# Simple functionality test - just test that the parameter works
plan tests => 1;

# Test that we can check for the GUC parameter existence
my $result = qx{echo "SELECT COUNT(*) FROM pg_settings WHERE name = 'deferred_temp_table_placement';" | /tmp/pg-install/usr/local/pgsql/bin/postgres --single -D /tmp/test_db template1 2>/dev/null | grep '= "1"'};

chomp($result);
ok($result ne '', 'deferred_temp_table_placement GUC parameter exists');

done_testing();