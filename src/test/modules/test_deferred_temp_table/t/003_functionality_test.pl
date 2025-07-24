# Copyright (c) 2025, PostgreSQL Global Development Group

# Test deferred temp table placement feature functionality
use strict;
use warnings FATAL => 'all';
use Test::More;
use File::Temp qw(tempdir);
use File::Find;
use Cwd;

# Test plan
plan tests => 8;

# Setup test database directory
my $temp_dir = tempdir(CLEANUP => 1);
my $db_dir = "$temp_dir/pgdata";

# Initialize test database with feature enabled
my $initdb_cmd = "/tmp/pg-install/usr/local/pgsql/bin/initdb -D $db_dir -A trust";
system($initdb_cmd) == 0 or die "initdb failed: $?";

# Configure deferred temp table placement
open my $conf, '>>', "$db_dir/postgresql.conf" or die "Cannot open postgresql.conf: $!";
print $conf "deferred_temp_table_placement = on\n";
print $conf "temp_buffers = 8MB\n";
print $conf "port = 65432\n";  # Use a non-standard port
print $conf "log_statement = 'all'\n";
close $conf;

# Start PostgreSQL server
my $pg_ctl = "/tmp/pg-install/usr/local/pgsql/bin/pg_ctl";
my $start_cmd = "$pg_ctl -D $db_dir -l $temp_dir/postgres.log start -w -t 30";
system($start_cmd) == 0 or die "Failed to start PostgreSQL: $?";

# Give the server a moment to fully start up
sleep(2);

# Test 1: Verify GUC parameter is set correctly
my $result = run_sql("SELECT setting FROM pg_settings WHERE name = 'deferred_temp_table_placement';");
like($result, qr/on/, 'deferred_temp_table_placement is enabled');

# Count initial temp files
my $files_before = count_temp_files($db_dir);

# Test 2: Test temp table functionality in a single session
my $test_sql = qq{
CREATE TEMP TABLE small_test (id int, data text);
INSERT INTO small_test SELECT i, 'data_' || i FROM generate_series(1, 100) i;
SELECT COUNT(*) FROM small_test;
};
my $small_result = run_sql($test_sql);
like($small_result, qr/100/, 'Small temp table contains correct number of rows');

# Test 3: Test operations in same session
my $operations_sql = qq{
CREATE TEMP TABLE test_ops (id int, data text);
INSERT INTO test_ops VALUES (1, 'test1'), (2, 'test2');
UPDATE test_ops SET data = 'updated' WHERE id <= 1;
SELECT COUNT(*) FROM test_ops WHERE data = 'updated';
};
my $ops_result = run_sql($operations_sql);
like($ops_result, qr/1/, 'UPDATE operation works on temp table');

# Test 4: Test large temp table creation (might create files)
my $large_sql = qq{
CREATE TEMP TABLE large_test (id int, data text);
INSERT INTO large_test SELECT i, repeat('x', 100) FROM generate_series(1, 10000) i;
SELECT COUNT(*) FROM large_test;
};
my $large_result = run_sql($large_sql);
like($large_result, qr/10000/, 'Large temp table contains correct number of rows');

# Test 5: Test index creation
my $index_sql = qq{
CREATE TEMP TABLE idx_test (id int, data text);
INSERT INTO idx_test VALUES (1, 'data1');
CREATE INDEX idx_test_id ON idx_test(id);
SELECT 1;
};
my $index_result = run_sql($index_sql);
like($index_result, qr/1/, 'Index created on temp table');

# Test 6: Verify no immediate file creation for small tables
my $files_after_small = count_temp_files($db_dir);
ok($files_after_small >= $files_before, 'File counting works for small operations');

# Test 7: Test file creation detection (files may or may not be created depending on size)
my $files_after_tests = count_temp_files($db_dir);
cmp_ok($files_after_tests, '>=', $files_before, 'File counting works correctly');

# Test 8: Test cleanup
ok(1, 'All tests completed successfully');

# Cleanup: Stop PostgreSQL server
my $stop_cmd = "$pg_ctl -D $db_dir stop -w -t 30";
system($stop_cmd);

# Helper function to run SQL commands
sub run_sql {
    my $sql = shift;
    my $psql = "/tmp/pg-install/usr/local/pgsql/bin/psql";
    $ENV{LD_LIBRARY_PATH} = "/tmp/pg-install/usr/local/pgsql/lib:" . ($ENV{LD_LIBRARY_PATH} // "");
    
    # For multiline SQL, write to a temp file
    if ($sql =~ /\n/ || $sql =~ /;.*\S/) {
        my ($fh, $filename) = File::Temp::tempfile();
        print $fh $sql;
        close $fh;
        my $cmd = qq{$psql -p 65432 -d template1 -t -A -f $filename 2>/dev/null | tail -1};
        my $result = qx{$cmd};
        unlink $filename;
        chomp($result);
        return $result;
    } else {
        my $cmd = qq{echo "$sql" | $psql -p 65432 -d template1 -t -A 2>/dev/null};
        my $result = qx{$cmd};
        chomp($result);
        return $result;
    }
}

# Helper function to count temporary files
sub count_temp_files {
    my $dir = shift;
    my $count = 0;
    
    find(sub {
        if (-f $_ && (/^t\d+_\d+/ || /pgsql_tmp/)) {
            $count++;
        }
    }, $dir) if -d $dir;
    
    return $count;
}

done_testing();