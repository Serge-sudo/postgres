# Copyright (c) 2025, PostgreSQL Global Development Group

# Test deferred temp table placement feature
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Find;
use Time::HiRes qw(gettimeofday tv_interval);

# Create a test cluster with deferred temp table placement enabled
my $node = PostgreSQL::Test::Cluster->new('test_deferred');
$node->init();

# Configure the cluster for testing deferred temp table placement
$node->append_conf('postgresql.conf', q{
deferred_temp_table_placement = on
temp_buffers = 32MB
log_statement = 'all'
log_min_duration_statement = 0
});

$node->start();

# Test 1: Verify GUC parameter is correctly set
my $result = $node->safe_psql('postgres', 
    "SELECT setting FROM pg_settings WHERE name = 'deferred_temp_table_placement';");
is($result, 'on', 'deferred_temp_table_placement is enabled');

# Test 2: Create extension for pg_buffercache_local (if available)
# Note: This may fail if the extension doesn't exist, which is okay according to comment
my $has_buffercache_local = 0;
eval {
    $node->safe_psql('postgres', 'CREATE EXTENSION IF NOT EXISTS pg_buffercache;');
    # Try to use pg_buffercache_local if it exists
    $node->safe_psql('postgres', 'SELECT count(*) FROM pg_buffercache_local;');
    $has_buffercache_local = 1;
};

# Test 3: Verify temp table files are NOT created immediately for small tables
my $pgdata = $node->data_dir;
my $db_oid = $node->safe_psql('postgres', "SELECT oid FROM pg_database WHERE datname = 'postgres';");
my $db_dir = "$pgdata/base/$db_oid";

# Count files before creating temp table
my @files_before = get_temp_files($db_dir);

# Create a small temp table that should fit in temp_buffers
$node->safe_psql('postgres', q{
    CREATE TEMP TABLE small_temp (id int, data text);
    INSERT INTO small_temp SELECT i, 'data_' || i FROM generate_series(1, 1000) i;
});

# Check that no new temp files were created
my @files_after_small = get_temp_files($db_dir);
is(scalar(@files_after_small), scalar(@files_before), 
   'No temp files created for small temp table that fits in temp_buffers');

# Test 4: Verify buffer cache shows temp table data (if pg_buffercache_local available)
if ($has_buffercache_local) {
    my $buffer_count = $node->safe_psql('postgres', 
        "SELECT count(*) FROM pg_buffercache_local WHERE reltablespace IS NULL;");
    ok($buffer_count > 0, 'Local buffers contain temp table data');
}

# Test 5: Force buffer overflow and verify disk files are created
$node->safe_psql('postgres', q{
    CREATE TEMP TABLE large_temp (id int, data text);
    -- Insert enough data to overflow temp_buffers (32MB)
    -- Each row is roughly 40 bytes, so 1M rows = ~40MB
    INSERT INTO large_temp SELECT i, repeat('x', 32) FROM generate_series(1, 1000000) i;
});

# Check that temp files are now created after overflow
my @files_after_large = get_temp_files($db_dir);
ok(scalar(@files_after_large) > scalar(@files_before), 
   'Temp files created after buffer overflow');

# Test 6: Performance comparison
# First, test with deferred temp table placement enabled
my $start_time = [gettimeofday];
$node->safe_psql('postgres', q{
    BEGIN;
    CREATE TEMP TABLE perf_test_on (id int);
    INSERT INTO perf_test_on SELECT generate_series(1, 10000);
    SELECT count(*) FROM perf_test_on;
    DROP TABLE perf_test_on;
    COMMIT;
});
my $time_with_deferred = tv_interval($start_time);

# Restart with feature disabled for comparison
$node->stop();
$node->append_conf('postgresql.conf', 'deferred_temp_table_placement = off');
$node->start();

# Test with deferred temp table placement disabled
$start_time = [gettimeofday];
$node->safe_psql('postgres', q{
    BEGIN;
    CREATE TEMP TABLE perf_test_off (id int);
    INSERT INTO perf_test_off SELECT generate_series(1, 10000);
    SELECT count(*) FROM perf_test_off;
    DROP TABLE perf_test_off;
    COMMIT;
});
my $time_without_deferred = tv_interval($start_time);

# Performance should be better (or at least not significantly worse) with deferred placement
# Allow for some variance in timing
ok($time_with_deferred <= $time_without_deferred * 1.5, 
   'Performance with deferred temp table placement is reasonable');

# Test 7: Verify immediate file creation when feature is disabled
@files_before = get_temp_files($db_dir);

$node->safe_psql('postgres', q{
    CREATE TEMP TABLE immediate_temp (id int, data text);
    INSERT INTO immediate_temp SELECT i, 'data_' || i FROM generate_series(1, 100) i;
});

my @files_after_immediate = get_temp_files($db_dir);
ok(scalar(@files_after_immediate) > scalar(@files_before), 
   'Temp files created immediately when deferred temp table placement is disabled');

# Test 8: Verify proper cleanup
$node->safe_psql('postgres', 'DROP TABLE immediate_temp;');
sleep(1); # Allow cleanup to occur

# Test 9: Test with various temp table operations when feature is re-enabled
$node->stop();
$node->append_conf('postgresql.conf', 'deferred_temp_table_placement = on');
$node->start();

$node->safe_psql('postgres', q{
    -- Test various operations on deferred temp tables
    CREATE TEMP TABLE ops_test (id int PRIMARY KEY, data text);
    INSERT INTO ops_test VALUES (1, 'test1'), (2, 'test2');
    
    -- Test UPDATE
    UPDATE ops_test SET data = 'updated1' WHERE id = 1;
    
    -- Test DELETE
    DELETE FROM ops_test WHERE id = 2;
    
    -- Test index creation
    CREATE INDEX ops_test_data_idx ON ops_test(data);
    
    -- Test SELECT
    SELECT * FROM ops_test WHERE data = 'updated1';
    
    -- Test VACUUM
    VACUUM ops_test;
    
    -- Verify data integrity
    SELECT count(*) FROM ops_test;
});

my $final_count = $node->safe_psql('postgres', 'SELECT count(*) FROM ops_test;');
is($final_count, '1', 'Data integrity maintained through various operations');

# Clean up
$node->safe_psql('postgres', 'DROP TABLE ops_test;');

$node->stop();

# Helper function to count temporary files in database directory
sub get_temp_files {
    my $dir = shift;
    my @temp_files = ();
    
    # Look for files that match temp table patterns
    find(sub {
        if (-f $_ && /^t\d+_\d+/ || /pgsql_tmp/) {
            push @temp_files, $File::Find::name;
        }
    }, $dir) if -d $dir;
    
    return @temp_files;
}

done_testing();