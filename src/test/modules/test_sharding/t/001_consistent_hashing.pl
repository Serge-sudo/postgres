#!/usr/bin/perl

# Copyright (c) 2024, PostgreSQL Global Development Group

# Test consistent hashing, resharding, and detach functionality
# for distributed shard groups

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Create three PostgreSQL nodes to act as shard members
my $node1 = PostgreSQL::Test::Cluster->new('node1');
$node1->init;
$node1->append_conf('postgresql.conf', qq(
cluster_name = 'node1'
));
$node1->start;

my $node2 = PostgreSQL::Test::Cluster->new('node2');
$node2->init;
$node2->append_conf('postgresql.conf', qq(
cluster_name = 'node2'
));
$node2->start;

my $node3 = PostgreSQL::Test::Cluster->new('node3');
$node3->init;
$node3->append_conf('postgresql.conf', qq(
cluster_name = 'node3'
));
$node3->start;

my $node1_port = $node1->port;
my $node1_host = $node1->host;
my $node2_port = $node2->port;
my $node2_host = $node2->host;
my $node3_port = $node3->port;
my $node3_host = $node3->host;

# Setup foreign data wrappers and servers on each node

# Install postgres_fdw on all nodes
foreach my $node ($node1, $node2, $node3)
{
	$node->safe_psql('postgres', 'CREATE EXTENSION postgres_fdw;');
}

# Setup foreign servers on node1
$node1->safe_psql('postgres', qq[
	CREATE SERVER node2 FOREIGN DATA WRAPPER postgres_fdw 
		OPTIONS (host '$node2_host', dbname 'postgres', port '$node2_port');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node2;
	
	CREATE SERVER node3 FOREIGN DATA WRAPPER postgres_fdw 
		OPTIONS (host '$node3_host', dbname 'postgres', port '$node3_port');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node3;

]);

# Setup foreign servers on node2
$node2->safe_psql('postgres', qq[
	CREATE SERVER node1 FOREIGN DATA WRAPPER postgres_fdw 
		OPTIONS (host '$node1_host', dbname 'postgres', port '$node1_port');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node1;
	
	CREATE SERVER node3 FOREIGN DATA WRAPPER postgres_fdw 
		OPTIONS (host '$node3_host', dbname 'postgres', port '$node3_port');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node3;

]);

# Setup foreign servers on node3
$node3->safe_psql('postgres', qq[
	CREATE SERVER node1 FOREIGN DATA WRAPPER postgres_fdw 
		OPTIONS (host '$node1_host', dbname 'postgres', port '$node1_port');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node1;
	
	CREATE SERVER node2 FOREIGN DATA WRAPPER postgres_fdw 
		OPTIONS (host '$node2_host', dbname 'postgres', port '$node2_port');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node2;

]);

# Create shard group on node1
$node1->safe_psql('postgres', qq(
    CREATE SHARD GROUP sg1;
    ALTER SHARD GROUP sg1 ADD MEMBER node2;
));

# Wait for synchronization
sleep(1);

# Verify shard group members are visible on all nodes
my $result = $node1->safe_psql('postgres', 
    "SELECT COUNT(*) FROM pg_shardmembers WHERE sgid = (SELECT oid FROM pg_shardgroups WHERE sgname = 'sg1');");
is($result, '1', 'node1 sees 1 shard members (excluding itself)');

$result = $node2->safe_psql('postgres', 
    "SELECT COUNT(*) FROM pg_shardmembers WHERE sgid = (SELECT oid FROM pg_shardgroups WHERE sgname = 'sg1');");
is($result, '1', 'node2 sees 1 shard members (excluding itself)');

# Create a distributed partitioned table on node1
$node1->safe_psql('postgres', qq(
    CREATE TABLE orders (
        order_id INT,
        customer_id INT,
        order_date DATE,
        amount DECIMAL(10,2)
    ) DISTRIBUTED BY RANGE (order_date) SHARD GROUP sg1;
));

# Create partitions - they should be distributed via consistent hashing
$node1->safe_psql('postgres', qq(
    CREATE TABLE orders_2024_01 PARTITION OF orders 
        FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
    CREATE TABLE orders_2024_02 PARTITION OF orders 
        FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
    CREATE TABLE orders_2024_03 PARTITION OF orders 
        FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
    CREATE TABLE orders_2024_04 PARTITION OF orders 
        FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
));

# Check that partitions are distributed (some should be real tables, some foreign)
my $node1_real_tables = $node1->safe_psql('postgres', qq(
    SELECT COUNT(*) FROM pg_class 
    WHERE relname LIKE 'orders_2024_%' AND relkind = 'r' AND relispartition;
));

my $node1_foreign_tables = $node1->safe_psql('postgres', qq(
    SELECT COUNT(*) FROM pg_class 
    WHERE relname LIKE 'orders_2024_%' AND relkind = 'f' AND relispartition;
));

note("Node1 has $node1_real_tables real partition tables and $node1_foreign_tables foreign partition tables");

# With 2 nodes and 4 partitions, we expect distribution via consistent hashing
# Each node should have some real tables and some foreign tables
ok($node1_real_tables > 0, 'node1 has at least one real partition table');
ok($node1_foreign_tables > 0, 'node1 has at least one foreign partition table');
is($node1_real_tables + $node1_foreign_tables, 4, 'node1 has all 4 partitions (real + foreign)');

# Check node2 as well
my $node2_real_tables = $node2->safe_psql('postgres', qq(
    SELECT COUNT(*) FROM pg_class 
    WHERE relname LIKE 'orders_2024_%' AND relkind = 'r' AND relispartition;
));

my $node2_foreign_tables = $node2->safe_psql('postgres', qq(
    SELECT COUNT(*) FROM pg_class 
    WHERE relname LIKE 'orders_2024_%' AND relkind = 'f' AND relispartition;
));

note("Node2 has $node2_real_tables real partition tables and $node2_foreign_tables foreign partition tables");

ok($node2_real_tables > 0, 'node2 has at least one real partition table');
ok($node2_foreign_tables > 0, 'node2 has at least one foreign partition table');
is($node2_real_tables + $node2_foreign_tables, 4, 'node2 has all 4 partitions (real + foreign)');

# Verify that real tables on node1 + real tables on node2 = total partitions
is($node1_real_tables + $node2_real_tables, 4, 'total real tables across nodes equals partition count');

# Insert some test data
$node1->safe_psql('postgres', qq(
    INSERT INTO orders VALUES (1, 100, '2024-01-15', 150.00);
    INSERT INTO orders VALUES (2, 101, '2024-02-15', 200.00);
    INSERT INTO orders VALUES (3, 102, '2024-03-15', 175.00);
    INSERT INTO orders VALUES (4, 103, '2024-04-15', 225.00);
));

# Verify data is accessible from both nodes
my $node1_count = $node1->safe_psql('postgres', "SELECT COUNT(*) FROM orders;");
is($node1_count, '4', 'node1 can see all 4 rows');

my $node2_count = $node2->safe_psql('postgres', "SELECT COUNT(*) FROM orders;");
is($node2_count, '4', 'node2 can see all 4 rows');

# Test RESHARD command - add a third node and reshard
$node1->safe_psql('postgres', qq(
    ALTER SHARD GROUP sg1 ADD MEMBER node3;
));

# Wait for synchronization
sleep(1);

# Verify node3 is in the shard group
$result = $node1->safe_psql('postgres', 
    "SELECT COUNT(*) FROM pg_shardmembers WHERE sgid = (SELECT oid FROM pg_shardgroups WHERE sgname = 'sg1');");
is($result, '3', 'shard group now has 3 members');

# At this point, node3 should have foreign tables only
my $node3_real_before_reshard = $node3->safe_psql('postgres', qq(
    SELECT COUNT(*) FROM pg_class 
    WHERE relname LIKE 'orders_2024_%' AND relkind = 'r' AND relispartition;
));

my $node3_foreign_before_reshard = $node3->safe_psql('postgres', qq(
    SELECT COUNT(*) FROM pg_class 
    WHERE relname LIKE 'orders_2024_%' AND relkind = 'f' AND relispartition;
));

note("Node3 before reshard: $node3_real_before_reshard real, $node3_foreign_before_reshard foreign");

# Node3 should have only foreign tables before reshard
is($node3_real_before_reshard, 0, 'node3 has no real tables before reshard');
is($node3_foreign_before_reshard, 4, 'node3 has all foreign tables before reshard');

# Execute RESHARD command
my ($ret, $stdout, $stderr) = $node1->psql('postgres', 
    "ALTER SHARD GROUP sg1 RESHARD;");
is($ret, 0, 'RESHARD command executed successfully');
note("RESHARD output: $stdout");

# After reshard, some partitions should have moved to node3
my $node3_real_after_reshard = $node3->safe_psql('postgres', qq(
    SELECT COUNT(*) FROM pg_class 
    WHERE relname LIKE 'orders_2024_%' AND relkind = 'r' AND relispartition;
));

my $node3_foreign_after_reshard = $node3->safe_psql('postgres', qq(
    SELECT COUNT(*) FROM pg_class 
    WHERE relname LIKE 'orders_2024_%' AND relkind = 'f' AND relispartition;
));

note("Node3 after reshard: $node3_real_after_reshard real, $node3_foreign_after_reshard foreign");

# After reshard, node3 should have some real tables
ok($node3_real_after_reshard > 0, 'node3 has real tables after reshard');
is($node3_real_after_reshard + $node3_foreign_after_reshard, 4, 'node3 has all 4 partitions after reshard');

# Verify data is still accessible from all nodes
$node1_count = $node1->safe_psql('postgres', "SELECT COUNT(*) FROM orders;");
is($node1_count, '4', 'node1 can still see all 4 rows after reshard');

$node2_count = $node2->safe_psql('postgres', "SELECT COUNT(*) FROM orders;");
is($node2_count, '4', 'node2 can still see all 4 rows after reshard');

my $node3_count = $node3->safe_psql('postgres', "SELECT COUNT(*) FROM orders;");
is($node3_count, '4', 'node3 can see all 4 rows after reshard');

# Test DETACH command - detach node2
($ret, $stdout, $stderr) = $node1->psql('postgres', 
    "ALTER SHARD GROUP sg1 DETACH node2;");
is($ret, 0, 'DETACH command executed successfully');
note("DETACH output: $stdout");

# After detach, node2 should have only foreign tables
my $node2_real_after_detach = $node2->safe_psql('postgres', qq(
    SELECT COUNT(*) FROM pg_class 
    WHERE relname LIKE 'orders_2024_%' AND relkind = 'r' AND relispartition;
));

my $node2_foreign_after_detach = $node2->safe_psql('postgres', qq(
    SELECT COUNT(*) FROM pg_class 
    WHERE relname LIKE 'orders_2024_%' AND relkind = 'f' AND relispartition;
));

note("Node2 after detach: $node2_real_after_detach real, $node2_foreign_after_detach foreign");

is($node2_real_after_detach, 0, 'node2 has no real tables after detach');
is($node2_foreign_after_detach, 4, 'node2 has all foreign tables after detach');

# Verify data is still accessible from all nodes
$node1_count = $node1->safe_psql('postgres', "SELECT COUNT(*) FROM orders;");
is($node1_count, '4', 'node1 can still see all 4 rows after detach');

$node2_count = $node2->safe_psql('postgres', "SELECT COUNT(*) FROM orders;");
is($node2_count, '4', 'node2 can still see all 4 rows after detach');

$node3_count = $node3->safe_psql('postgres', "SELECT COUNT(*) FROM orders;");
is($node3_count, '4', 'node3 can still see all 4 rows after detach');

# Now node2 can be safely removed from the shard group
$node1->safe_psql('postgres', qq(
    ALTER SHARD GROUP sg1 DROP MEMBER node2;
));

# Verify node2 is removed
$result = $node1->safe_psql('postgres', 
    "SELECT COUNT(*) FROM pg_shardmembers WHERE sgid = (SELECT oid FROM pg_shardgroups WHERE sgname = 'sg1');");
is($result, '2', 'shard group now has 2 members after dropping node2');

# Cleanup
$node1->stop;
$node2->stop;
$node3->stop;

done_testing();
