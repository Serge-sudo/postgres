# Test automatic foreign table synchronization when adding shard members
# This test verifies that when a foreign table with shard group is created,
# and then new shard members are added, the foreign table is replicated
# to the new members pointing to the same server

use strict;
use warnings;

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More tests => 10;

# Create three nodes to simulate a sharded cluster
my $node1 = PostgreSQL::Test::Cluster->new("node1");
$node1->init;
$node1->append_conf('postgresql.conf', qq(
	max_prepared_transactions = 10
	cluster_name = 'node1'
));
$node1->start;

my $node2 = PostgreSQL::Test::Cluster->new("node2");
$node2->init;
$node2->append_conf('postgresql.conf', qq(
	max_prepared_transactions = 10
	cluster_name = 'node2'
));
$node2->start;

my $node3 = PostgreSQL::Test::Cluster->new("node3");
$node3->init;
$node3->append_conf('postgresql.conf', qq(
	max_prepared_transactions = 10
	cluster_name = 'node3'
));
$node3->start;

###############################################################################
# Setup: Install postgres_fdw and create foreign servers
###############################################################################

# Install postgres_fdw on all nodes
foreach my $node ($node1, $node2, $node3)
{
	$node->safe_psql('postgres', 'CREATE EXTENSION postgres_fdw;');
}

# Create cross-node foreign servers for shard coordination
my $node2_port = $node2->port;
my $node2_host = $node2->host;
my $node3_port = $node3->port;
my $node3_host = $node3->host;
my $node1_port = $node1->port;
my $node1_host = $node1->host;

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

###############################################################################
# Test 1: Create shard group without members initially
###############################################################################

$node1->safe_psql('postgres', qq[
	CREATE SHARD GROUP sg_foreign_test;
]);

# Verify shard group was created
my $result = $node1->safe_psql('postgres', 
	"SELECT COUNT(*) FROM pg_shardgroups WHERE sgname = 'sg_foreign_test'");
is($result, '1', 'Shard group sg_foreign_test created on node1');

###############################################################################
# Test 2: Create foreign table with shard group on node1
###############################################################################

$node1->safe_psql('postgres', qq[
	CREATE WORLDWIDE TABLE customers (
		customer_id INT,
		customer_name TEXT,
		email TEXT
	) SHARD GROUP sg_foreign_test;
	
	INSERT INTO customers VALUES 
		(1, 'Alice', 'alice\@example.com'),
		(2, 'Bob', 'bob\@example.com'),
		(3, 'Charlie', 'charlie\@example.com');
]);

# Verify foreign table created on node1
$result = $node1->safe_psql('postgres', 
	"SELECT COUNT(*) FROM pg_class WHERE relname = 'customers' AND relkind = 'r'");
is($result, '1', 'Foreign table customers created on node1');

# Verify we can query it
$result = $node1->safe_psql('postgres', 
	"SELECT COUNT(*) FROM customers");
is($result, '3', 'Can query foreign table customers from node1');

###############################################################################
# Test 3: Add node2 as shard member - should sync foreign table
###############################################################################

$node1->safe_psql('postgres', qq[
	ALTER SHARD GROUP sg_foreign_test ADD MEMBER node2;
]);

# Verify foreign table was created on node2
$result = $node2->safe_psql('postgres', 
	"SELECT COUNT(*) FROM pg_class WHERE relname = 'customers' AND relkind = 'f'");
is($result, '1', 'Foreign table customers synced to node2');

# Verify it points to the correct server (node1)
$result = $node2->safe_psql('postgres', qq[
	SELECT fs.srvname 
	FROM pg_foreign_table ft
	JOIN pg_class c ON ft.ftrelid = c.oid
	JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
	WHERE c.relname = 'customers'
]);
is($result, 'node1', 'Foreign table on node2 points to node1 server');

# Verify we can query it from node2
$result = $node2->safe_psql('postgres', 
	"SELECT COUNT(*) FROM customers");
is($result, '3', 'Can query foreign table customers from node2');

###############################################################################
# Test 4: Add node3 as another shard member - should also sync foreign table
###############################################################################

$node1->safe_psql('postgres', qq[
	ALTER SHARD GROUP sg_foreign_test ADD MEMBER node3;
]);

# Verify foreign table was created on node3
$result = $node3->safe_psql('postgres', 
	"SELECT COUNT(*) FROM pg_class WHERE relname = 'customers' AND relkind = 'f'");
is($result, '1', 'Foreign table customers synced to node3');

# Verify it points to the correct server (node1)
$result = $node3->safe_psql('postgres', qq[
	SELECT fs.srvname 
	FROM pg_foreign_table ft
	JOIN pg_class c ON ft.ftrelid = c.oid
	JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
	WHERE c.relname = 'customers'
]);
is($result, 'node1', 'Foreign table on node3 points to node1 server');

# Verify we can query it from node3
$result = $node3->safe_psql('postgres', 
	"SELECT COUNT(*) FROM customers");
is($result, '3', 'Can query foreign table customers from node3');

# Verify data content
$result = $node3->safe_psql('postgres', 
	"SELECT customer_name FROM customers WHERE customer_id = 1");
is($result, 'Alice', 'Can read correct data from foreign table on node3');



###############################################################################
# Cleanup
###############################################################################

$node1->stop;
$node2->stop;
$node3->stop;
