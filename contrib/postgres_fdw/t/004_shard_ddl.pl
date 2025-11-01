# Test automatic DDL execution on shard members
# This test verifies that tables are automatically created on all shard members
# when using SHARD GROUP with postgres_fdw

use strict;
use warnings;

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More tests => 12;

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
# Setup: Install postgres_fdw and create cross-node foreign servers
###############################################################################

# Install postgres_fdw on all nodes
foreach my $node ($node1, $node2, $node3)
{
	$node->safe_psql('postgres', 'CREATE EXTENSION postgres_fdw;');
}

# Create foreign servers on each node pointing to the other nodes
# node1 -> node2, node3
my $node2_port = $node2->port;
my $node2_host = $node2->host;
my $node3_port = $node3->port;
my $node3_host = $node3->host;

$node1->safe_psql('postgres', qq[
	CREATE SERVER node2 FOREIGN DATA WRAPPER postgres_fdw 
		OPTIONS (host '$node2_host', dbname 'postgres', port '$node2_port');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node2;
	
	CREATE SERVER node3 FOREIGN DATA WRAPPER postgres_fdw 
		OPTIONS (host '$node3_host', dbname 'postgres', port '$node3_port');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node3;
]);

# node2 -> node1, node3
my $node1_port = $node1->port;
my $node1_host = $node1->host;

$node2->safe_psql('postgres', qq[
	CREATE SERVER node1 FOREIGN DATA WRAPPER postgres_fdw 
		OPTIONS (host '$node1_host', dbname 'postgres', port '$node1_port');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node1;
	
	CREATE SERVER node3 FOREIGN DATA WRAPPER postgres_fdw 
		OPTIONS (host '$node3_host', dbname 'postgres', port '$node3_port');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node3;
]);

# node3 -> node1, node2
$node3->safe_psql('postgres', qq[
	CREATE SERVER node1 FOREIGN DATA WRAPPER postgres_fdw 
		OPTIONS (host '$node1_host', dbname 'postgres', port '$node1_port');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node1;
	
	CREATE SERVER node2 FOREIGN DATA WRAPPER postgres_fdw 
		OPTIONS (host '$node2_host', dbname 'postgres', port '$node2_port');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node2;
]);

###############################################################################
# Test 1: Create shard group and add members
###############################################################################

$node1->safe_psql('postgres', qq[
	CREATE SHARD GROUP sg_test;
	ALTER SHARD GROUP sg_test ADD MEMBER node2;
	ALTER SHARD GROUP sg_test ADD MEMBER node3;
]);

# Verify shard group was created
my $result = $node1->safe_psql('postgres', 
	"SELECT sgname FROM pg_shardgroups WHERE sgname = 'sg_test';");
is($result, 'sg_test', 'Shard group created on node1');

###############################################################################
# Test 2: Create distributed table - should be created on all shard members
###############################################################################

$node1->safe_psql('postgres', qq[
	CREATE TABLE users (
		id integer PRIMARY KEY,
		username text NOT NULL,
		email text
	) SHARD GROUP sg_test;
]);

# Verify table exists on node1 (local)
$result = $node1->safe_psql('postgres',
	"SELECT count(*) FROM pg_tables WHERE tablename = 'users' AND schemaname = 'public';");
is($result, '1', 'Table users created on node1 (local)');

# Verify table was automatically created on node2 (remote)
$result = $node2->safe_psql('postgres',
	"SELECT count(*) FROM pg_tables WHERE tablename = 'users' AND schemaname = 'public';");
is($result, '1', 'Table users automatically created on node2 (remote)');

# Verify table was automatically created on node3 (remote)
$result = $node3->safe_psql('postgres',
	"SELECT count(*) FROM pg_tables WHERE tablename = 'users' AND schemaname = 'public';");
is($result, '1', 'Table users automatically created on node3 (remote)');

###############################################################################
# Test 3: Verify table structure is created on all nodes
###############################################################################

$node1->safe_psql('postgres', qq[
	INSERT INTO users VALUES (1, 'alice', 'alice\@example.com');
	INSERT INTO users VALUES (2, 'bob', 'bob\@example.com');
]);

$result = $node1->safe_psql('postgres', 
	"SELECT count(*) FROM users;");
is($result, '2', 'Data inserted on node1');

# Each node has its own copy of the table - data is not automatically replicated
# This is expected behavior - DDL replication creates the schema, not data replication
# Users would need to setup logical replication or partitioning for data distribution
$result = $node2->safe_psql('postgres',
	"SELECT count(*) FROM users;");
is($result, '0', 'Table exists on node2 (schema replicated, not data)');

###############################################################################
# Test 4: Create partitioned table with shard group
###############################################################################

$node1->safe_psql('postgres', qq[
	CREATE TABLE orders (
		order_id integer,
		customer_id integer NOT NULL,
		region text NOT NULL,
		amount numeric(10,2)
	) PARTITION BY LIST(region) SHARD GROUP sg_test;
]);

# Verify parent table exists on all nodes
$result = $node1->safe_psql('postgres',
	"SELECT count(*) FROM pg_tables WHERE tablename = 'orders' AND schemaname = 'public';");
is($result, '1', 'Partitioned table orders created on node1');

$result = $node2->safe_psql('postgres',
	"SELECT count(*) FROM pg_tables WHERE tablename = 'orders' AND schemaname = 'public';");
is($result, '1', 'Partitioned table orders created on node2');

###############################################################################
# Test 5: Create partition - should be foreign table on remote nodes
###############################################################################

$node1->safe_psql('postgres', qq[
	CREATE TABLE orders_us PARTITION OF orders FOR VALUES IN ('US', 'CANADA');
]);

# Verify partition exists as regular table on node1
$result = $node1->safe_psql('postgres',
	"SELECT count(*) FROM pg_tables WHERE tablename = 'orders_us' AND schemaname = 'public';");
is($result, '1', 'Partition orders_us created as regular table on node1');

# Verify partition exists as foreign table on node2
$result = $node2->safe_psql('postgres', qq[
	SELECT count(*) 
	FROM pg_foreign_table ft
	JOIN pg_class c ON ft.ftrelid = c.oid
	WHERE c.relname = 'orders_us';
]);
is($result, '1', 'Partition orders_us created as foreign table on node2');

# Verify the foreign table points to node1
$result = $node2->safe_psql('postgres', qq[
	SELECT fs.srvname
	FROM pg_foreign_table ft
	JOIN pg_class c ON ft.ftrelid = c.oid
	JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
	WHERE c.relname = 'orders_us';
]);
is($result, 'node1', 'Foreign table orders_us on node2 points to node1');

###############################################################################
# Test 6: Test data flow through partition foreign tables
###############################################################################

$node1->safe_psql('postgres', qq[
	INSERT INTO orders_us VALUES (1, 100, 'US', 99.99);
	INSERT INTO orders_us VALUES (2, 101, 'CANADA', 149.99);
]);

# Verify data is accessible from node2 via foreign table
$result = $node2->safe_psql('postgres',
	"SELECT count(*) FROM orders_us;");
is($result, '2', 'Data in partition accessible from node2 via foreign table');

###############################################################################
# Cleanup
###############################################################################

$node1->stop;
$node2->stop;
$node3->stop;
