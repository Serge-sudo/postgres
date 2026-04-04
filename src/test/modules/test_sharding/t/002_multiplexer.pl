#!/usr/bin/perl

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node1 = PostgreSQL::Test::Cluster->new('node1');
$node1->init;
$node1->append_conf('postgresql.conf', qq(
cluster_name = 'node1'
mux_worker_count = 2
mux_tcp_port = 6432
shared_preload_libraries = 'postgres_fdw'
));
$node1->start;

my $node2 = PostgreSQL::Test::Cluster->new('node2');
$node2->init;
$node2->append_conf('postgresql.conf', qq(
cluster_name = 'node2'
mux_worker_count = 2
mux_tcp_port = 6433
shared_preload_libraries = 'postgres_fdw'
));
$node2->start;

my $node3 = PostgreSQL::Test::Cluster->new('node3');
$node3->init;
$node3->append_conf('postgresql.conf', qq(
cluster_name = 'node3'
mux_worker_count = 2
mux_tcp_port = 6434
shared_preload_libraries = 'postgres_fdw'
));
$node3->start;

my $node1_port = $node1->port;
my $node2_port = $node2->port;
my $node3_port = $node3->port;
my $host = '127.0.0.1';

foreach my $node ($node1, $node2, $node3)
{
	$node->safe_psql('postgres', 'CREATE EXTENSION postgres_fdw;');
}

$node1->safe_psql('postgres', qq[
	CREATE SERVER node2 FOREIGN DATA WRAPPER postgres_fdw
	  OPTIONS (host '$host', dbname 'postgres', port '$node2_port', mux_port '6433');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node2;

	CREATE SERVER node3 FOREIGN DATA WRAPPER postgres_fdw
	  OPTIONS (host '$host', dbname 'postgres', port '$node3_port', mux_port '6434');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node3;
]);

$node2->safe_psql('postgres', qq[
	CREATE SERVER node1 FOREIGN DATA WRAPPER postgres_fdw
	  OPTIONS (host '$host', dbname 'postgres', port '$node1_port', mux_port '6432');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node1;

	CREATE SERVER node3 FOREIGN DATA WRAPPER postgres_fdw
	  OPTIONS (host '$host', dbname 'postgres', port '$node3_port', mux_port '6434');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node3;
]);

$node3->safe_psql('postgres', qq[
	CREATE SERVER node1 FOREIGN DATA WRAPPER postgres_fdw
	  OPTIONS (host '$host', dbname 'postgres', port '$node1_port', mux_port '6432');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node1;

	CREATE SERVER node2 FOREIGN DATA WRAPPER postgres_fdw
	  OPTIONS (host '$host', dbname 'postgres', port '$node2_port', mux_port '6433');
	CREATE USER MAPPING FOR CURRENT_USER SERVER node2;
]);

$node1->safe_psql('postgres', qq[
	CREATE SHARD GROUP group1;
	ALTER SHARD GROUP group1 ADD MEMBER node2;
	ALTER SHARD GROUP group1 ADD MEMBER node3;

	CREATE TABLE t1 (a int, src text)
	  DISTRIBUTED BY RANGE (a) SHARD GROUP group1;
	CREATE TABLE t1_1 PARTITION OF t1 FOR VALUES FROM (0) TO (100);
	CREATE TABLE t1_2 PARTITION OF t1 FOR VALUES FROM (100) TO (200);
	CREATE TABLE t1_3 PARTITION OF t1 FOR VALUES FROM (200) TO (300);
]);

$node1->safe_psql('postgres',
	"INSERT INTO t1 VALUES (1, 'n1'), (101, 'n1');");
$node2->safe_psql('postgres',
	"INSERT INTO t1 VALUES (2, 'n2'), (102, 'n2');");
$node3->safe_psql('postgres',
	"INSERT INTO t1 VALUES (3, 'n3'), (103, 'n3');");

is($node1->safe_psql('postgres', "SELECT count(*) FROM t1;"),
	'6', 'node1 reads all rows');
is($node2->safe_psql('postgres', "SELECT count(*) FROM t1;"),
	'6', 'node2 reads all rows');
is($node3->safe_psql('postgres', "SELECT count(*) FROM t1;"),
	'6', 'node3 reads all rows');

$node1->safe_psql('postgres',
	"UPDATE t1 SET src = 'u1' WHERE a IN (2, 102);");
$node2->safe_psql('postgres',
	"UPDATE t1 SET src = 'u2' WHERE a IN (3, 103);");
$node3->safe_psql('postgres',
	"UPDATE t1 SET src = 'u3' WHERE a IN (1, 101);");

is($node1->safe_psql('postgres',
	"SELECT count(*) FROM t1 WHERE src = 'u1';"), '2',
	'updates from node1 are visible');
is($node2->safe_psql('postgres',
	"SELECT count(*) FROM t1 WHERE src = 'u2';"), '2',
	'updates from node2 are visible');
is($node3->safe_psql('postgres',
	"SELECT count(*) FROM t1 WHERE src = 'u3';"), '2',
	'updates from node3 are visible');

$node1->safe_psql('postgres', "DELETE FROM t1 WHERE a = 2;");
$node2->safe_psql('postgres', "DELETE FROM t1 WHERE a = 101;");
$node3->safe_psql('postgres', "DELETE FROM t1 WHERE a = 103;");

is($node1->safe_psql('postgres', "SELECT count(*) FROM t1;"),
	'3', 'node1 sees deletes from all nodes');
is($node2->safe_psql('postgres', "SELECT count(*) FROM t1;"),
	'3', 'node2 sees deletes from all nodes');
is($node3->safe_psql('postgres', "SELECT count(*) FROM t1;"),
	'3', 'node3 sees deletes from all nodes');

is($node1->safe_psql('postgres',
	"SELECT string_agg(a::text, ',' ORDER BY a) FROM t1;"),
	'1,3,102', 'remaining rows are consistent after writes from every node');

$node1->stop;
$node2->stop;
$node3->stop;

done_testing();
