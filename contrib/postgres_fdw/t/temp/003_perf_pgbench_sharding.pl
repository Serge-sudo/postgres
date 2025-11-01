use strict;
use warnings;

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More tests => 16;  # 16 cases we will run

###############################################################################
# Helpers
###############################################################################

sub setup_clusters {
    my ($shard_count, $scale, $partition_count) = @_;

    my $rows     = $scale * 100000;  # pgbench_accounts rows
    my $node_cnt = $shard_count + 1; # master + shards

    # Master
    my $master = PostgreSQL::Test::Cluster->new("master_${shard_count}_${partition_count}");
    $master->init;
    $master->append_conf('postgresql.conf', qq(
        max_prepared_transactions = 30
        enable_csn_snapshot = on
        postgres_fdw.use_csn_snapshots = on
    ));
    $master->start;

    # Shards
    my @shards;
    for my $i (0..$shard_count-1) {
        my $shard = PostgreSQL::Test::Cluster->new("shard${shard_count}_${partition_count}_$i");
        $shard->init;
        $shard->append_conf('postgresql.conf', qq(
            max_prepared_transactions = 30
            enable_csn_snapshot = on
        ));
        $shard->start;
        push @shards, $shard;
    }

    # Schema on master
    $master->safe_psql('postgres', qq[
        CREATE EXTENSION postgres_fdw;
        CREATE TABLE pgbench_accounts (
            aid int,
            bid int,
            abalance int,
            filler char(84)
        ) PARTITION BY HASH (aid);

        CREATE TABLE pgbench_branches(
            bid int PRIMARY KEY,
            bbalance int,
            filler char(88)
        );
        CREATE TABLE pgbench_tellers(
            tid int PRIMARY KEY,
            bid int,
            tbalance int,
            filler char(84)
        );
        CREATE TABLE pgbench_history(
            tid int,
            bid int,
            aid int,
            delta int,
            mtime timestamp,
            filler char(22)
        );
    ]);

    # Create one FDW server + mapping per shard
    my %servers;
    for my $i (0..$shard_count-1) {
        my $shard = $shards[$i];
        my $port  = $shard->port;
        my $host  = $shard->host;
        my $srv   = "shard_${port}";
        $servers{$i} = $srv;

        $master->safe_psql('postgres', qq[
            CREATE SERVER $srv FOREIGN DATA WRAPPER postgres_fdw
                OPTIONS (dbname 'postgres', host '$host', port '$port');
            CREATE USER MAPPING FOR CURRENT_USER SERVER $srv;
        ]);
    }

    # Create partitions and distribute them round-robin across master + shards
    for my $p (0..$partition_count-1) {
        my $target_idx = ($shard_count == 0) ? $shard_count : $p % ($shard_count + 1);

        if ($target_idx == $shard_count) {
            # Partition on master
            $master->safe_psql('postgres', qq[
                CREATE TABLE pgbench_accounts_part_$p (
                    aid int,
                    bid int,
                    abalance int,
                    filler char(84)
                );
                ALTER TABLE pgbench_accounts
                ATTACH PARTITION pgbench_accounts_part_$p
                FOR VALUES WITH (modulus $partition_count, remainder $p);
                CREATE INDEX ON pgbench_accounts_part_$p (aid);
            ]);

        } else {
            # Partition on shard
            my $srv   = $servers{$target_idx};
            my $shard = $shards[$target_idx];
            $shard->safe_psql('postgres', qq[
                CREATE TABLE pgbench_accounts_part_$p (
                    aid int,
                    bid int,
                    abalance int,
                    filler char(84)
                );
                CREATE INDEX ON pgbench_accounts_part_$p (aid);
            ]);

            $master->safe_psql('postgres', qq[
                CREATE FOREIGN TABLE pgbench_accounts_fdw_${srv}_$p
                    PARTITION OF pgbench_accounts
                    FOR VALUES WITH (modulus $partition_count, remainder $p)
                    SERVER $srv
                    OPTIONS (table_name 'pgbench_accounts_part_$p');
            ]);
        }
    }

    $master->safe_psql('postgres', qq[
        CREATE INDEX ON pgbench_accounts (aid);
    ]);

    # Initialize pgbench data
    my $port = $master->port;
    my $cmd;
    if ($shard_count == 0) {
        $cmd = "pgbench -i -Ig -s $scale -p $port postgres";
    } else {
        $cmd = "pgbench -i -Ig --partition-method=hash -s $scale -p $port --partitions=$partition_count postgres";
    }
    my $out = `$cmd 2>&1`;


    return ($master, \@shards);
}

sub run_pgbench_test {
    my ($node, $seconds) = @_;

    my $port = $node->port;

    # Capture output for TPS extraction
    # Create a SQL file for the custom pgbench script
    my $sql_file = "pgbench_sum_aid.sql";
    open my $fh, '>', $sql_file or die "Could not create $sql_file: $!";
    print $fh "SELECT MAX(aid) FROM pgbench_accounts;\n";
    close $fh;

    my $cmd = "pgbench -c 10 -j 10 -T $seconds -n -p $port -f $sql_file";
    my $out = `$cmd 2>&1`;

    my ($tps) = $out =~ /^tps\s*=\s*([\d\.]+)/m;
    return $tps;
}

###############################################################################
# Parameters
###############################################################################

my $scale   = 10;     # ~100k rows in accounts
my $seconds = 30;

###############################################################################
# Test cases
###############################################################################

foreach my $case (
    # [0, $scale, 16],
    # [0, $scale, 32],
    # [0, $scale, 64],
    # [0, $scale, 128],
    # [1, $scale, 16],
    # [1, $scale, 32],
    # [1, $scale, 64],
    # [1, $scale, 128],
    # [3, $scale, 16],
    # [3, $scale, 32],
    # [3, $scale, 64],
    [3, $scale, 128],
    # [7, $scale, 16],
    # [7, $scale, 32],
    # [7, $scale, 64],
    [7, $scale, 128],
) {
    my ($shards, $sc, $parts) = @$case;
    my ($master, $shlist) = setup_clusters($shards, $sc, $parts);
    my $tps = run_pgbench_test($master, $seconds);
    diag("Case shards=$shards, partitions=$parts TPS=$tps");
    ok($tps > 0, "pgbench works with $shards shards and $parts partitions");
    $master->stop; $_->stop for @$shlist;
}
