#!/bin/bash

# Test virtual catalog specific functionality

set -e

# Configuration
PGDATA="/tmp/pg_data_vctest"
PGPORT="5434"
LOGFILE="/tmp/pg_vctest.log"

echo "Starting PostgreSQL virtual catalog specific test..."

# Clean up any existing test data
rm -rf $PGDATA
killall postgres 2>/dev/null || true

# Initialize database
echo "Initializing database..."
/tmp/pg_install/bin/initdb -D $PGDATA --auth-local=trust --auth-host=trust

# Start PostgreSQL
echo "Starting PostgreSQL..."
/tmp/pg_install/bin/pg_ctl -D $PGDATA -l $LOGFILE start -o "-p $PGPORT"

# Wait for startup
sleep 2

echo "Testing virtual catalog functionality..."

# Test virtual catalog functionality
cat > /tmp/test_virtual_catalog_specific.sql << 'EOF'
-- Test that we can create and query temp tables
CREATE TEMP TABLE vc_test1 (id int, data text);
INSERT INTO vc_test1 VALUES (1, 'test data');

-- Verify the table exists and works
SELECT * FROM vc_test1;

-- Check that temp table metadata is visible in pg_class
SELECT relname, relpersistence, relkind FROM pg_class 
WHERE relname = 'vc_test1';

-- Test another temp table
CREATE TEMP TABLE vc_test2 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO vc_test2 (name) VALUES ('Test User 1'), ('Test User 2');

-- Verify both tables work
SELECT COUNT(*) as vc_test1_count FROM vc_test1;
SELECT COUNT(*) as vc_test2_count FROM vc_test2;

-- Test that we can drop temp tables
DROP TABLE vc_test1;

-- Verify vc_test1 is gone but vc_test2 still exists
SELECT COUNT(*) as remaining_vc_test1 FROM pg_class WHERE relname = 'vc_test1';
SELECT COUNT(*) as remaining_vc_test2 FROM pg_class WHERE relname = 'vc_test2';

-- Clean up
DROP TABLE vc_test2;

-- Verify both are gone
SELECT COUNT(*) as total_vc_test_tables FROM pg_class WHERE relname LIKE 'vc_test%';
EOF

echo "Running virtual catalog specific tests..."
/tmp/pg_install/bin/psql -p $PGPORT -d postgres -f /tmp/test_virtual_catalog_specific.sql

echo "Virtual catalog test completed!"

# Shutdown PostgreSQL
echo "Shutting down PostgreSQL..."
/tmp/pg_install/bin/pg_ctl -D $PGDATA stop

# Clean up
rm -rf $PGDATA /tmp/test_virtual_catalog_specific.sql

echo "Virtual catalog test completed successfully!"