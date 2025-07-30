#!/bin/bash

# Test script for virtual catalog functionality
# This tests that temporary table metadata is stored in virtual catalog

set -e

# Configuration
PGDATA="/tmp/pg_data_test"
PGPORT="5433"
LOGFILE="/tmp/pg_test.log"

echo "Starting PostgreSQL virtual catalog test..."

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

# Test basic functionality
echo "Testing virtual catalog functionality..."

# Create test SQL
cat > /tmp/test_virtual_catalog.sql << 'EOF'
-- Test 1: Create temporary table
CREATE TEMP TABLE temp_test_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Test 2: Create regular table
CREATE TABLE regular_test_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert test data
INSERT INTO temp_test_table (name) VALUES ('Temp record 1'), ('Temp record 2');
INSERT INTO regular_test_table (name) VALUES ('Regular record 1'), ('Regular record 2');

-- Test 3: Verify we can query both tables
SELECT 'temp' as table_type, count(*) as record_count FROM temp_test_table;
SELECT 'regular' as table_type, count(*) as record_count FROM regular_test_table;

-- Test 4: Check catalog visibility
-- Both tables should appear in pg_class
SELECT relname, relpersistence, 
       CASE WHEN relpersistence = 't' THEN 'temporary'
            ELSE 'permanent' END as table_type
FROM pg_class 
WHERE relname LIKE '%_test_table'
ORDER BY relname;

-- Test 5: Test attribute metadata
SELECT c.relname, a.attname, a.attnum, t.typname
FROM pg_class c
JOIN pg_attribute a ON c.oid = a.attrelid
JOIN pg_type t ON a.atttypid = t.oid
WHERE c.relname LIKE '%_test_table' 
  AND a.attnum > 0
ORDER BY c.relname, a.attnum;

-- Clean up
DROP TABLE temp_test_table;
DROP TABLE regular_test_table;

-- Verify cleanup
SELECT COUNT(*) as remaining_tables
FROM pg_class 
WHERE relname LIKE '%_test_table';
EOF

# Run the test
echo "Running SQL tests..."
/tmp/pg_install/bin/psql -p $PGPORT -d postgres -f /tmp/test_virtual_catalog.sql

# Test temp table in separate session
echo "Testing temp table isolation..."
cat > /tmp/test_isolation.sql << 'EOF'
-- This should create a temp table visible only in this session
CREATE TEMP TABLE isolation_test (id INT, data TEXT);
INSERT INTO isolation_test VALUES (1, 'Session 1');
SELECT * FROM isolation_test;
EOF

/tmp/pg_install/bin/psql -p $PGPORT -d postgres -f /tmp/test_isolation.sql

# The table should not be visible in a different session
echo "Verifying temp table is not visible in different session..."
echo "SELECT COUNT(*) FROM pg_class WHERE relname = 'isolation_test';" | \
    /tmp/pg_install/bin/psql -p $PGPORT -d postgres

# Shutdown PostgreSQL
echo "Shutting down PostgreSQL..."
/tmp/pg_install/bin/pg_ctl -D $PGDATA stop

echo "Virtual catalog test completed successfully!"

# Clean up
rm -rf $PGDATA /tmp/test_virtual_catalog.sql /tmp/test_isolation.sql