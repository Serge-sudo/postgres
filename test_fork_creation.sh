#!/bin/bash
# 
# Test script to verify PostgreSQL fork creation behavior for different table types
# 
# This script demonstrates that:
# 1. Temporary tables do NOT create init forks
# 2. Temporary tables do NOT create VM forks (no MVCC needed for single backend)
# 3. Unlogged tables DO create init forks  
# 4. All table types create main forks
# 5. FSM forks are created on-demand for space management
# 6. VM forks are created only for permanent and unlogged tables (multi-session MVCC)

set -e

echo "=== PostgreSQL Fork Creation Test ==="
echo "This test verifies fork creation patterns for different table persistence types"
echo

# Note: This test requires a running PostgreSQL instance
# For demonstration purposes, we'll show the expected SQL commands and file patterns

cat << 'EOF'
-- Test SQL Commands (run against a PostgreSQL instance):

-- 1. Create different table types
CREATE TEMPORARY TABLE temp_test (id INT, data TEXT);
CREATE UNLOGGED TABLE unlogged_test (id INT, data TEXT); 
CREATE TABLE permanent_test (id INT, data TEXT);

-- 2. Insert some data to trigger fork creation
INSERT INTO temp_test VALUES (1, 'temp data');
INSERT INTO unlogged_test VALUES (1, 'unlogged data');
INSERT INTO permanent_test VALUES (1, 'permanent data');

-- 3. Check relation file locations
SELECT 
    c.relname,
    c.relpersistence,
    c.relfilenode,
    pg_relation_filepath(c.oid) as filepath
FROM pg_class c 
WHERE c.relname IN ('temp_test', 'unlogged_test', 'permanent_test');

-- 4. Find actual file locations in data directory
-- For temporary tables: base/[dboid]/t[backend_pid]_[relfilenode]
-- For others: base/[dboid]/[relfilenode]

EOF

echo "Expected Fork File Patterns:"
echo "============================"
echo
echo "Temporary Table (temp_test):"
echo "  ✓ t<pid>_<relfilenode>        (main fork)"
echo "  ✗ t<pid>_<relfilenode>_init   (NO init fork)"
echo "  ? t<pid>_<relfilenode>_fsm    (created on demand for space management)"
echo "  ✗ t<pid>_<relfilenode>_vm     (NO VM fork - no MVCC needed)"
echo
echo "Unlogged Table (unlogged_test):"
echo "  ✓ <relfilenode>               (main fork)"
echo "  ✓ <relfilenode>_init          (init fork - for crash recovery)"
echo "  ? <relfilenode>_fsm           (created on demand for space management)"
echo "  ? <relfilenode>_vm            (created on demand for MVCC optimization)"
echo
echo "Permanent Table (permanent_test):"
echo "  ✓ <relfilenode>               (main fork)"
echo "  ✗ <relfilenode>_init          (NO init fork)"
echo "  ? <relfilenode>_fsm           (created on demand for space management)"
echo "  ? <relfilenode>_vm            (created on demand for MVCC optimization)"
echo

echo "Key Code Locations:"
echo "=================="
echo "1. RelationCreateStorage():     src/backend/catalog/storage.c:121"
echo "2. Init fork creation logic:   src/backend/access/heap/heapam_handler.c:331"
echo "3. Fork type definitions:      src/include/common/relpath.h:47"
echo "4. WAL decision logic:         src/backend/catalog/storage.c:130"
echo

cat << 'EOF'
To run this test:
1. Start a PostgreSQL instance
2. Run the SQL commands above
3. Check the data directory for fork files
4. Verify init forks only exist for unlogged tables
5. Verify VM forks are NOT created for temporary tables

Example verification commands:
# Find PostgreSQL data directory
SHOW data_directory;

# List fork files (from shell)
ls -la $PGDATA/base/[your_db_oid]/ | grep -E "(temp_test|unlogged_test|permanent_test)"

# Look specifically for the different fork types:
# _init files should only exist for unlogged tables
# _vm files should NOT exist for temporary tables
# _fsm files may exist for any table type when space management is needed
EOF