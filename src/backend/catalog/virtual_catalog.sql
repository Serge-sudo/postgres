/*
 * SQL functions for virtual catalog inspection
 */

-- Function to get virtual catalog entry count
CREATE OR REPLACE FUNCTION pg_virtual_catalog_entry_count()
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_virtual_catalog_entry_count'
LANGUAGE C STRICT;