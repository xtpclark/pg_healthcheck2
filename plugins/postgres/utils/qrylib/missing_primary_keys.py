"""
Query library for the missing_primary_keys check.
"""

def get_missing_primary_keys_query():
    """
    Returns a query that finds user tables (excluding partitions) that do
    not have a primary key constraint.
    """
    return """
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            pg_size_pretty(pg_total_relation_size(c.oid)) as table_size
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND c.relispartition = false -- Exclude partitions from the check
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND NOT EXISTS (
              SELECT 1 FROM pg_constraint con
              WHERE con.conrelid = c.oid AND con.contype = 'p'
          )
        ORDER BY pg_total_relation_size(c.oid) DESC;
    """
