"""
Query library for the index_health_analysis check.
"""

def get_unused_indexes_query(connector):
    """
    Returns a query to find large, unused indexes. This is a key indicator of
    unnecessary write overhead and wasted space.
    """
    return """
        SELECT
            schemaname AS schema_name,
            relname AS table_name,
            indexrelname AS index_name,
            pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
            idx_scan AS index_scans
        FROM pg_stat_user_indexes
        WHERE idx_scan < 100 AND pg_relation_size(indexrelid) > 1048576 -- Scanned < 100 times and > 1MB
        ORDER BY pg_relation_size(indexrelid) DESC
        LIMIT %(limit)s;
    """

def get_duplicate_indexes_query(connector):
    """
    Returns a query to find indexes that are functionally duplicates of each other.
    """
    return """
        SELECT n.nspname || '.' || t.relname AS table_name,
        pg_size_pretty(SUM(pg_relation_size(pi.indexrelid))::bigint) AS total_wasted_size,
        array_agg(i.relname ORDER BY i.relname) AS redundant_indexes
        FROM pg_index AS pi
        JOIN pg_class AS i ON i.oid = pi.indexrelid
        JOIN pg_class AS t ON t.oid = pi.indrelid
        JOIN pg_namespace AS n ON n.oid = t.relnamespace
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND pi.indisprimary = false
        GROUP BY pi.indrelid, pi.indkey, pi.indclass, pi.indpred, n.nspname, t.relname
        HAVING COUNT(*) > 1 ORDER BY SUM(pg_relation_size(pi.indexrelid)) DESC LIMIT %(limit)s;
    """

def get_invalid_indexes_query(connector):
    """
    Returns a query to find invalid indexes that are unusable by the planner.
    """
    return """
        SELECT n.nspname AS schema_name, c.relname AS table_name, i.relname AS index_name
        FROM pg_class c, pg_index ix, pg_class i, pg_namespace n
        WHERE ix.indisvalid = false
          AND ix.indexrelid = i.oid
          AND i.relnamespace = c.relnamespace
          AND c.oid = ix.indrelid
          AND i.relnamespace = n.oid;
    """

def get_specialized_indexes_summary_query(connector):
    """
    Returns a query that provides a summary count of each specialized index type.
    """
    return """
        SELECT
            am.amname AS index_type,
            COUNT(*) AS count
        FROM pg_class i
        JOIN pg_am am ON am.oid = i.relam
        JOIN pg_namespace n ON n.oid = i.relnamespace
        WHERE i.relkind = 'i'
          AND am.amname NOT IN ('btree')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        GROUP BY am.amname;
    """

def get_specialized_indexes_details_query(connector):
    """
    Returns a query to get detailed information about all non-B-Tree indexes.
    """
    return """
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            i.relname AS index_name,
            am.amname AS index_type,
            pg_size_pretty(pg_relation_size(i.oid)) as index_size
        FROM pg_class c
        JOIN pg_index ix ON ix.indrelid = c.oid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON am.oid = i.relam
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE am.amname NOT IN ('btree')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY am.amname, n.nspname, c.relname, i.relname;
    """
