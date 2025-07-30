def get_object_inventory_query(connector):
    """
    Returns the SQL query to list all database objects.
    This query is version-agnostic but is centralized for consistency.
    """
    return """
    -- Tables, Views, Materialized Views, Sequences
    SELECT
        n.nspname AS schema_name,
        c.relname AS object_name,
        CASE c.relkind
            WHEN 'r' THEN 'TABLE'
            WHEN 'v' THEN 'VIEW'
            WHEN 'm' THEN 'MATERIALIZED VIEW'
            WHEN 'S' THEN 'SEQUENCE'
            WHEN 'f' THEN 'FOREIGN TABLE'
        END AS object_type
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'v', 'm', 'S', 'f')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')

    UNION ALL

    -- Indexes
    SELECT
        n.nspname AS schema_name,
        c.relname AS object_name,
        'INDEX' AS object_type
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'i'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')

    UNION ALL

    -- Functions and Procedures
    SELECT
        n.nspname AS schema_name,
        p.proname AS object_name,
        CASE p.prokind
            WHEN 'f' THEN 'FUNCTION'
            WHEN 'p' THEN 'PROCEDURE'
            WHEN 'a' THEN 'AGGREGATE FUNCTION'
            WHEN 'w' THEN 'WINDOW FUNCTION'
        END AS object_type
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')

    ORDER BY schema_name, object_type, object_name;
    """
