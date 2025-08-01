def get_pk_exhaustion_summary_query(connector):
    """
    Returns a query that provides a high-level summary of primary keys that
    are integer-based, for AI analysis.
    """
    return """
        SELECT
            COUNT(*) AS total_integer_pks,
            COUNT(*) FILTER (WHERE a.atttypid = 21) AS smallint_pk_count,
            COUNT(*) FILTER (WHERE a.atttypid = 23) AS integer_pk_count
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_constraint con ON con.conrelid = c.oid
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(con.conkey)
        WHERE con.contype = 'p'
          AND a.atttypid IN (21, 23) -- smallint, integer
          AND n.nspname NOT IN ('information_schema', 'pg_catalog');
    """

def get_pk_exhaustion_details_query(connector):
    """
    Returns a query to find integer-based primary keys that are nearing
    their maximum value (exhaustion) by correctly finding the associated sequence.
    """
    # This query now uses pg_get_serial_sequence to reliably find the sequence name
    # and correctly uses a WHERE clause for filtering.
    return """
        WITH pk_info AS (
            SELECT
                n.nspname,
                c.relname,
                a.attname,
                a.atttypid,
                pg_get_serial_sequence(quote_ident(n.nspname) || '.' || quote_ident(c.relname), a.attname) AS seq_name
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_constraint con ON con.conrelid = c.oid
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(con.conkey)
            WHERE con.contype = 'p'
              AND a.atttypid IN (21, 23) -- smallint, integer
              AND n.nspname NOT IN ('information_schema', 'pg_catalog')
        )
        SELECT
            pi.nspname AS table_schema,
            pi.relname AS table_name,
            pi.attname AS column_name,
            format_type(pi.atttypid, -1) AS data_type,
            s.last_value,
            CASE
                WHEN pi.atttypid = 21 THEN 32767
                WHEN pi.atttypid = 23 THEN 2147483647
            END as max_value,
            ROUND((s.last_value::numeric / (
                CASE
                    WHEN pi.atttypid = 21 THEN 32767
                    WHEN pi.atttypid = 23 THEN 2147483647
                END
            )::numeric) * 100, 2) AS percentage_used
        FROM pk_info pi
        JOIN pg_sequences s ON pi.seq_name = s.schemaname || '.' || s.sequencename
        WHERE s.last_value IS NOT NULL
          -- CORRECTED: Filtering logic moved from HAVING to WHERE
          AND (s.last_value::numeric / (
                CASE
                    WHEN pi.atttypid = 21 THEN 32767
                    WHEN pi.atttypid = 23 THEN 2147483647
                END
            )::numeric) > 0.80 -- Threshold for reporting (80%)
        ORDER BY percentage_used DESC;
    """
