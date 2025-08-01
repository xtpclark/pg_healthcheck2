"""
Query library for the table_health_analysis check.
"""

def get_table_health_query():
    """
    Returns a query that analyzes table size, bloat, and scan patterns.

    The query has been made more robust by adding NULLIF checks to prevent
    potential division-by-zero errors and correcting the fillfactor extraction logic.
    """
    # The limit parameter is supplied by the calling check module.
    return """
        WITH table_scans AS (
            SELECT
                relid,
                seq_scan,
                idx_scan
            FROM pg_stat_user_tables
        ),
        table_bloat AS (
            SELECT
                tblid,
                bs,
                real_size,
                extra_size,
                extra_ratio,
                fillfactor,
                (real_size - bs * fillfactor / 100) AS bloat_size
            FROM (
                SELECT
                    c.oid AS tblid,
                    -- MODIFIED: Cast relpages to bigint to prevent integer overflow
                    c.relpages::bigint * 8192 AS real_size,
                    (SELECT setting FROM pg_settings WHERE name = 'block_size')::numeric AS bs,
                    -- MODIFIED: Cast relpages to bigint here as well
                    CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE c.relpages::bigint * 8192 * (1 - (c.reltuples / NULLIF(c.relpages * (((SELECT setting FROM pg_settings WHERE name = 'block_size')::numeric - 24) / 8192), 0))) END AS extra_size,
                    CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE 1 - (c.reltuples / NULLIF(c.relpages * (((SELECT setting FROM pg_settings WHERE name = 'block_size')::numeric - 24) / 8192), 0)) END AS extra_ratio,
                    CASE
                        WHEN array_to_string(c.reloptions, ',') ~ 'fillfactor'
                        THEN substring(array_to_string(c.reloptions, ','), 'fillfactor=([0-9]+)')::numeric
                        ELSE 100
                    END AS fillfactor
                FROM pg_class c
                WHERE c.relkind = 'r'
            ) AS s
        )
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
            pg_size_pretty(tb.bloat_size::bigint) AS estimated_bloat,
            ROUND((tb.bloat_size * 100 / NULLIF(tb.real_size, 0))::numeric, 2) AS bloat_pct,
            ts.seq_scan,
            COALESCE(ts.idx_scan, 0) AS idx_scan,
            CASE WHEN ts.seq_scan > 0 OR ts.idx_scan > 0 THEN ROUND((COALESCE(ts.idx_scan, 0) * 100.0 / (ts.seq_scan + COALESCE(ts.idx_scan, 0)))::numeric, 2) ELSE 100 END AS idx_scan_pct
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN table_scans ts ON ts.relid = c.oid
        LEFT JOIN table_bloat tb ON tb.tblid = c.oid
        -- CORRECTED: Added 'pg_toast' to the list of excluded system schemas
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND pg_total_relation_size(c.oid) > (10 * 1024 * 1024) -- Only tables > 10MB
        ORDER BY tb.bloat_size DESC NULLS LAST, ts.seq_scan DESC NULLS LAST
        LIMIT %(limit)s;
    """
