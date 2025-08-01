"""
Query library for the vacuum_analysis check.
"""

def get_tables_needing_vacuum_or_analyze_query(connector):
    """
    Returns a query that identifies tables that have crossed their specific
    autovacuum or autoanalyze thresholds and are pending action by the
    autovacuum daemon.

    This query respects per-table custom autovacuum settings.
    Source: https://www.depesz.com/2020/02/18/which-tables-should-be-auto-vacuumed-or-auto-analyzed-update/
    """
    return """
        WITH s AS (
            SELECT
                n.nspname,
                c.relname,
                c.oid as relid,
                c.reltuples,
                s.n_dead_tup,
                s.n_mod_since_analyze,
                coalesce(
                    (SELECT split_part(x, '=', 2) FROM unnest(c.reloptions) q (x) WHERE x ~ '^autovacuum_analyze_scale_factor=' ),
                    current_setting('autovacuum_analyze_scale_factor')
                )::float8 as analyze_factor,
                coalesce(
                    (SELECT split_part(x, '=', 2) FROM unnest(c.reloptions) q (x) WHERE x ~ '^autovacuum_analyze_threshold=' ),
                    current_setting('autovacuum_analyze_threshold')
                )::float8 as analyze_threshold,
                coalesce(
                    (SELECT split_part(x, '=', 2) FROM unnest(c.reloptions) q (x) WHERE x ~ '^autovacuum_vacuum_scale_factor=' ),
                    current_setting('autovacuum_vacuum_scale_factor')
                )::float8 as vacuum_factor,
                coalesce(
                    (SELECT split_part(x, '=', 2) FROM unnest(c.reloptions) q (x) WHERE x ~ '^autovacuum_vacuum_threshold=' ),
                    current_setting('autovacuum_vacuum_threshold')
                )::float8 as vacuum_threshold
            FROM
                pg_class c
                join pg_namespace n on c.relnamespace = n.oid
                LEFT OUTER JOIN pg_stat_all_tables s ON c.oid = s.relid
            WHERE
                c.relkind = 'r'
        ), tt AS (
            SELECT
                nspname,
                relname,
                relid,
                n_dead_tup,
                n_mod_since_analyze,
                reltuples * vacuum_factor + vacuum_threshold AS v_threshold,
                reltuples * analyze_factor + analyze_threshold AS a_threshold
            FROM
                s
        )
        SELECT
            nspname as schema_name,
            relname as table_name,
            CASE WHEN n_dead_tup > v_threshold THEN 'VACUUM' ELSE '' END AS needs_vacuum,
            CASE WHEN n_mod_since_analyze > a_threshold THEN 'ANALYZE' ELSE '' END AS needs_analyze
        FROM
            tt
        WHERE
            n_dead_tup > v_threshold OR
            n_mod_since_analyze > a_threshold;
    """
