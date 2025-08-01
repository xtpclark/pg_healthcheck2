def get_weight():
    """Returns the importance score for this module."""
    return 8

def run_table_metrics(connector, settings):
    """
    Analyzes key table-level metrics, focusing on bloat estimation and scan efficiency to identify performance bottlenecks.
    """
    adoc_content = [
        "=== Table Health and Scan Efficiency",
        "Provides an analysis of table size, bloat, and scan patterns. High bloat or frequent sequential scans on large tables can severely degrade performance.\n"
    ]
    structured_data = {}

    try:
        # Optional: Check minimum PostgreSQL version for compatibility
        version_info = connector.version_info
        if version_info.get('major_version', 0) < 9:
            raise ValueError(f"PostgreSQL version {version_info.get('version_string', 'Unknown')} is not supported for table metrics analysis.")

        # Updated query to correctly extract fillfactor
        table_metrics_query = """
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
                    c.relpages * 8192 AS real_size,
                    (SELECT setting FROM pg_settings WHERE name = 'block_size')::numeric AS bs,
                    CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE c.relpages * 8192 * (1 - (c.reltuples / (c.relpages * ((SELECT setting FROM pg_settings WHERE name = 'block_size')::numeric - 24) / 8192))) END AS extra_size,
                    CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE 1 - (c.reltuples / (c.relpages * ((SELECT setting FROM pg_settings WHERE name = 'block_size')::numeric - 24) / 8192)) END AS extra_ratio,
                    COALESCE(
                        NULLIF(
                            regexp_replace(
                                (SELECT reloptions::text FROM pg_class WHERE oid = c.oid AND reloptions::text LIKE '%%fillfactor%%'),
                                '.*fillfactor=(\\d+).*', '\\1'
                            ), ''
                        )::numeric,
                        100
                    ) AS fillfactor
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
            CASE WHEN ts.seq_scan > 0 THEN ROUND((COALESCE(ts.idx_scan, 0) * 100.0 / (ts.seq_scan + COALESCE(ts.idx_scan, 0)))::numeric, 2) ELSE 100 END AS idx_scan_pct
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN table_scans ts ON ts.relid = c.oid
        LEFT JOIN table_bloat tb ON tb.tblid = c.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND pg_total_relation_size(c.oid) > (10 * 1024 * 1024) -- Only tables > 10MB
        ORDER BY tb.bloat_size DESC NULLS LAST, ts.seq_scan DESC
        LIMIT %(limit)s;
        """

        if settings.get('show_qry') == 'true':
            adoc_content.append("Table metrics query:")
            adoc_content.append("[,sql]\n----")
            adoc_content.append(table_metrics_query % {'limit': settings.get('row_limit', 20)})
            adoc_content.append("----")
            
        params_for_query = {'limit': settings.get('row_limit', 20)}
        formatted_result, raw_result = connector.execute_query(table_metrics_query, params=params_for_query, return_raw=True)

        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["table_metrics"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nNo significant table metric issues found for tables larger than 10MB.\n====\n")
            structured_data["table_metrics"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nReview the tables below for high bloat or low index scan usage. High `estimated_bloat` indicates wasted space that can slow down queries. A low `idx_scan_pct` on a frequently scanned table suggests missing or ineffective indexes.\n====\n")
            adoc_content.append(formatted_result)
            structured_data["table_metrics"] = {"status": "success", "data": raw_result}

    except Exception as e:
        error_msg = f"Failed during table metrics analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["table_metrics"] = {"status": "error", "details": error_msg}

    adoc_content.append("\n[TIP]\n====\n* **To fix bloat**: Run `VACUUM FULL <table_name>;` (requires an exclusive lock) or use the `pg_repack` extension for an online solution.\n* **To improve scan efficiency**: Analyze queries hitting tables with high `seq_scan` counts and low `idx_scan_pct` to identify opportunities for new indexes.\n====\n")

    return "\n".join(adoc_content), structured_data
