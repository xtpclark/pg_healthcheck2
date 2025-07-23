# Import the centralized compatibility module
from .postgresql_version_compatibility import get_postgresql_version

def run_table_metrics(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes table sizes, bloat, and vacuum status, providing both a detailed
    view of the worst offenders and a high-level summary of systemic issues.
    This module is version-aware.
    """
    adoc_content = ["=== Table Size, Bloat, and Vacuum Analysis\nAnalyzes table health to identify storage issues and autovacuum tuning opportunities.\n"]
    structured_data = {}

    # --- Get PostgreSQL Version Info ---
    try:
        compatibility_info = get_postgresql_version(cursor, execute_query)
        pg_major_version = compatibility_info.get('major_version_number', 0)
        structured_data["postgres_version_checked"] = pg_major_version
    except Exception as e:
        # Handle cases where version detection might fail
        adoc_content.append(f"[WARNING]\n====\nCould not determine PostgreSQL version: {e}. Queries may not be version-specific.\n====\n")
        structured_data["postgres_version_checked"] = "Unknown"

    # --- Query for Top N Largest Tables (for display) ---
    table_sizes_query = """
        SELECT schemaname || '.' || relname AS table_name,
               pg_size_pretty(pg_total_relation_size(relid)) AS size
        FROM pg_stat_user_tables
        ORDER BY pg_total_relation_size(relid) DESC
        LIMIT %(limit)s;
    """
    
    # --- Query for Top N Most Bloated Tables (for display) ---
    live_dead_tuples_query = """
        SELECT relname,
               n_live_tup AS live_tuples,
               n_dead_tup AS dead_tuples,
               ROUND((n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0)) * 100, 2) AS dead_tuple_percent
        FROM pg_stat_user_tables
        WHERE n_dead_tup > 1000 -- Only consider tables with a meaningful number of dead tuples
        ORDER BY n_dead_tup DESC
        LIMIT %(limit)s;
    """

    # --- Query for Bloat Summary Statistics (no LIMIT) ---
    # NOTE: The FILTER clause is compatible with PostgreSQL 9.4+
    bloat_summary_query = """
        SELECT
            COUNT(*) AS tables_analyzed,
            COUNT(*) FILTER (WHERE dead_tuple_percent > 20) AS tables_with_high_bloat,
            COUNT(*) FILTER (WHERE dead_tuple_percent > 50) AS tables_with_critical_bloat,
            pg_size_pretty(SUM(estimated_bloat_bytes)) AS total_estimated_bloat
        FROM (
            SELECT
                n_dead_tup,
                n_live_tup,
                (n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0)) * 100 AS dead_tuple_percent,
                (n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0)) * pg_total_relation_size(relid) AS estimated_bloat_bytes
            FROM pg_stat_user_tables
            WHERE n_live_tup > 0 -- Avoid division by zero
        ) AS bloat_stats;
    """

    if settings['show_qry'] == 'true':
        adoc_content.append("Table metrics queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(table_sizes_query)
        adoc_content.append(live_dead_tuples_query)
        adoc_content.append(bloat_summary_query)
        adoc_content.append("----")

    # --- Execute Queries and Structure Data ---

    # 1. Table Sizes
    params = {'limit': settings['row_limit']}
    formatted_result, raw_result = execute_query(table_sizes_query, params=params, return_raw=True)
    adoc_content.append("Top Tables by Total Size")
    adoc_content.append(formatted_result)
    structured_data["top_table_sizes"] = {"status": "success", "data": raw_result}

    # 2. Top Bloated Tables
    formatted_result, raw_result = execute_query(live_dead_tuples_query, params=params, return_raw=True)
    adoc_content.append("\nTop Tables by Dead Tuples")
    adoc_content.append(formatted_result)
    structured_data["top_bloated_tables"] = {"status": "success", "data": raw_result}

    # 3. Bloat Summary
    formatted_result, raw_result = execute_query(bloat_summary_query, return_raw=True)
    adoc_content.append("\nOverall Bloat Summary")
    adoc_content.append(formatted_result)
    structured_data["bloat_summary"] = {"status": "success", "data": raw_result}


    adoc_content.append("\n[TIP]\n====\nA high number of dead tuples or a large 'Total Estimated Bloat' suggests that autovacuum may not be running aggressively enough for your workload. Consider tuning autovacuum parameters either globally or on a per-table basis.\n====")
    
    return "\n".join(adoc_content), structured_data
