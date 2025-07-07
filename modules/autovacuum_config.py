def run_autovacuum_config(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes PostgreSQL autovacuum configuration settings to ensure optimal
    performance and bloat prevention.
    """
    adoc_content = ["=== Autovacuum Configuration Analysis", "Analyzes key autovacuum settings to ensure efficient bloat management and performance."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Autovacuum configuration queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT name, setting, unit, short_desc FROM pg_settings WHERE name LIKE 'autovacuum_%' ORDER BY name;")
        adoc_content.append("""
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    COALESCE(
        (SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'autovacuum_enabled'),
        (SELECT setting FROM pg_settings WHERE name = 'autovacuum')::text
    ) AS autovacuum_enabled,
    COALESCE(
        (SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'toast.autovacuum_enabled'),
        (SELECT setting FROM pg_settings WHERE name = 'autovacuum')::text
    ) AS toast_autovacuum_enabled,
    COALESCE(
        (SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'autovacuum_vacuum_threshold'),
        (SELECT setting FROM pg_settings WHERE name = 'autovacuum_vacuum_threshold')
    ) AS autovacuum_vacuum_threshold,
    COALESCE(
        (SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'autovacuum_vacuum_scale_factor'),
        (SELECT setting FROM pg_settings WHERE name = 'autovacuum_vacuum_scale_factor')
    ) AS autovacuum_vacuum_scale_factor,
    COALESCE(
        (SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'autovacuum_analyze_threshold'),
        (SELECT setting FROM pg_settings WHERE name = 'autovacuum_analyze_threshold')
    ) AS autovacuum_analyze_threshold,
    COALESCE(
        (SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'autovacuum_analyze_scale_factor'),
        (SELECT setting FROM pg_settings WHERE name = 'autovacuum_analyze_scale_factor')
    ) AS autovacuum_analyze_scale_factor
FROM
    pg_class c
JOIN
    pg_namespace n ON n.oid = c.relnamespace
WHERE
    c.relkind IN ('r', 'm') -- 'r' for tables, 'm' for materialized views
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
ORDER BY
    schema_name, table_name
LIMIT %(limit)s;
""")
        adoc_content.append("----")

    queries = [
        (
            "Global Autovacuum Settings", 
            "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name LIKE 'autovacuum_%' ORDER BY name;", 
            True,
            "global_autovacuum_settings" # Data key
        ),
        (
            "Tables with Custom Autovacuum Settings (or explicitly disabled)", 
            """
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    COALESCE(
        (SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'autovacuum_enabled'),
        (SELECT setting FROM pg_settings WHERE name = 'autovacuum')::text
    ) AS autovacuum_enabled,
    COALESCE(
        (SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'toast.autovacuum_enabled'),
        (SELECT setting FROM pg_settings WHERE name = 'autovacuum')::text
    ) AS toast_autovacuum_enabled,
    COALESCE(
        (SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'autovacuum_vacuum_threshold'),
        (SELECT setting FROM pg_settings WHERE name = 'autovacuum_vacuum_threshold')
    ) AS autovacuum_vacuum_threshold,
    COALESCE(
        (SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'autovacuum_vacuum_scale_factor'),
        (SELECT setting FROM pg_settings WHERE name = 'autovacuum_vacuum_scale_factor')
    ) AS autovacuum_vacuum_scale_factor,
    COALESCE(
        (SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'autovacuum_analyze_threshold'),
        (SELECT setting FROM pg_settings WHERE name = 'autovacuum_analyze_threshold')
    ) AS autovacuum_analyze_threshold,
    COALESCE(
        (SELECT option_value FROM pg_options_to_table(c.reloptions) WHERE option_name = 'autovacuum_analyze_scale_factor'),
        (SELECT setting FROM pg_settings WHERE name = 'autovacuum_analyze_scale_factor')
    ) AS autovacuum_analyze_scale_factor
FROM
    pg_class c
JOIN
    pg_namespace n ON n.oid = c.relnamespace
WHERE
    c.relkind IN ('r', 'm') -- 'r' for tables, 'm' for materialized views
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
ORDER BY
    schema_name, table_name
LIMIT %(limit)s;
""", 
            True,
            "custom_table_autovacuum_settings" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = {'limit': settings['row_limit']} if '%(limit)s' in query else None
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\n"
                   "Autovacuum is critical for maintaining database health and performance by reclaiming dead tuples and preventing transaction ID wraparound. "
                   "Ensure `autovacuum` is `on` globally. "
                   "Review `autovacuum_vacuum_scale_factor` and `autovacuum_analyze_scale_factor` for appropriate thresholds. "
                   "High `autovacuum_vacuum_cost_delay` can slow down vacuuming; consider reducing it for busy systems. "
                   "Identify tables with disabled autovacuum or custom settings that might be causing bloat or performance issues. "
                   "For Aurora, autovacuum parameters are managed via the DB cluster parameter group.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora handles autovacuum internally, but you can still tune parameters like `autovacuum_vacuum_cost_delay` via the DB cluster parameter group. "
                       "Monitoring `FreeStorageSpace` and `CPUUtilization` in CloudWatch can help assess autovacuum effectiveness. "
                       "Ensure your autovacuum settings are optimized for your workload to prevent performance degradation.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

