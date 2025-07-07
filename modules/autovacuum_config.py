def run_autovacuum_config(cursor, settings, execute_query, execute_pgbouncer):
    """
    Analyzes PostgreSQL autovacuum configuration settings to ensure optimal
    performance and bloat prevention.
    """
    content = ["=== Autovacuum Configuration Analysis", "Analyzes key autovacuum settings to ensure efficient bloat management and performance."]
    
    if settings['show_qry'] == 'true':
        content.append("Autovacuum configuration queries:")
        content.append("[,sql]\n----")
        content.append("SELECT name, setting, unit, short_desc FROM pg_settings WHERE name LIKE 'autovacuum_%' ORDER BY name;")
        content.append("""
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
        content.append("----")

    queries = [
        (
            "Global Autovacuum Settings", 
            "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name LIKE 'autovacuum_%' ORDER BY name;", 
            True
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
            True
        )
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = {'limit': settings['row_limit']} if '%(limit)s' in query else None
        
        result = execute_query(query, params=params_for_query)
        
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"{title}\n{result}")
        else:
            content.append(title)
            content.append(result)
    
    content.append("[TIP]\n====\n"
                   "Autovacuum is critical for maintaining database health and performance by reclaiming dead tuples and preventing transaction ID wraparound. "
                   "Ensure `autovacuum` is `on` globally. "
                   "Review `autovacuum_vacuum_scale_factor` and `autovacuum_analyze_scale_factor` for appropriate thresholds. "
                   "High `autovacuum_vacuum_cost_delay` can slow down vacuuming; consider reducing it for busy systems. "
                   "Identify tables with disabled autovacuum or custom settings that might be causing bloat or performance issues. "
                   "For Aurora, autovacuum parameters are managed via the DB cluster parameter group.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora handles autovacuum internally, but you can still tune parameters like `autovacuum_vacuum_cost_delay` via the DB cluster parameter group. "
                       "Monitoring `FreeStorageSpace` and `CPUUtilization` in CloudWatch can help assess autovacuum effectiveness. "
                       "Ensure your autovacuum settings are optimized for your workload to prevent performance degradation.\n"
                       "====\n")
    
    return "\n".join(content)

