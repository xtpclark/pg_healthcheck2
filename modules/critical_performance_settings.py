def run_critical_performance_settings(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes a focused set of critical PostgreSQL configuration settings
    directly impacting performance, such as memory, connections, and WAL.
    """
    adoc_content = ["=== Critical Performance Settings", "Analyzes key PostgreSQL configuration settings for performance impact.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    critical_settings_query = """
        SELECT name, setting, unit, short_desc
        FROM pg_settings
        WHERE name IN (
            'max_connections', 'work_mem', 'shared_buffers', 'effective_cache_size',
            'maintenance_work_mem', 'autovacuum', 'checkpoint_timeout',
            'wal_level', 'max_wal_size'
        )
        ORDER BY name;
    """

    if settings['show_qry'] == 'true':
        adoc_content.append("Critical performance settings query:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(critical_settings_query)
        adoc_content.append("----")

    # Standardized parameter passing pattern: This query does NOT use %(limit)s
    params_for_query = None 
    
    formatted_result, raw_result = execute_query(critical_settings_query, params=params_for_query, return_raw=True)
    
    if "[ERROR]" in formatted_result:
        adoc_content.append(f"Critical Performance Settings\n{formatted_result}")
        structured_data["critical_settings"] = {"status": "error", "details": raw_result}
    else:
        adoc_content.append("Critical Performance Settings")
        adoc_content.append(formatted_result)
        structured_data["critical_settings"] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\nReview `max_connections` to ensure sufficient connection slots. Adjust `work_mem` and `shared_buffers` to balance memory usage and performance. Ensure `autovacuum` is enabled. Tune `checkpoint_timeout` and `max_wal_size` to optimize WAL activity, especially for CPU saturation issues.\n====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nFor AWS RDS Aurora, critical settings like `shared_buffers` and `max_connections` are managed via the parameter group. Adjust these via the AWS Console. Performance tuning often involves a combination of parameter changes and query optimization.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

