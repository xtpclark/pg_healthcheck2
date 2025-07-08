def run_settings(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes key PostgreSQL configuration settings to identify potential tuning
    opportunities for performance and stability.
    """
    adoc_content = ["=== PostgreSQL Settings", "Analyzes key PostgreSQL configuration settings to identify potential tuning opportunities for performance and stability."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Settings queries:")
        adoc_content.append("[,sql]\n----")
        # Corrected and expanded category names based on PostgreSQL 15 output
        adoc_content.append("SELECT name, setting, unit, category, short_desc FROM pg_settings WHERE category IN ('Connections and Authentication / Connection Settings', 'Connections and Authentication / Authentication', 'Connections and Authentication / SSL', 'Resource Usage / Memory', 'Query Tuning / Planner Cost Constants', 'Query Tuning / Other Planner Options', 'Query Tuning / Planner Method Configuration', 'Reporting and Logging / What to Log', 'Reporting and Logging / Where to Log', 'Reporting and Logging / When to Log', 'Write-Ahead Log / Settings', 'Write-Ahead Log / Checkpoints', 'Resource Usage / Asynchronous Behavior', 'Resource Usage / Background Writer', 'Resource Usage / Disk', 'Resource Usage / Kernel Resources') ORDER BY category, name LIMIT %(limit)s;")
        adoc_content.append("SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('max_connections', 'work_mem', 'shared_buffers', 'effective_cache_size', 'maintenance_work_mem', 'autovacuum', 'checkpoint_timeout', 'wal_level', 'max_wal_size') ORDER BY name;")
        adoc_content.append("----")

    queries = [
        (
            "General Configuration Settings", # Renamed title for broader scope
            "SELECT name, setting, unit, category, short_desc FROM pg_settings WHERE category IN ('Connections and Authentication / Connection Settings', 'Connections and Authentication / Authentication', 'Connections and Authentication / SSL', 'Resource Usage / Memory', 'Query Tuning / Planner Cost Constants', 'Query Tuning / Other Planner Options', 'Query Tuning / Planner Method Configuration', 'Reporting and Logging / What to Log', 'Reporting and Logging / Where to Log', 'Reporting and Logging / When to Log', 'Write-Ahead Log / Settings', 'Write-Ahead Log / Checkpoints', 'Resource Usage / Asynchronous Behavior', 'Resource Usage / Background Writer', 'Resource Usage / Disk', 'Resource Usage / Kernel Resources') ORDER BY category, name LIMIT %(limit)s;", 
            True, 
            "general_configuration_settings" # Data key
        ),
        (
            "Critical Performance Settings", 
            "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('max_connections', 'work_mem', 'shared_buffers', 'effective_cache_size', 'maintenance_work_mem', 'autovacuum', 'checkpoint_timeout', 'wal_level', 'max_wal_size') ORDER BY name;", 
            True, 
            "critical_performance_settings" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Execute query, requesting raw data
        params_for_query = {'limit': settings['row_limit']} # These queries use %(limit)s
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\nReview max_connections to ensure sufficient connection slots for your workload, especially for Aurora CPU saturation issues. Adjust work_mem and shared_buffers to balance memory usage and performance. Enable autovacuum if disabled to prevent bloat.\n====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nFor AWS RDS Aurora, some settings (e.g., shared_buffers) are managed by the parameter group. Use AWS Console to adjust these parameters.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

