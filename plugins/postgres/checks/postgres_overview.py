def run_postgres_overview(connector, settings):
    """
    Provides an overview of the PostgreSQL database, including version, uptime, size, and key configuration settings.
    """
    adoc_content = ["Provides an overview of the PostgreSQL database, including version, uptime, size, and key configuration settings."]
    structured_data = {} # Dictionary to hold structured findings for this module

    if settings['show_qry'] == 'true':
        adoc_content.append("Overview queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT version();")
        adoc_content.append("SELECT current_database() AS database, pg_size_pretty(pg_database_size(current_database())) AS size;")
        adoc_content.append("SELECT setting AS uptime FROM pg_settings WHERE name = 'pg_uptime';")
        adoc_content.append("SELECT name, setting, unit FROM pg_settings WHERE name IN ('max_connections', 'work_mem', 'shared_buffers', 'effective_cache_size') ORDER BY name;")
        adoc_content.append("----")

    queries = [
        ("Database Version", "SELECT version();", True, "version_info"),
        ("Database Size", "SELECT current_database() AS database, pg_size_pretty(pg_database_size(current_database())) AS size;", True, "database_size"),
        ("Uptime", "SELECT setting AS uptime FROM pg_settings WHERE name = 'pg_uptime';", settings.get('is_aurora', 'false') == 'false', "uptime"), # Aurora-specific condition
        ("Key Configuration Settings", "SELECT name, setting, unit FROM pg_settings WHERE name IN ('max_connections', 'work_mem', 'shared_buffers', 'effective_cache_size') ORDER BY name;", True, "key_config")
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            note_msg = 'Aurora-specific metrics not available.' if 'pg_uptime' in query else 'Query not applicable.'
            adoc_content.append(f"{title}\n[NOTE]\n====\n{note_msg}\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": note_msg}
            continue
        
        # Execute query using the connector, requesting raw data
        params_for_query = None # No named parameters for these specific queries
        formatted_result, raw_result = connector.execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data

    adoc_content.append("[TIP]\n====\nReview database version for compatibility and upgrades. Check max_connections and work_mem for performance tuning, especially for CPU saturation issues.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
