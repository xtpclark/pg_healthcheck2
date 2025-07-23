def run_bgwriter(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes background writer activity to optimize buffer management and reduce I/O load.
    """
    adoc_content = ["Analyzes background writer activity to optimize buffer management and reduce I/O load.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Import version compatibility module
    from .postgresql_version_compatibility import get_postgresql_version, validate_postgresql_version
    
    # Get PostgreSQL version compatibility information
    compatibility = get_postgresql_version(cursor, execute_query)
    
    # Validate PostgreSQL version
    is_supported, error_msg = validate_postgresql_version(compatibility)
    if not is_supported:
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["version_error"] = {"status": "error", "details": error_msg}
        return "\n".join(adoc_content), structured_data

    if settings['show_qry'] == 'true':
        adoc_content.append("Background writer queries:")
        adoc_content.append("[,sql]\n----")
        # Show background writer queries based on PostgreSQL version
        if compatibility['is_pg17_or_newer']:
            adoc_content.append("SELECT buffers_clean, maxwritten_clean, buffers_alloc FROM pg_stat_bgwriter;")
        else:
            adoc_content.append("SELECT buffers_clean, maxwritten_clean, buffers_backend, buffers_alloc, buffers_backend_fsync FROM pg_stat_bgwriter;")
        adoc_content.append("SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('bgwriter_lru_maxpages', 'bgwriter_lru_multiplier', 'bgwriter_delay') ORDER BY name;")
        adoc_content.append("----")

    queries = []

    if compatibility['is_pg17_or_newer']:
        queries.append(
            (
                "Background Writer Metrics (PostgreSQL 17+)", 
                "SELECT buffers_clean, maxwritten_clean, buffers_alloc FROM pg_stat_bgwriter;", 
                True,
                "bgwriter_metrics" # Data key
            )
        )
    else:
        queries.append(
            (
                "Background Writer Metrics (PostgreSQL < 17)", 
                "SELECT buffers_clean, maxwritten_clean, buffers_backend, buffers_alloc, buffers_backend_fsync FROM pg_stat_bgwriter;", 
                True,
                "bgwriter_metrics" # Data key
            )
        )
    
    queries.append(
        (
            "Background Writer Configuration", 
            "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('bgwriter_lru_maxpages', 'bgwriter_lru_multiplier', 'bgwriter_delay') ORDER BY name;", 
            True,
            "bgwriter_configuration" # Data key
        )
    )

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = None 
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    # Adjust TIP based on PG version
    if compatibility['is_pg17_or_newer']:
        adoc_content.append("[TIP]\n====\nHigh `buffers_alloc` values indicate significant buffer allocation activity. Adjust `bgwriter_lru_maxpages` or reduce `bgwriter_delay` for more aggressive cleaning to optimize buffer management. For Aurora, tune these settings via the RDS parameter group to mitigate CPU and IOPS saturation.\n====\n")
    else:
        adoc_content.append("[TIP]\n====\nHigh `buffers_backend` or `buffers_backend_fsync` values indicate heavy backend writes, increasing I/O load. Adjust `bgwriter_lru_maxpages` or reduce `bgwriter_delay` for more aggressive cleaning. For Aurora, tune these settings via the RDS parameter group to mitigate CPU and IOPS saturation.\n====\n")
    
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nAWS RDS Aurora manages background writer settings via the parameter group. Use the AWS Console to adjust `bgwriter_lru_maxpages` or `bgwriter_delay`.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
