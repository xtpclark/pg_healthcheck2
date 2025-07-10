def run_bgwriter(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes background writer activity to optimize buffer management and reduce I/O load.
    """
    adoc_content = ["=== Background Writer Statistics", "Analyzes background writer activity to optimize buffer management and reduce I/O load.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Get PostgreSQL version
    pg_version_query = "SHOW server_version_num;"
    _, raw_pg_version = execute_query(pg_version_query, is_check=True, return_raw=True)
    pg_version_num = int(raw_pg_version) # e.g., 170000 for PG 17

    # Determine if it's PostgreSQL 17 or newer (version number 170000 and above)
    is_pg17_or_newer = pg_version_num >= 170000

    if settings['show_qry'] == 'true':
        adoc_content.append("Background writer queries:")
        adoc_content.append("[,sql]\n----")
        if is_pg17_or_newer:
            # For PG17+, pg_stat_bgwriter has fewer columns related to backend writes
            adoc_content.append("SELECT buffers_clean, maxwritten_clean, buffers_alloc FROM pg_stat_bgwriter;")
        else:
            adoc_content.append("SELECT buffers_clean, maxwritten_clean, buffers_backend, buffers_alloc, buffers_backend_fsync FROM pg_stat_bgwriter;")
        adoc_content.append("SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('bgwriter_lru_maxpages', 'bgwriter_lru_multiplier', 'bgwriter_delay') ORDER BY name;")
        adoc_content.append("----")

    queries = []

    if is_pg17_or_newer:
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
    if is_pg17_or_newer:
        adoc_content.append("[TIP]\n====\nHigh `buffers_alloc` values indicate significant buffer allocation activity. Adjust `bgwriter_lru_maxpages` or reduce `bgwriter_delay` for more aggressive cleaning to optimize buffer management. For Aurora, tune these settings via the RDS parameter group to mitigate CPU and IOPS saturation.\n====\n")
    else:
        adoc_content.append("[TIP]\n====\nHigh `buffers_backend` or `buffers_backend_fsync` values indicate heavy backend writes, increasing I/O load. Adjust `bgwriter_lru_maxpages` or reduce `bgwriter_delay` for more aggressive cleaning. For Aurora, tune these settings via the RDS parameter group to mitigate CPU and IOPS saturation.\n====\n")
    
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nAWS RDS Aurora manages background writer settings via the parameter group. Use the AWS Console to adjust `bgwriter_lru_maxpages` or `bgwriter_delay`.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
