def run_cache_analysis(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes PostgreSQL buffer cache usage and hit ratios to identify performance bottlenecks.
    """
    adoc_content = ["\n=== Cache Hits and Usage\nAnalyzes PostgreSQL buffer cache usage and hit ratios to identify performance bottlenecks."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Import version compatibility module
    from .postgresql_version_compatibility import get_postgresql_version, get_cache_analysis_query, validate_postgresql_version
    
    # Get PostgreSQL version compatibility information
    compatibility = get_postgresql_version(cursor, execute_query)
    
    # Validate PostgreSQL version
    is_supported, error_msg = validate_postgresql_version(compatibility)
    if not is_supported:
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["version_error"] = {"status": "error", "details": error_msg}
        return "\n".join(adoc_content), structured_data

    if settings['show_qry'] == 'true':
        adoc_content.append("Cache analysis queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT datname, blks_hit, blks_read, round((blks_hit::float / (blks_hit + blks_read) * 100)::numeric, 2) AS hit_ratio_percent FROM pg_stat_database WHERE blks_read > 0 AND datname = %(database)s;")
        
        # Get version-specific cache analysis query
        cache_query = get_cache_analysis_query(compatibility)
        adoc_content.append(cache_query)
        adoc_content.append("----")

    queries = [
        (
            "\nDatabase Cache Hit Ratio", 
            "SELECT datname, blks_hit, blks_read, round((blks_hit::float / (blks_hit + blks_read) * 100)::numeric, 2) AS hit_ratio_percent FROM pg_stat_database WHERE blks_read > 0 AND datname = %(database)s;", 
            True, 
            "database_cache_hit_ratio" # Data key
        )
    ]

    # Add Buffer Cache Statistics based on PG version
    if compatibility['is_pg17_or_newer']:
        queries.append(
            (
                "Background Writer Buffer Statistics (PostgreSQL 17+)", 
                "SELECT buffers_alloc, buffers_clean FROM pg_stat_bgwriter;", 
                True, 
                "bgwriter_buffer_statistics" # Data key for bgwriter specific buffer stats
            )
        )
        queries.append(
            (
                "Checkpoint Buffer Statistics (PostgreSQL 17+)", 
                "SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_req, buffers_written AS buffers_checkpoint FROM pg_stat_checkpointer;", 
                True, 
                "checkpoint_buffer_statistics" # Data key for checkpoint specific buffer stats
            )
        )
    else:
        queries.append(
            (
                "Buffer Cache Statistics (PostgreSQL < 17)", 
                "SELECT buffers_alloc, buffers_backend, buffers_clean, buffers_checkpoint, checkpoints_timed, checkpoints_req FROM pg_stat_bgwriter;", 
                True, 
                "buffer_cache_statistics" # Data key
            )
        )

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = None
        if '%(database)s' in query:
            params_for_query = {'database': settings['database']}
        if '%(limit)s' in query and params_for_query is None:
            params_for_query = {'limit': settings['row_limit']}
        elif '%(limit)s' in query and params_for_query is not None:
            params_for_query['limit'] = settings['row_limit']

        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    # Enhanced cache hit ratio guidance
    adoc_content.append("\n=== Cache Hit Ratio Best Practices")
    adoc_content.append("**Target Cache Hit Ratio: >90%**")
    adoc_content.append("")
    adoc_content.append("Cache hit ratio indicates how efficiently your database is using memory for data access:")
    adoc_content.append("")
    adoc_content.append("* **>95%**: Excellent - Your database is efficiently using memory")
    adoc_content.append("* **90-95%**: Good - Monitor for trends, consider optimization")
    adoc_content.append("* **80-90%**: Fair - Review shared_buffers and query patterns")
    adoc_content.append("* **<80%**: Poor - Immediate attention required")
    adoc_content.append("")
    adoc_content.append("**Recommendations for Low Cache Hit Ratio:**")
    adoc_content.append("")
    adoc_content.append("1. **Increase shared_buffers**: Set to 25% of available RAM (up to 8GB)")
    adoc_content.append("2. **Optimize query patterns**: Ensure frequently accessed data fits in memory")
    adoc_content.append("3. **Review index usage**: Ensure indexes are being used effectively")
    adoc_content.append("4. **Monitor table sizes**: Large tables may not fit in cache")
    adoc_content.append("5. **Consider connection pooling**: Reduce memory per connection")
    adoc_content.append("")
    adoc_content.append("[TIP]\n====\nA cache hit ratio below 90% may indicate insufficient shared_buffers or ineffective query plans. Increase shared_buffers in the RDS parameter group for Aurora or adjust queries to improve cache efficiency. High checkpoint activity suggests tuning `checkpoint_timeout` or `max_wal_size`.\n====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nFor AWS RDS Aurora, shared_buffers and checkpoint settings are managed via the parameter group. Use AWS Console to adjust these parameters.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
