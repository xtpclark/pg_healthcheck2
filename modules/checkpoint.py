def run_checkpoint(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes checkpoint activity to optimize WAL performance and reduce I/O load.
    """
    adoc_content = ["Analyzes checkpoint activity to optimize WAL performance and reduce I/O load.\n"]
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
        adoc_content.append("Checkpoint queries:")
        adoc_content.append("[,sql]\n----")
        # Show checkpoint queries based on PostgreSQL version
        if compatibility['is_pg17_or_newer']:
            adoc_content.append("SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_req, write_time AS checkpoint_write_time, sync_time AS checkpoint_sync_time, buffers_written AS buffers_checkpoint FROM pg_stat_checkpointer;")
        else:
            adoc_content.append("SELECT checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time, buffers_checkpoint FROM pg_stat_bgwriter;")
        adoc_content.append("SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('checkpoint_timeout', 'max_wal_size', 'checkpoint_completion_target') ORDER BY name;")
        adoc_content.append("----")

    queries = []

    if compatibility['is_pg17_or_newer']:
        queries.append(
            (
                "Checkpoint Statistics (PostgreSQL 17+)", 
                "SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_req, write_time AS checkpoint_write_time, sync_time AS checkpoint_sync_time, buffers_written AS buffers_checkpoint FROM pg_stat_checkpointer;", 
                True,
                "checkpoint_statistics" # Data key - keeping consistent key name
            )
        )
    else:
        queries.append(
            (
                "Checkpoint Statistics (PostgreSQL < 17)", 
                "SELECT checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time, buffers_checkpoint FROM pg_stat_bgwriter;", 
                True,
                "checkpoint_statistics" # Data key
            )
        )
    
    queries.append(
        (
            "Checkpoint Configuration", 
            "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('checkpoint_timeout', 'max_wal_size', 'checkpoint_completion_target') ORDER BY name;", 
            True,
            "checkpoint_configuration" # Data key
        )
    )

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        # These queries do not use %(limit)s or %(database)s, so params_for_query will be None.
        params_for_query = None 
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\nHigh checkpoint frequency can increase I/O load. Adjust `checkpoint_timeout` or `max_wal_size` to reduce checkpoint frequency. For Aurora, tune these settings via the RDS parameter group to mitigate CPU and IOPS saturation.\n====\n")
    
    if compatibility['is_pg17_or_newer']:
        adoc_content.append("[NOTE]\n====\nIn PostgreSQL 17 and newer, checkpoint statistics like timed and requested checkpoints, write time, sync time, and buffers written are available in `pg_stat_checkpointer`.\n====\n")

    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nAWS RDS Aurora manages checkpoint settings via the parameter group. Use the AWS Console to adjust `checkpoint_timeout` or `max_wal_size`.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
