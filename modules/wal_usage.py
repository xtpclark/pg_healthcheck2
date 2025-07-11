def run_wal_usage(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes Write-Ahead Log (WAL) usage and archiving status to optimize performance and ensure reliable recovery.
    """
    adoc_content = ["Analyzes Write-Ahead Log (WAL) usage and archiving status to optimize performance and ensure reliable recovery.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("WAL usage queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT archived_count, failed_count, last_archived_wal, last_archived_time, last_failed_wal, last_failed_time FROM pg_stat_archiver;")
        adoc_content.append("SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('wal_level', 'archive_mode', 'archive_timeout', 'max_wal_size') ORDER BY name;")
        adoc_content.append("----")

    queries = [
        (
            "WAL Archiving Status", 
            "SELECT archived_count, failed_count, last_archived_wal, last_archived_time, last_failed_wal, last_failed_time FROM pg_stat_archiver;", 
            True,
            "wal_archiving_status" # Data key
        ),
        (
            "WAL Configuration Settings", 
            "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('wal_level', 'archive_mode', 'archive_timeout', 'max_wal_size') ORDER BY name;", 
            True,
            "wal_configuration_settings" # Data key
        )
    ]

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
    
    adoc_content.append("[TIP]\n====\nEnsure archive_mode is 'on' and monitor failed_count for archiving issues. Adjust archive_timeout to balance WAL segment creation and I/O load. For Aurora, verify WAL archiving setup via the RDS parameter group and CloudWatch metrics (e.g., WriteIOPS) to address CPU saturation.\n====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nAWS RDS Aurora manages WAL archiving internally. Use the AWS Console to configure archive settings and monitor WriteIOPS in CloudWatch for performance insights.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
