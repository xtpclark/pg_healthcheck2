def run_datadog_setup(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Checks for Datadog integration setup and relevant PostgreSQL configurations
    that Datadog might monitor.
    Note: This module primarily provides conceptual checks as direct file system
    access or external API calls are not typically allowed from within PostgreSQL.
    """
    adoc_content = ["=== Datadog Monitoring Setup", "Analyzes PostgreSQL configuration relevant to Datadog monitoring."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Datadog setup queries (conceptual):")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("-- Check if pg_stat_statements is enabled (critical for Datadog query metrics)")
        adoc_content.append("SELECT name, setting FROM pg_settings WHERE name = 'shared_preload_libraries';")
        adoc_content.append("-- Check logging settings (Datadog can ingest logs)")
        adoc_content.append("SELECT name, setting FROM pg_settings WHERE name IN ('log_min_duration_statement', 'log_statement', 'log_connections', 'log_disconnections');")
        adoc_content.append("----")

    queries = [
        (
            "Shared Preload Libraries (for pg_stat_statements)", 
            "SELECT name, setting, short_desc FROM pg_settings WHERE name = 'shared_preload_libraries';", 
            True,
            "shared_preload_libraries" # Data key
        ),
        (
            "Key Logging Settings", 
            "SELECT name, setting, short_desc FROM pg_settings WHERE name IN ('log_min_duration_statement', 'log_statement', 'log_connections', 'log_disconnections');", 
            True,
            "logging_settings" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = None # No named parameters in these queries
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\n"
                   "Datadog relies on specific PostgreSQL configurations (like `pg_stat_statements` and detailed logging) to provide comprehensive monitoring. "
                   "Ensure `shared_preload_libraries` includes `pg_stat_statements` for query-level metrics. "
                   "Configure `log_min_duration_statement` to capture slow queries and enable `log_connections`/`log_disconnections` for connection auditing. "
                   "For self-hosted instances, verify the Datadog Agent is running and configured correctly.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "For AWS RDS Aurora, Datadog integrates with CloudWatch and Enhanced Monitoring. "
                       "Ensure Enhanced Monitoring is enabled in RDS. "
                       "Configure `pg_stat_statements` via the DB cluster parameter group for query insights. "
                       "Datadog's agent typically runs outside the DB instance, collecting metrics via CloudWatch APIs and PostgreSQL connections.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

