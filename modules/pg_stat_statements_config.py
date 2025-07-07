def run_pg_stat_statements_config(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Checks if the pg_stat_statements extension is enabled and properly configured,
    which is essential for detailed query performance analysis.
    """
    adoc_content = ["=== pg_stat_statements Configuration", "Checks if pg_stat_statements is enabled and properly configured for query analysis."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("pg_stat_statements configuration queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT name, setting, unit, short_desc FROM pg_settings WHERE name LIKE 'pg_stat_statements.%' ORDER BY name;")
        adoc_content.append("SELECT setting FROM pg_settings WHERE name = 'shared_preload_libraries';")
        adoc_content.append("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements');")
        adoc_content.append("----")

    queries = [
        (
            "pg_stat_statements Settings", 
            "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name LIKE 'pg_stat_statements.%' ORDER BY name;", 
            True, # Always attempt to query settings
            "pg_stat_statements_settings" # Data key
        )
    ]

    # Check if pg_stat_statements is in shared_preload_libraries
    preload_libs_query = "SELECT setting FROM pg_settings WHERE name = 'shared_preload_libraries';"
    formatted_preload_libs, raw_preload_libs = execute_query(preload_libs_query, is_check=True, return_raw=True)
    is_preloaded = 'pg_stat_statements' in str(raw_preload_libs).lower() # Convert to string for robust check

    # Check if pg_stat_statements extension is actually created in the current DB
    extension_exists_query = "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements');"
    formatted_ext_exists, raw_ext_exists = execute_query(extension_exists_query, is_check=True, return_raw=True)
    is_extension_created = (raw_ext_exists == 't') # True if extension is created

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = None # No named parameters in this query
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    # Provide a combined status for pg_stat_statements
    adoc_content.append("\n=== pg_stat_statements Status Summary\n")
    if is_preloaded and is_extension_created:
        adoc_content.append("[NOTE]\n====\n"
                           "The `pg_stat_statements` extension is **fully enabled** and loaded. "
                           "Ensure its configuration parameters (e.g., `pg_stat_statements.track`) are set appropriately for your monitoring needs.\n"
                           "====\n")
        structured_data["extension_status"] = {"status": "enabled", "note": "pg_stat_statements extension is fully enabled."}
    elif is_preloaded and not is_extension_created:
        adoc_content.append("[IMPORTANT]\n====\n"
                           "The `pg_stat_statements` extension is **preloaded but NOT created** in this database. "
                           "It needs to be created to collect query statistics.\n\n"
                           "To fully enable it:\n"
                           "1. Connect to your database.\n"
                           "2. Run `CREATE EXTENSION pg_stat_statements;`\n"
                           "====\n")
        structured_data["extension_status"] = {"status": "preloaded_not_created", "note": "pg_stat_statements preloaded but not created in DB."}
    elif not is_preloaded and is_extension_created:
        adoc_content.append("[IMPORTANT]\n====\n"
                           "The `pg_stat_statements` extension is **created but NOT preloaded**. "
                           "It will not collect statistics unless it's added to `shared_preload_libraries` and the database is restarted.\n\n"
                           "To fully enable it:\n"
                           "1. Add `pg_stat_statements` to `shared_preload_libraries` in `postgresql.conf` (or your RDS/Aurora parameter group).\n"
                           "2. Restart the PostgreSQL service/instance.\n"
                           "====\n")
        structured_data["extension_status"] = {"status": "created_not_preloaded", "note": "pg_stat_statements created but not preloaded."}
    else:
        adoc_content.append("[IMPORTANT]\n====\n"
                           "The `pg_stat_statements` extension is **NOT currently installed or enabled** in this database. "
                           "This is crucial for detailed query performance analysis. "
                           "Without it, sections like 'Top Queries by Execution Time' and 'Top CPU-Intensive Queries' will not provide data.\n\n"
                           "To enable it:\n"
                           "1. Add `pg_stat_statements` to `shared_preload_libraries` in `postgresql.conf` (or your RDS/Aurora parameter group).\n"
                           "2. Restart the PostgreSQL service/instance.\n"
                           "3. Run `CREATE EXTENSION pg_stat_statements;` in your database.\n"
                           "====\n")
        structured_data["extension_status"] = {"status": "disabled", "note": "pg_stat_statements extension is not installed or enabled."}

    adoc_content.append("[TIP]\n====\n"
                   "Proper configuration of `pg_stat_statements` is vital for capturing comprehensive query metrics. "
                   "Adjust `pg_stat_statements.max` to ensure enough statements are tracked, and `pg_stat_statements.track` to `all` for full visibility. "
                   "Regularly reset statistics (`pg_stat_statements_reset()`) for focused analysis periods.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora supports `pg_stat_statements`. Configure it via the DB cluster parameter group. "
                       "Query data from `pg_stat_statements` is also surfaced in Amazon RDS Performance Insights, providing a graphical interface for analysis.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

