def run_pgset(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Retrieves all PostgreSQL configuration settings from pg_settings,
    providing a comprehensive overview of the database's current configuration.
    """
    adoc_content = ["=== All PostgreSQL Settings", "Provides a comprehensive list of all PostgreSQL configuration settings."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("All PostgreSQL settings query:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT name, setting, unit, short_desc, category, context FROM pg_settings ORDER BY category, name;")
        adoc_content.append("----")

    query = "SELECT name, setting, unit, short_desc, category, context FROM pg_settings ORDER BY category, name;"
    
    # No parameters needed for this query
    params_for_query = None 
    
    formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
    
    if "[ERROR]" in formatted_result:
        adoc_content.append(f"All PostgreSQL Settings\n{formatted_result}")
        structured_data["all_settings"] = {"status": "error", "details": raw_result}
    else:
        adoc_content.append("All PostgreSQL Settings")
        adoc_content.append(formatted_result)
        structured_data["all_settings"] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\n"
                   "Reviewing all PostgreSQL settings provides a complete picture of your database's configuration. "
                   "Pay attention to settings related to memory, connections, logging, autovacuum, and WAL. "
                   "Ensure settings are optimized for your workload and hardware, and align with best practices. "
                   "For managed services like RDS/Aurora, most settings are managed via parameter groups.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora uses DB cluster parameter groups and DB parameter groups to manage most PostgreSQL settings. "
                       "Changes to static parameters require a reboot of the DB instance. "
                       "Use the AWS Console to review and modify these parameters. "
                       "Some settings might have different defaults or behaviors specific to Aurora.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

