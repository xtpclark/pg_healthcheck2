def run_vacstat2(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes ongoing vacuum operations, historical vacuum statistics, and suggests tables that may benefit from per-table statistics due to high insert rates.
    """
    adoc_content = ["Analyzes ongoing vacuum operations, historical vacuum statistics, and suggests tables that may benefit from per-table statistics due to high insert rates.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Import version compatibility module
    from .postgresql_version_compatibility import get_postgresql_version, get_vacuum_progress_query, validate_postgresql_version
    
    # Get PostgreSQL version compatibility information
    compatibility = get_postgresql_version(cursor, execute_query)
    
    # Validate PostgreSQL version
    is_supported, error_msg = validate_postgresql_version(compatibility)
    if not is_supported:
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["version_error"] = {"status": "error", "details": error_msg}
        return "\n".join(adoc_content), structured_data

    # Define the autovacuum pattern for LIKE clauses
    autovacuum_pattern = 'autovacuum:%'

    if settings['show_qry'] == 'true':
        adoc_content.append("Vacuum progress and statistics queries:")
        adoc_content.append("[,sql]\n----")
        # Get version-specific vacuum progress query
        vacuum_query = get_vacuum_progress_query(compatibility)
        adoc_content.append(vacuum_query)
        adoc_content.append("SELECT schemaname||'.'||relname AS table_name, autovacuum_count, last_autovacuum, autoanalyze_count, last_autoanalyze FROM pg_stat_user_tables WHERE autovacuum_count > 0 ORDER BY autovacuum_count DESC LIMIT %(limit)s;")
        adoc_content.append(f"SELECT relname, n_tup_ins FROM pg_stat_user_tables WHERE n_tup_ins > {settings.get('min_tup_ins_threshold', 1000000)} AND last_autovacuum IS NOT NULL AND last_autovacuum::date < now()::date ORDER BY n_tup_ins DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    queries = [
        (
            "Ongoing Vacuum Operations", 
            get_vacuum_progress_query(compatibility),
            True, 
            "ongoing_vacuum_operations" # Data key
        ),
        (
            "Historical Vacuum Statistics", 
            "SELECT schemaname||'.'||relname AS table_name, autovacuum_count, last_autovacuum, autoanalyze_count, last_autoanalyze FROM pg_stat_user_tables WHERE autovacuum_count > 0 ORDER BY autovacuum_count DESC LIMIT %(limit)s;", 
            True, 
            "historical_vacuum_statistics" # Data key
        ),
        (
            "Tables Potentially Needing Per-Table Statistics",
            f"SELECT relname AS table_name, n_tup_ins FROM pg_stat_user_tables WHERE n_tup_ins > {settings.get('min_tup_ins_threshold', 1000000)} AND last_autovacuum IS NOT NULL AND last_autovacuum::date < now()::date ORDER BY n_tup_ins DESC LIMIT %(limit)s;",
            True,
            "tables_for_per_table_stats" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = {}
        if '%(database)s' in query:
            params_for_query['database'] = settings['database']
        if '%(limit)s' in query:
            params_for_query['limit'] = settings['row_limit']
        if '%(autovacuum_pattern)s' in query: # Ensure this is handled if query uses it
            params_for_query['autovacuum_pattern'] = autovacuum_pattern

        params_to_pass = params_for_query if params_for_query else None

        formatted_result, raw_result = execute_query(query, params=params_to_pass, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
            
            # Special handling for "Tables Potentially Needing Per-Table Statistics"
            if data_key == "tables_for_per_table_stats" and raw_result:
                adoc_content.append("\n[IMPORTANT]\n====\n"
                                   "The tables listed above may benefit from setting per-table statistics. "
                                   "This will force PostgreSQL to sample more rows when analyzing the table, "
                                   "leading to more accurate query plans. The default is 100, and a value of 300 is often sensible.\n\n"
                                   "Example SQL to apply:\n"
                                   "[,sql]\n----\n"
                                   "ALTER TABLE <table_name> ALTER COLUMN <column_name> SET STATISTICS <value>;\n"
                                   "----\n"
                                   "Consult your query patterns to identify specific columns that are frequently filtered or joined on.\n"
                                   "====\n")
    
    adoc_content.append("[TIP]\n====\n"
                   "Monitor ongoing vacuum operations to ensure they complete without excessive CPU or IOPS usage. "
                   "High `autovacuum_count` may indicate frequent updates; tune `autovacuum_vacuum_threshold` or `autovacuum_vacuum_cost_limit`. "
                   "For Aurora, adjust these settings via the RDS parameter group.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "For AWS RDS Aurora, vacuum performance is influenced by parameter group settings. "
                       "Use AWS Console to adjust autovacuum parameters.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
