import json

def run_high_insert_tables(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Identifies tables with high insert rates based on n_tup_ins from pg_stat_user_tables.
    """
    adoc_content = ["=== Tables with High Insert Activity", "Identifies tables experiencing a high volume of new row insertions."]
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

    # Get the configurable threshold for high tuple inserts, default to 1,000,000
    min_tup_ins_threshold = settings.get('min_tup_ins_threshold', 1000000)
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Query for tables with high insert rates:")
        adoc_content.append("[,sql]\n----")
        # Show query for high insert tables (pg_stat_user_tables is stable across versions)
        adoc_content.append(f"SELECT schemaname||'.'||relname AS table_name, n_tup_ins, n_dead_tup, last_autovacuum, autovacuum_count FROM pg_stat_user_tables WHERE n_tup_ins > {min_tup_ins_threshold} ORDER BY n_tup_ins DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    # Query to find tables with high insert rates (top N based on row_limit)
    # pg_stat_user_tables is stable across PostgreSQL versions
    high_insert_tables_query = f"""
        SELECT schemaname||'.'||relname AS table_name, n_tup_ins, n_dead_tup,
               last_autovacuum, autovacuum_count
        FROM pg_stat_user_tables
        WHERE n_tup_ins > {min_tup_ins_threshold}
        ORDER BY n_tup_ins DESC LIMIT %(limit)s;
    """
    
    # Execute the query for high insert tables, requesting raw data
    params_for_query = {'limit': settings['row_limit']}
    formatted_high_insert_tables_result, raw_high_insert_tables_result = execute_query(
        high_insert_tables_query, params=params_for_query, return_raw=True
    )

    adoc_content.append(f"Tables with High Insert Activity (n_tup_ins > {min_tup_ins_threshold})")
    if "[ERROR]" in formatted_high_insert_tables_result:
        adoc_content.append(formatted_high_insert_tables_result)
        structured_data["high_insert_tables"] = {"status": "error", "details": raw_high_insert_tables_result}
    elif not raw_high_insert_tables_result: # Check if raw_result is empty list (no results)
        adoc_content.append("[NOTE]\n====\nNo tables found with high insert activity.\n====\n")
        structured_data["high_insert_tables"] = {"status": "success", "data": []}
    else:
        adoc_content.append(formatted_high_insert_tables_result)
        structured_data["high_insert_tables"] = {"status": "success", "data": raw_high_insert_tables_result}
        
    adoc_content.append("[TIP]\n====\n"
                   "Tables with consistently high insert rates can be a source of increased CPU usage, IOPS, and table bloat. "
                   "Ensure that `autovacuum` is aggressively configured for such tables to prevent excessive dead tuple accumulation. "
                   "Consider optimizing application-side insert patterns, such as batching inserts or using `COPY` for bulk data loading, to reduce transaction overhead. "
                   "For Aurora, high insert activity directly impacts `WriteIOPS` and `CPUUtilization` metrics in CloudWatch.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora's storage layer is optimized for high write throughput, but excessive insert activity can still saturate instance CPU or network. "
                       "Monitor `WriteIOPS`, `CPUUtilization`, and `DatabaseConnections` in CloudWatch. "
                       "Regularly analyze `pg_stat_user_tables` to identify and address tables with high `n_tup_ins`.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
