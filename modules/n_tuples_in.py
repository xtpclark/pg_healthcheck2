import json # Import json for structured data output, if needed for debugging/logging

def run_tuples_in(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes tables with high insert rates and associated queries, which may
    contribute to CPU and IOPS usage.
    """
    adoc_content = ["=== High Tuple Write Queries", "Identifies tables with high insert rates and associated queries, which may contribute to CPU and IOPS usage."]
    structured_data = {} # Dictionary to hold structured findings for this module

    # Get PostgreSQL version
    pg_version_query = "SHOW server_version_num;"
    _, raw_pg_version = execute_query(pg_version_query, is_check=True, return_raw=True)
    pg_version_num = int(raw_pg_version) # e.g., 170000 for PG 17

    # Determine if it's PostgreSQL 14 or newer (version number 140000 and above)
    is_pg14_or_newer = pg_version_num >= 140000

    # Get the configurable threshold for high tuple inserts, default to 1,000,000
    min_tup_ins_threshold = settings.get('min_tup_ins_threshold', 1000000)
    
    if settings['show_qry'] == 'true':
        adoc_content.append("High tuple write queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(f"SELECT schemaname||'.'||relname AS table_name, n_tup_ins, n_dead_tup, last_autovacuum, autovacuum_count FROM pg_stat_user_tables WHERE n_tup_ins > {min_tup_ins_threshold} ORDER BY n_tup_ins DESC LIMIT %(limit)s;")
        
        if is_pg14_or_newer:
            # For PG14+, blk_read_time and blk_write_time are not in pg_stat_statements
            adoc_content.append("SELECT trim(regexp_replace(query, '\\s+',' ','g'))::varchar(100) AS query, calls, total_exec_time, mean_exec_time, temp_blks_written FROM pg_stat_statements WHERE LOWER(query) LIKE '%%{table_name_placeholder}%%' ORDER BY calls DESC LIMIT %(limit)s;")
        else:
            # For older versions, keep original query
            adoc_content.append("SELECT trim(regexp_replace(query, '\\s+',' ','g'))::varchar(100) AS query, calls, total_exec_time, mean_exec_time, rows FROM pg_stat_statements WHERE LOWER(query) LIKE '%%{table_name_placeholder}%%' ORDER BY calls DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    # Query to find tables with high insert rates (top N based on row_limit)
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
        adoc_content.append("[NOTE]\n====\nNo results returned.\n====\n")
        structured_data["high_insert_tables"] = {"status": "success", "data": []}
    else:
        adoc_content.append(formatted_high_insert_tables_result)
        structured_data["high_insert_tables"] = {"status": "success", "data": raw_high_insert_tables_result}
        
        # If there are high insert tables, proceed to find associated queries
        high_insert_table_names = [row['table_name'] for row in raw_high_insert_tables_result]
        structured_data["associated_queries_by_table"] = {} # To store queries per table

        if settings['has_pgstat'] == 't':
            for table_name in high_insert_table_names:
                # Use %(table_name_lower)s for the LIKE clause to avoid f-string issues with %
                if is_pg14_or_newer:
                    # For PG14+, blk_read_time and blk_write_time are not in pg_stat_statements
                    query_for_table = """
                        SELECT trim(regexp_replace(query, '\\s+',' ','g'))::varchar(100) AS query, calls,
                               total_exec_time, mean_exec_time, temp_blks_written
                        FROM pg_stat_statements
                        WHERE LOWER(query) LIKE %(table_name_pattern)s
                        ORDER BY calls DESC LIMIT %(limit)s;
                    """
                else:
                    # For older versions, keep original query
                    query_for_table = """
                        SELECT trim(regexp_replace(query, '\\s+',' ','g'))::varchar(100) AS query, calls,
                               total_exec_time, mean_exec_time, temp_blks_written, blk_read_time, blk_write_time
                        FROM pg_stat_statements
                        WHERE LOWER(query) LIKE %(table_name_pattern)s
                        ORDER BY calls DESC LIMIT %(limit)s;
                    """
                params_for_query_table = {
                    'limit': settings['row_limit'],
                    'table_name_pattern': f'%{table_name.lower()}%' # Pattern for LIKE clause
                }
                
                adoc_content.append(f"\nQueries Writing to Table: {table_name}")
                formatted_query_result, raw_query_result = execute_query(
                    query_for_table, params=params_for_query_table, return_raw=True
                )
                
                if "[ERROR]" in formatted_query_result:
                    adoc_content.append(formatted_query_result)
                    structured_data["associated_queries_by_table"][table_name] = {"status": "error", "details": raw_query_result}
                elif not raw_query_result:
                    adoc_content.append("[NOTE]\n====\nNo queries found writing to this table.\n====\n")
                    structured_data["associated_queries_by_table"][table_name] = {"status": "success", "data": []}
                else:
                    adoc_content.append(formatted_query_result)
                    structured_data["associated_queries_by_table"][table_name] = {"status": "success", "data": raw_query_result}
        else:
            adoc_content.append("\nQueries Writing to Tables (pg_stat_statements not installed)")
            adoc_content.append("[NOTE]\n====\npg_stat_statements extension is not installed or enabled. Install pg_stat_statements to analyze queries writing to tables.\n====\n")
            structured_data["associated_queries_by_table"] = {"status": "warning", "note": "pg_stat_statements not installed."}

    adoc_content.append("[TIP]\n====\n"
                   "High tuple insert rates can lead to increased CPU usage, IOPS, and table bloat. "
                   "Identify the queries responsible for high write activity and optimize them, especially if they involve large temporary blocks written or significant block I/O. "
                   "Consider batching inserts, using `COPY` command for bulk loads, or optimizing indexing strategies for write-heavy tables. "
                   "For Aurora, high write activity directly impacts `WriteIOPS` and `CPUUtilization` metrics in CloudWatch.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora's storage layer is optimized for high write throughput, but excessive write activity can still saturate instance CPU or network. "
                       "Monitor `WriteIOPS`, `CPUUtilization`, and `DatabaseConnections` in CloudWatch. "
                       "Ensure `pg_stat_statements` is enabled to gain insights into top write queries.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
