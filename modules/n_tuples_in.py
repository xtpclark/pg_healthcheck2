def run_tuples_in(cursor, settings, execute_query, execute_pgbouncer):
    """
    Analyzes tables with high insert rates and associated queries, which may
    contribute to CPU and IOPS usage.
    """
    content = ["=== High Tuple Write Queries", "Identifies tables with high insert rates and associated queries, which may contribute to CPU and IOPS usage."]

    # Get the configurable threshold for high tuple inserts, default to 1,000,000
    min_tup_ins_threshold = settings.get('min_tup_ins_threshold', 1000000)
    
    if settings['show_qry'] == 'true':
        content.append("High tuple write queries:")
        content.append("[,sql]\n----")
        content.append(f"SELECT schemaname||'.'||relname AS table_name, n_tup_ins, n_dead_tup, last_autovacuum, autovacuum_count FROM pg_stat_user_tables WHERE n_tup_ins > {min_tup_ins_threshold} ORDER BY n_tup_ins DESC LIMIT %(limit)s;")
        content.append("SELECT trim(regexp_replace(query, '\\s+',' ','g'))::varchar(100) AS query, calls, total_exec_time, temp_blks_written, blk_read_time, blk_write_time FROM pg_stat_statements WHERE LOWER(query) LIKE '%%{table_name_placeholder}%%' ORDER BY calls DESC LIMIT %(limit)s;")
        content.append("----")

    # Query to find tables with high insert rates (top N based on row_limit)
    high_insert_tables_query = f"""
        SELECT schemaname||'.'||relname AS table_name, n_tup_ins, n_dead_tup,
               last_autovacuum, autovacuum_count
        FROM pg_stat_user_tables
        WHERE n_tup_ins > {min_tup_ins_threshold}
        ORDER BY n_tup_ins DESC LIMIT %(limit)s;
    """
    
    # Execute the query for high insert tables
    params_for_query = {'limit': settings['row_limit']}
    high_insert_tables_result = execute_query(high_insert_tables_query, params=params_for_query)

    content.append(f"Tables with High Insert Activity (n_tup_ins > {min_tup_ins_threshold})")
    if "[ERROR]" in high_insert_tables_result or "[NOTE]" in high_insert_tables_result:
        content.append(high_insert_tables_result)
    else:
        content.append(high_insert_tables_result)
        
        # If there are high insert tables, proceed to find associated queries
        if "[NOTE]\n====\nNo results returned.\n====\n" not in high_insert_tables_result:
            # Parse the table names from the result (assuming simple table output from execute_query)
            # This parsing is a bit fragile if execute_query output format changes significantly.
            # A more robust solution would involve execute_query returning structured data.
            table_lines = high_insert_tables_result.split('\n')
            # Skip header and footer, extract table names from the second column
            # Example: |schemaname.relname|n_tup_ins|...
            high_insert_table_names = []
            if len(table_lines) > 2: # Check if there's at least a header, separator, and one data row
                for line in table_lines[2:-1]: # Iterate over data rows
                    if line.startswith('|') and '|' in line[1:]: # Ensure it's a data row
                        parts = line.split('|')
                        if len(parts) > 1:
                            table_name = parts[1].strip()
                            high_insert_table_names.append(table_name)

            if settings['has_pgstat'] == 't':
                for table_name in high_insert_table_names:
                    query_for_table = f"""
                        SELECT trim(regexp_replace(query, '\\s+',' ','g'))::varchar(100) AS query, calls,
                               total_exec_time, temp_blks_written, blk_read_time, blk_write_time
                        FROM pg_stat_statements
                        WHERE LOWER(query) LIKE '%%{table_name.lower()}%%'
                        ORDER BY calls DESC LIMIT %(limit)s;
                    """
                    content.append(f"\nQueries Writing to Table: {table_name}")
                    query_result = execute_query(query_for_table, params={'limit': settings['row_limit']})
                    content.append(query_result)
            else:
                content.append("\nQueries Writing to Tables (pg_stat_statements not installed)")
                content.append("[NOTE]\n====\npg_stat_statements extension is not installed or enabled. Install pg_stat_statements to analyze queries writing to tables.\n====\n")
        else:
            content.append("\nQueries Writing to Tables (No high insert tables found)")
            content.append("[NOTE]\n====\nNo tables with high insert activity found to analyze associated queries.\n====\n")

    content.append("[TIP]\n====\n"
                   "High tuple insert rates can lead to increased CPU usage, IOPS, and table bloat. "
                   "Identify the queries responsible for high write activity and optimize them, especially if they involve large temporary blocks written or significant block I/O. "
                   "Consider batching inserts, using `COPY` command for bulk loads, or optimizing indexing strategies for write-heavy tables. "
                   "For Aurora, high write activity directly impacts `WriteIOPS` and `CPUUtilization` metrics in CloudWatch.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora's storage layer is optimized for high write throughput, but excessive write activity can still saturate instance CPU or network. "
                       "Monitor `WriteIOPS`, `CPUUtilization`, and `DatabaseConnections` in CloudWatch. "
                       "Ensure `pg_stat_statements` is enabled to gain insights into top write queries.\n"
                       "====\n")
    
    return "\n".join(content)

