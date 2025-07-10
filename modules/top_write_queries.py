import json

def run_top_write_queries(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Identifies top write-intensive queries from pg_stat_statements,
    adapting to PostgreSQL versions.
    """
    adoc_content = ["=== Top Write-Intensive Queries", "Identifies queries that generate significant write activity."]
    structured_data = {} # Dictionary to hold structured findings for this module

    # Get PostgreSQL version
    pg_version_query = "SHOW server_version_num;"
    _, raw_pg_version = execute_query(pg_version_query, is_check=True, return_raw=True)
    pg_version_num = int(raw_pg_version) # e.g., 170000 for PG 17

    # Determine if it's PostgreSQL 14 or newer (version number 140000 and above)
    is_pg14_or_newer = pg_version_num >= 140000

    # Define a common query prefix for replacing newlines and truncating
    # NEW: Also replace pipe characters to prevent table formatting issues
    query_select_prefix = "REPLACE(REPLACE(LEFT(query, 150), E'\\n', ' '), '|', ' ') || '...' AS query"

    if settings['show_qry'] == 'true':
        adoc_content.append("Query for top write-intensive queries:")
        adoc_content.append("[,sql]\n----")
        if is_pg14_or_newer:
            # For PG14+, use columns available in pg_stat_statements for write activity
            adoc_content.append(f"SELECT {query_select_prefix}, calls, total_exec_time, mean_exec_time, rows, shared_blks_written, local_blks_written, temp_blks_written, wal_bytes FROM pg_stat_statements ORDER BY rows DESC, wal_bytes DESC LIMIT %(limit)s;")
        else:
            # For older versions, use columns that might have been present (adjust if specific columns are missing for older versions)
            # Note: blk_read_time/blk_write_time were not consistently in pg_stat_statements across all older versions.
            # We're focusing on common ones for write activity.
            adoc_content.append(f"SELECT {query_select_prefix}, calls, total_exec_time, mean_exec_time, rows, temp_blks_written, blk_read_time, blk_write_time FROM pg_stat_statements ORDER BY rows DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    # Query for top write-intensive queries
    if settings['has_pgstat'] == 't':
        if is_pg14_or_newer:
            query_for_write_queries = f"""
                SELECT {query_select_prefix}, calls, total_exec_time, mean_exec_time, rows,
                       shared_blks_written, local_blks_written, temp_blks_written, wal_bytes
                FROM pg_stat_statements
                ORDER BY rows DESC, wal_bytes DESC LIMIT %(limit)s;
            """
        else:
            # For older versions (pre-PG14)
            # Note: blk_read_time and blk_write_time were not universally present in pg_stat_statements
            # in all pre-PG14 versions. This query might still fail on very old versions.
            query_for_write_queries = f"""
                SELECT {query_select_prefix}, calls, total_exec_time, mean_exec_time, rows,
                       temp_blks_written, blk_read_time, blk_write_time
                FROM pg_stat_statements
                ORDER BY rows DESC LIMIT %(limit)s;
            """
        
        params_for_query = {'limit': settings['row_limit']}
        
        formatted_result, raw_result = execute_query(
            query_for_write_queries, params=params_for_query, return_raw=True
        )
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["top_write_queries"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nNo write-intensive queries found in `pg_stat_statements`.\n====\n")
            structured_data["top_write_queries"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[NOTE]\n====\nThis section lists the top queries that generate significant write activity (based on rows affected, WAL bytes, or blocks written). Due to query normalization in `pg_stat_statements`, these queries may not directly show the table names if they are parameterized. Manual inspection of the query text is recommended for correlation.\n====\n")
            adoc_content.append(formatted_result)
            structured_data["top_write_queries"] = {"status": "success", "data": raw_result}
    else:
        adoc_content.append("[NOTE]\n====\n`pg_stat_statements` extension is not installed or enabled. Write-intensive query analysis cannot be performed.\n====\n")
        structured_data["top_write_queries"] = {"status": "not_applicable", "reason": "pg_stat_statements not enabled."}

    adoc_content.append("[TIP]\n====\n"
                   "Optimizing write-intensive queries is crucial for overall database performance. "
                   "Look for opportunities to batch operations, use `COPY` for bulk loads, or improve indexing strategies on heavily written tables. "
                   "Analyze query plans (`EXPLAIN (ANALYZE, BUFFERS)`) to understand resource consumption. "
                   "For Aurora, excessive write activity directly impacts `WriteIOPS` and `CPUUtilization` metrics in CloudWatch.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora's storage layer is optimized for high write throughput, but excessive write activity can still saturate instance CPU or network. "
                       "Monitor `WriteIOPS`, `CPUUtilization`, and `DatabaseConnections` in CloudWatch. "
                       "Ensure `pg_stat_statements` is enabled to gain insights into top write queries.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
