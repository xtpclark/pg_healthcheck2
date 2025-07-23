import json

def run_top_write_queries(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Identifies top write-intensive queries from pg_stat_statements,
    adapting to PostgreSQL versions.
    """
    adoc_content = ["=== Top Write-Intensive Queries", "Identifies queries that generate significant write activity."]
    structured_data = {} # Dictionary to hold structured findings for this module

    # Import version compatibility module
    from .postgresql_version_compatibility import get_postgresql_version, get_pg_stat_statements_query, validate_postgresql_version
    
    # Get PostgreSQL version compatibility information
    compatibility = get_postgresql_version(cursor, execute_query)
    
    # Validate PostgreSQL version
    is_supported, error_msg = validate_postgresql_version(compatibility)
    if not is_supported:
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["version_error"] = {"status": "error", "details": error_msg}
        return "\n".join(adoc_content), structured_data

    # Define a common query prefix for replacing newlines and truncating
    # NEW: Also replace pipe characters to prevent table formatting issues
    query_select_prefix = "REPLACE(REPLACE(LEFT(query, 150), E'\\n', ' '), '|', ' ') || '...' AS query"

    if settings['show_qry'] == 'true':
        adoc_content.append("Query for top write-intensive queries:")
        adoc_content.append("[,sql]\n----")
        # Show version-specific write activity query
        if compatibility['is_pg14_or_newer']:
            adoc_content.append(f"SELECT {query_select_prefix}, calls, total_exec_time, mean_exec_time, rows, shared_blks_written, local_blks_written, temp_blks_written, wal_bytes FROM pg_stat_statements ORDER BY rows DESC, wal_bytes DESC LIMIT %(limit)s;")
        else:
            adoc_content.append(f"SELECT {query_select_prefix}, calls, total_time, mean_time, rows, temp_blks_written FROM pg_stat_statements ORDER BY rows DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    # Query for top write-intensive queries
    if settings['has_pgstat'] == 't':
        # Build version-specific query
        if compatibility['is_pg14_or_newer']:
            query_for_write_queries = f"""
                SELECT {query_select_prefix}, calls, total_exec_time, mean_exec_time, rows,
                       shared_blks_written, local_blks_written, temp_blks_written, wal_bytes
                FROM pg_stat_statements
                ORDER BY rows DESC, wal_bytes DESC LIMIT %(limit)s;
            """
        else:
            query_for_write_queries = f"""
                SELECT {query_select_prefix}, calls, total_time, mean_time, rows,
                       temp_blks_written
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
