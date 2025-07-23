from plugins.postgres.utils.postgresql_version_compatibility import get_postgresql_version, validate_postgresql_version

def run_top_write_queries(connector, settings):
    """
    Identifies top write-intensive queries from pg_stat_statements, adapting to PostgreSQL versions.
    """
    adoc_content = ["=== Top Write-Intensive Queries", "Identifies queries generating significant write activity (high WAL generation or disk writes), which can impact I/O performance.\n"]
    structured_data = {}

    try:
        if settings.get('has_pgstat') != 't':
            adoc_content.append("[NOTE]\n====\n`pg_stat_statements` extension is not enabled. Analysis cannot be performed.\n====\n")
            structured_data["top_write_queries"] = {"status": "not_applicable", "reason": "pg_stat_statements not enabled."}
            return "\n".join(adoc_content), structured_data

        compatibility = get_postgresql_version(connector.cursor, connector.execute_query)
        is_supported, error_msg = validate_postgresql_version(compatibility)
        if not is_supported:
            raise ValueError(error_msg)

        # Sanitize query text for safe AsciiDoc table display
        query_select_prefix = "REPLACE(REPLACE(LEFT(query, 150), E'\\n', ' '), '|', ' ') || '...' AS query"
        
        if compatibility.get('is_pg14_or_newer'):
            query_for_write_queries = f"""
                SELECT {query_select_prefix}, calls, total_exec_time, mean_exec_time, rows,
                       shared_blks_written, local_blks_written, temp_blks_written, wal_bytes
                FROM pg_stat_statements
                ORDER BY wal_bytes DESC, shared_blks_written DESC
                LIMIT %(limit)s;
            """
        else:
            # Fallback for older versions without wal_bytes
            query_for_write_queries = f"""
                SELECT {query_select_prefix}, calls, total_time AS total_exec_time, mean_time AS mean_exec_time, rows,
                       shared_blks_written, local_blks_written, temp_blks_written
                FROM pg_stat_statements
                ORDER BY shared_blks_written DESC, rows DESC
                LIMIT %(limit)s;
            """

        if settings.get('show_qry') == 'true':
            adoc_content.append("Top write-intensive queries query:")
            adoc_content.append("[,sql]\n----")
            adoc_content.append(query_for_write_queries % {'limit': settings.get('row_limit', 10)})
            adoc_content.append("----")
        
        params_for_query = {'limit': settings.get('row_limit', 10)}
        formatted_result, raw_result = connector.execute_query(query_for_write_queries, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["top_write_queries"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nNo significant write-intensive queries found in `pg_stat_statements`.\n====\n")
            structured_data["top_write_queries"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[NOTE]\n====\nThis section lists queries that generate the most write activity. Due to normalization, table names may be parameterized. Manual inspection is recommended for correlation.\n====\n")
            adoc_content.append(formatted_result)
            structured_data["top_write_queries"] = {"status": "success", "data": raw_result}

    except Exception as e:
        error_msg = f"Failed during top write queries analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["top_write_queries"] = {"status": "error", "details": error_msg}

    adoc_content.append("\n[TIP]\n====\nOptimizing write-intensive queries is crucial for I/O performance. Look for opportunities to batch `INSERT`/`UPDATE` operations, use `COPY` for bulk loads, or remove unnecessary indexes on heavily written tables.\n====\n")
    
    return "\n".join(adoc_content), structured_data
