from plugins.postgres.utils.postgresql_version_compatibility import get_pg_stat_statements_query

def run_top_queries_by_execution_time(connector, settings):
    """
    Identifies the most resource-intensive queries based on their total cumulative execution time.
    """
    adoc_content = ["=== Top Queries by Total Execution Time", "Identifies queries that consume the most database time overall. Optimizing these queries often yields the largest performance gains.\n"]
    structured_data = {}

    try:
        if settings.get('has_pgstat') != 't':
            adoc_content.append("[NOTE]\n====\n`pg_stat_statements` extension is not enabled. Analysis cannot be performed.\n====\n")
            structured_data["top_queries"] = {"status": "not_applicable", "reason": "pg_stat_statements not enabled."}
            return "\n".join(adoc_content), structured_data
        
        # FIX: Get version info from the connector
        version_info = connector.version_info
        if version_info.get('major_version', 0) < 13:
            raise ValueError(f"PostgreSQL version {version_info.get('version_string', 'Unknown')} is not supported.")

        # FIX: Pass the entire connector to the utility function
        top_queries_query = get_pg_stat_statements_query(connector, 'standard') + " LIMIT %(limit)s;"

        if settings.get('show_qry') == 'true':
            adoc_content.append("Top queries by execution time query:")
            adoc_content.append("[,sql]\n----")
            adoc_content.append(top_queries_query % {'limit': settings.get('row_limit', 10)})
            adoc_content.append("----")

        params_for_query = {'limit': settings.get('row_limit', 10)}
        formatted_result, raw_result = connector.execute_query(top_queries_query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"Top Queries by Execution Time\n{formatted_result}")
            structured_data["top_queries"] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(formatted_result)
            structured_data["top_queries"] = {"status": "success", "data": raw_result}
    
    except Exception as e:
        error_msg = f"Failed during top queries analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["top_queries"] = {"status": "error", "details": error_msg}

    adoc_content.append("\n[TIP]\n====\nQueries with high `total_exec_time` are the primary contributors to database load. Focus optimization efforts here first. Use `EXPLAIN (ANALYZE, BUFFERS)` on these queries to understand their execution plans and identify opportunities for indexing or rewriting.\n====\n")
    
    return "\n".join(adoc_content), structured_data
