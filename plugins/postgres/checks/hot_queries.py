from plugins.postgres.utils.postgresql_version_compatibility import get_postgresql_version, get_pg_stat_statements_query, validate_postgresql_version

def run_hot_queries(connector, settings):
    """
    Identifies "hot" queries based on their high number of shared buffer hits,
    indicating frequently accessed data that is ideal for memory caching.
    """
    adoc_content = ["=== 'Hot' Queries (by Buffer Hits)", "Identifies frequently executed queries that heavily access cached data (`shared_buffers`). These are often critical, high-throughput queries in your application.\n"]
    structured_data = {}

    try:
        if settings.get('has_pgstat') != 't':
            adoc_content.append("[NOTE]\n====\n`pg_stat_statements` extension is not enabled. Analysis cannot be performed.\n====\n")
            structured_data["hot_queries"] = {"status": "not_applicable", "reason": "pg_stat_statements not enabled."}
            return "\n".join(adoc_content), structured_data
            
        compatibility = get_postgresql_version(connector.cursor, connector.execute_query)
        is_supported, error_msg = validate_postgresql_version(compatibility)
        if not is_supported:
            raise ValueError(error_msg)

        # Query to find queries with the most buffer hits
        hot_queries_query = get_pg_stat_statements_query(compatibility, 'standard') + " ORDER BY shared_blks_hit DESC LIMIT %(limit)s;"
        
        if settings.get('show_qry') == 'true':
            adoc_content.append("Hot queries query:")
            adoc_content.append("[,sql]\n----")
            adoc_content.append(hot_queries_query % {'limit': settings.get('row_limit', 10)})
            adoc_content.append("----")

        params_for_query = {'limit': settings.get('row_limit', 10)}
        formatted_result, raw_result = connector.execute_query(hot_queries_query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["hot_queries"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nNo significant hot queries found in `pg_stat_statements`.\n====\n")
            structured_data["hot_queries"] = {"status": "success", "data": []}
        else:
            adoc_content.append(formatted_result)
            structured_data["hot_queries"] = {"status": "success", "data": raw_result}

    except Exception as e:
        error_msg = f"Failed during hot queries analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["hot_queries"] = {"status": "error", "details": error_msg}

    adoc_content.append("\n[TIP]\n====\nQueries with high `shared_blks_hit` are your most frequently accessed data paths. Ensure these queries have optimal indexes and that `shared_buffers` is adequately sized to keep this 'hot' data in memory, minimizing disk I/O.\n====\n")
    
    return "\n".join(adoc_content), structured_data
