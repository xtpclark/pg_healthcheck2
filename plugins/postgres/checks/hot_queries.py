from plugins.postgres.utils.qrylib.hot_queries import get_hot_queries_query

def get_weight():
    """Returns the importance score for this module."""
    # Performance tuning is an important activity.
    return 6

def run_hot_queries(connector, settings):
    """
    Identifies "hot" queries based on their high number of shared buffer hits,
    indicating frequently accessed data that is ideal for memory caching.
    """
    adoc_content = ["=== 'Hot' Queries (by Buffer Hits)", "Identifies frequently executed queries that heavily access cached data (`shared_buffers`). These are often critical, high-throughput queries in your application.\n"]
    structured_data = {}

    try:
        # Pre-check: Ensure pg_stat_statements is available
        if not connector.has_pgstat:
            adoc_content.append("[NOTE]\n====\n`pg_stat_statements` extension is not enabled. Analysis cannot be performed.\n====\n")
            structured_data["hot_queries"] = {"status": "not_applicable", "reason": "pg_stat_statements not enabled."}
            return "\n".join(adoc_content), structured_data
        
        # Pre-check: This analysis is most relevant on newer PG versions
        if connector.version_info.get('major_version', 0) < 13:
            adoc_content.append("[NOTE]\n====\nThis specific analysis is intended for PostgreSQL 13+ and has been skipped.\n====\n")
            structured_data["hot_queries"] = {"status": "skipped", "reason": "Unsupported PostgreSQL version."}
            return "\n".join(adoc_content), structured_data

        query = get_hot_queries_query(connector)
        params = {'limit': settings.get('row_limit', 10)}
        
        if settings.get('show_qry') == 'true':
            adoc_content.append("Hot queries query:")
            adoc_content.append(f"[,sql]\n----\n{query % params}\n----")

        formatted_result, raw_result = connector.execute_query(query, params=params, return_raw=True)
        
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
        structured_data["hot_queries"] = {"status": "error", "details": str(e)}

    adoc_content.append("\n[TIP]\n====\nQueries with high `shared_blks_hit` are your most frequently accessed data paths. Ensure these queries have optimal indexes and that `shared_buffers` is adequately sized to keep this 'hot' data in memory, minimizing disk I/O.\n====\n")
    
    return "\n".join(adoc_content), structured_data
