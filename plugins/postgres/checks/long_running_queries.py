from plugins.postgres.utils.postgresql_version_compatibility import get_postgresql_version, get_long_running_query

def run_long_running_queries(connector, settings):
    """
    Identifies queries that are currently running for an extended period, which could indicate performance bottlenecks or locking issues.
    """
    adoc_content = ["=== Actively Long-Running Queries", "Shows queries currently in an active state for longer than the defined threshold. These can indicate performance issues or resource contention.\n"]
    structured_data = {}

    try:
        # Get the version-specific query for long-running transactions
        long_running_query = get_long_running_query(connector.cursor, connector.execute_query)
        
        if settings.get('show_qry') == 'true':
            adoc_content.append("Long-running queries query:")
            adoc_content.append("[,sql]\n----")
            # Replace placeholder for display
            display_query = long_running_query.replace('%(long_running_query_seconds)s', str(settings.get('long_running_query_seconds', 60)))
            adoc_content.append(display_query)
            adoc_content.append("----")

        params_for_query = {'long_running_query_seconds': settings.get('long_running_query_seconds', 60)}
        formatted_result, raw_result = connector.execute_query(long_running_query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["long_running_queries"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nNo queries found running longer than the configured threshold of `%(long_running_query_seconds)s` seconds.\n====\n" % params_for_query)
            structured_data["long_running_queries"] = {"status": "success", "data": []}
        else:
            adoc_content.append(f"[IMPORTANT]\n====\nThe following queries have been running for more than `{params_for_query['long_running_query_seconds']}` seconds. Immediate investigation is recommended.\n====\n")
            adoc_content.append(formatted_result)
            structured_data["long_running_queries"] = {"status": "success", "data": raw_result}

    except Exception as e:
        error_msg = f"Failed during long-running query analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["long_running_queries"] = {"status": "error", "details": error_msg}

    adoc_content.append("\n[TIP]\n====\nLong-running queries can hold locks, consume resources, and block other processes. Use `EXPLAIN` to analyze their query plan. Check the `wait_event` and `wait_event_type` columns to see if the query is blocked on I/O, a lock, or another resource. For `idle in transaction` queries, investigate the application to find and fix unclosed transactions.\n====\n")
    
    return "\n".join(adoc_content), structured_data
