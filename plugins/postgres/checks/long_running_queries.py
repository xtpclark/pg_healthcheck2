# No longer need to import custom functions for versioning
# from plugins.postgres.utils.postgresql_version_compatibility import get_postgresql_version, get_long_running_query

def run_long_running_queries(connector, settings):
    """
    Identifies queries that are currently running for an extended period, which could indicate performance bottlenecks or locking issues.
    """
    adoc_content = ["=== Actively Long-Running Queries", "Shows queries currently in an active state for longer than the defined threshold. These can indicate performance issues or resource contention.\n"]
    structured_data = {}

    try:
        version_info = connector.version_info
        # The wait_event columns were added in PostgreSQL 9.6, but checking for 10+ is safer and covers most modern systems.
        wait_event_column = "wait_event_type, wait_event" if version_info.get('major_version', 0) >= 10 else "'N/A' AS wait_event_type, 'N/A' AS wait_event"
        
        # FIX: Removed the erroneous backslash before the triple quotes
        long_running_query = f"""
            SELECT
                pid,
                usename,
                application_name,
                client_addr,
                state,
                {wait_event_column},
                age(clock_timestamp(), query_start) AS duration,
                query
            FROM pg_stat_activity
            WHERE state <> 'idle'
              AND (backend_type = 'client backend' OR backend_type IS NULL)
              AND age(clock_timestamp(), query_start) > (%(long_running_query_seconds)s * interval '1 second')
            ORDER BY duration DESC;
        """
        
        params_for_query = {'long_running_query_seconds': settings.get('long_running_query_seconds', 60)}

        if settings.get('show_qry') == 'true':
            adoc_content.append("Long-running queries query:")
            adoc_content.append("[,sql]\n----")
            adoc_content.append(long_running_query % params_for_query)
            adoc_content.append("----")
        
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
