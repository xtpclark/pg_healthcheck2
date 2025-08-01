from plugins.postgres.utils.qrylib.query_analysis import (
    get_query_workload_summary_query,
    get_top_queries_by_metric_query,
    get_top_queries_by_io_time_query
)

def get_weight():
    """Returns the importance score for this module."""
    return 5

def run_query_analysis(connector, settings):
    """
    Performs a deep analysis of query performance using pg_stat_statements,
    creating a summary and identifying top queries.
    """
    adoc_content = ["=== Deep Query Analysis (pg_stat_statements)"]
    structured_data = {}
    params = {'limit': settings.get('row_limit', 10)}

    if not connector.has_pgstat:
        adoc_content.append("[NOTE]\n====\n`pg_stat_statements` is not enabled. No query analysis is available.\n====\n")
        structured_data["query_analysis"] = {"status": "not_applicable", "reason": "pg_stat_statements not enabled."}
        return "\n".join(adoc_content), structured_data

    try:
        # --- High-level summary ---
        summary_query = get_query_workload_summary_query()
        _, summary_raw = connector.execute_query(summary_query, return_raw=True)
        structured_data["query_workload_summary"] = {"status": "success", "data": summary_raw[0] if summary_raw else {}}
        
        # --- Top Queries by Total Time ---
        adoc_content.append("\n==== Top Queries by Total Execution Time")
        time_query = get_top_queries_by_metric_query(connector, 'time') + " LIMIT %(limit)s;"
        time_formatted, time_raw = connector.execute_query(time_query, params=params, return_raw=True)
        adoc_content.append(time_formatted)
        structured_data["top_by_time"] = {"status": "success", "data": time_raw}

        # --- Top Queries by Calls ---
        adoc_content.append("\n==== Top Queries by Call Count")
        calls_query = get_top_queries_by_metric_query(connector, 'calls') + " LIMIT %(limit)s;"
        calls_formatted, calls_raw = connector.execute_query(calls_query, params=params, return_raw=True)
        adoc_content.append(calls_formatted)
        structured_data["top_by_calls"] = {"status": "success", "data": calls_raw}

        # --- Top Queries by I/O ---
        adoc_content.append("\n==== Top Queries by I/O Wait Time")
        io_query = get_top_queries_by_io_time_query(connector) + " LIMIT %(limit)s;"
        io_formatted, io_raw = connector.execute_query(io_query, params=params, return_raw=True)

        has_io_data = any(item.get('total_io_time', 0) > 0 for item in io_raw) if isinstance(io_raw, list) else False

        if not has_io_data:
            adoc_content.append("[CAUTION]\n====\n**All I/O times are zero.** This usually means the `track_io_timing` parameter is disabled. Enable it to collect I/O statistics.\n====\n")
        
        adoc_content.append(io_formatted)
        structured_data["top_by_io"] = {"status": "success", "data": io_raw}

    except Exception as e:
        error_msg = f"[ERROR]\n====\nCould not perform query analysis: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["query_analysis"] = {"status": "error", "error": str(e)}

    return "\n".join(adoc_content), structured_data
