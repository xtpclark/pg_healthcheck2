from plugins.postgres.utils.qrylib.pg_stat_statements import (get_pg_stat_statements_query)
from plugins.postgres.utils.qrylib.query_analysis import (get_top_queries_by_io_time_query)

def run_query_analysis(connector, settings):
    """
    Performs a deep analysis of query performance using pg_stat_statements,
    creating a summary and identifying top queries.
    """
    adoc_content = ["=== Deep Query Analysis (pg_stat_statements)"]
    # This will hold all data for this module, including the new summary
    structured_data = {}
    params = {'limit': settings.get('row_limit', 10)}

    if not connector.has_pgstat:
        adoc_content.append("[NOTE]\n====\n`pg_stat_statements` is not enabled. No query analysis is available.\n====\n")
        structured_data["query_analysis"] = {"status": "not_applicable", "reason": "pg_stat_statements not enabled."}
        return "\n".join(adoc_content), structured_data

    try:
        # --- NEW: Create a high-level summary for AI analysis ---
        summary_query = """
            SELECT
                COUNT(*) as total_queries_tracked,
                SUM(calls) as total_calls,
                SUM(total_exec_time) as total_execution_time_all_queries_ms,
                SUM(rows) as total_rows_processed
            FROM pg_stat_statements;
        """
        _, summary_raw = connector.execute_query(summary_query, return_raw=True)
        # Store the summary. We use summary_raw[0] because it returns a list with one dict.
        structured_data["query_workload_summary"] = {"status": "success", "data": summary_raw[0] if summary_raw else {}}
        
        # --- Top Queries by Total Time (for AsciiDoc report) ---
        adoc_content.append("==== Top Queries by Total Execution Time")
        time_query = get_pg_stat_statements_query(connector, 'total_time') + " LIMIT %(limit)s;"
        time_formatted, time_raw = connector.execute_query(time_query, params=params, return_raw=True)
        adoc_content.append(time_formatted)
        structured_data["top_by_time"] = {"status": "success", "data": time_raw}

        # --- Top Queries by Calls (for AsciiDoc report) ---
        adoc_content.append("\n==== Top Queries by Call Count")
        calls_query = get_pg_stat_statements_query(connector, 'calls') + " LIMIT %(limit)s;"
        calls_formatted, calls_raw = connector.execute_query(calls_query, params=params, return_raw=True)
        adoc_content.append(calls_formatted)
        structured_data["top_by_calls"] = {"status": "success", "data": calls_raw}

        # --- Top Queries by I/O (for AsciiDoc report) ---
        adoc_content.append("\n==== Top Queries by I/O Wait Time")
        io_query = get_top_queries_by_io_time_query(connector)
        io_formatted, io_raw = connector.execute_query(io_query, params=params, return_raw=True)

        has_io_data = any(item.get('total_io_time', 0) > 0 for item in io_raw) if isinstance(io_raw, list) else False

        if not has_io_data:
            adoc_content.append("[CAUTION]\n====\n**All I/O times are zero.** This usually means the `track_io_timing` parameter is disabled in your `postgresql.conf`. Enable it and reload PostgreSQL to collect I/O statistics.\n====\n")
        elif connector.has_pgstat_new_io_time:
            adoc_content.append("[NOTE]\n====\nShowing top queries by combined I/O time (shared, local, temp) from PostgreSQL 17+ `pg_stat_statements`.\n====\n")
        elif connector.has_pgstat_legacy_io_time:
            adoc_content.append("[NOTE]\n====\nShowing top queries by I/O time. High I/O queries are often candidates for index optimization.\n====\n")
        else:
            adoc_content.append("[NOTE]\n====\nDetailed I/O time statistics are not available in your version of the `pg_stat_statements` extension. Showing top queries by total execution time as a proxy.\n====\n")

        adoc_content.append(io_formatted)
        structured_data["top_by_io"] = {"status": "success", "data": io_raw}

    except Exception as e:
        error_msg = f"[ERROR]\n====\nCould not perform query analysis: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["query_analysis"] = {"status": "error", "error": str(e)}

    return "\n".join(adoc_content), structured_data
