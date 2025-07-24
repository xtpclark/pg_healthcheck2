from plugins.postgres.utils.postgresql_version_compatibility import (
    get_pg_stat_statements_query,
    get_top_queries_by_io_time_query
)

def run_query_analysis(connector, settings):
    """
    Performs a deep analysis of query performance using pg_stat_statements,
    covering execution time, call frequency, and I/O.
    """
    adoc_content = ["=== Deep Query Analysis (pg_stat_statements)"]
    structured_data = {}
    params = {'limit': settings.get('row_limit', 10)}

    if not connector.has_pgstat:
        adoc_content.append("[NOTE]\n====\n`pg_stat_statements` is not enabled. No query analysis is available.\n====\n")
        structured_data["query_analysis"] = {"status": "not_applicable", "reason": "pg_stat_statements not enabled."}
        return "\n".join(adoc_content), structured_data

    try:
        # --- Top Queries by Total Time ---
        adoc_content.append("==== Top Queries by Total Execution Time")
        time_query = get_pg_stat_statements_query(connector, 'total_time') + " LIMIT %(limit)s;"
        time_formatted, time_raw = connector.execute_query(time_query, params=params, return_raw=True)
        adoc_content.append(time_formatted)
        structured_data["top_by_time"] = {"status": "success", "data": time_raw}

        # --- Top Queries by Calls ---
        adoc_content.append("\n==== Top Queries by Call Count")
        calls_query = get_pg_stat_statements_query(connector, 'calls') + " LIMIT %(limit)s;"
        calls_formatted, calls_raw = connector.execute_query(calls_query, params=params, return_raw=True)
        adoc_content.append(calls_formatted)
        structured_data["top_by_calls"] = {"status": "success", "data": calls_raw}

        # --- Top Queries by I/O ---
        adoc_content.append("\n==== Top Queries by I/O Wait Time")
        io_query = get_top_queries_by_io_time_query(connector)
        io_formatted, io_raw = connector.execute_query(io_query, params=params, return_raw=True)

        # --- NEW: Improved, more helpful note ---
        # Check if the returned data actually contains non-zero I/O times
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
