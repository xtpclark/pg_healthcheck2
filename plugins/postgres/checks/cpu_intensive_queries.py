from plugins.postgres.utils.qrylib.cpu_intensive_queries import (
    get_aurora_replica_status_query,
    get_active_connections_query
)
from plugins.postgres.utils.qrylib.pg_stat_statements import get_pg_stat_statements_query

def get_weight():
    """Returns the importance score for this module."""
    # Diagnosing performance issues is a high priority.
    return 9

def run_cpu_intensive_queries(connector, settings):
    """
    Analyzes metrics from pg_stat_activity and pg_stat_statements to identify
    potential causes of CPU saturation.
    """
    adoc_content = ["=== CPU Intensive Query Analysis", "Analyzes database activity and query statistics to identify potential causes of high CPU usage.\n"]
    structured_data = {}

    # --- Active Connections (Excluding Autovacuum) ---
    try:
        adoc_content.append("==== Active Connections (Excluding Autovacuum)")
        query = get_active_connections_query()
        params = {'autovacuum_pattern': 'autovacuum:%'}
        formatted, raw = connector.execute_query(query, params=params, return_raw=True)
        
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        else:
            adoc_content.append(formatted)
        structured_data["active_connections"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze active connections: {e}\n====\n")

    # --- Top Queries by Execution Time ---
    if settings.get('has_pgstat'):
        try:
            adoc_content.append("\n==== Top Queries by Total Execution Time")
            # Get base query from the pg_stat_statements qrylib
            query = get_pg_stat_statements_query(connector, query_type='standard')
            # Append the LIMIT clause
            query += " LIMIT %(limit)s;"
            params = {'limit': settings.get('row_limit', 10)}
            
            formatted, raw = connector.execute_query(query, params=params, return_raw=True)

            if "[ERROR]" in formatted:
                adoc_content.append(formatted)
            else:
                adoc_content.append("[IMPORTANT]\n====\nQueries with high `total_exec_time` (or `total_time`) are the primary contributors to CPU load. Focus optimization efforts here first.\n====\n")
                adoc_content.append(formatted)
            structured_data["top_queries_by_time"] = {"status": "success", "data": raw}
        except Exception as e:
            adoc_content.append(f"\n[ERROR]\n====\nCould not analyze pg_stat_statements: {e}\n====\n")
    else:
        adoc_content.append("\n==== Top Queries by Total Execution Time")
        adoc_content.append("[NOTE]\n====\nThe `pg_stat_statements` extension is not enabled. This is the most effective tool for diagnosing CPU-intensive queries. It is highly recommended to enable it.\n====\n")

    # --- Aurora-Specific Metrics ---
    if settings.get('is_aurora'):
        try:
            adoc_content.append("\n==== Aurora Replication Status")
            query = get_aurora_replica_status_query()
            formatted, raw = connector.execute_query(query, return_raw=True)
            
            if "[ERROR]" in formatted:
                adoc_content.append(formatted)
            else:
                adoc_content.append("[NOTE]\n====\nHigh `replica_lag` can sometimes correlate with an overloaded writer node, which may also manifest as high CPU.\n====\n")
                adoc_content.append(formatted)
            structured_data["aurora_replica_status"] = {"status": "success", "data": raw}
        except Exception as e:
            adoc_content.append(f"\n[ERROR]\n====\nCould not retrieve Aurora replica status: {e}\n====\n")

    adoc_content.append("\n[TIP]\n====\nUse the `EXPLAIN (ANALYZE, BUFFERS)` command on high-CPU queries to understand their execution plan. Look for inefficient operations like sequential scans on large tables or nested loop joins.\n====\n")

    return "\n".join(adoc_content), structured_data
