from plugins.postgres.utils.qrylib.pg_stat_statements import get_pg_stat_statements_query

def get_weight():
    """Returns the importance score for this module."""
    return 3

def run_top_queries_by_mean_time(connector, settings):
    """
    Identifies the slowest individual queries based on their mean (average) execution time.
    """
    adoc_content = ["=== Top Queries by Mean Execution Time", "Identifies queries that are the slowest on an individual basis. These queries can cause user-facing latency and hold locks for extended periods.\n"]
    structured_data = {}

    try:
        if not connector.has_pgstat:
            adoc_content.append("[NOTE]\n====\n`pg_stat_statements` extension is not enabled. Analysis cannot be performed.\n====\n")
            return "\n".join(adoc_content), {}
        
        # Call the qrylib function telling it to order by mean time
        query = get_pg_stat_statements_query(connector, 'standard', order_by='mean_time') + " LIMIT %(limit)s;"

        params = {'limit': settings.get('row_limit', 10)}
        formatted, raw = connector.execute_query(query, params=params, return_raw=True)
        
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["top_queries_by_mean_time"] = {"status": "error", "details": raw}
        else:
            adoc_content.append(formatted)
            structured_data["top_queries_by_mean_time"] = {"status": "success", "data": raw}
    
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nFailed during mean time query analysis: {e}\n====\n")

    adoc_content.append("\n[TIP]\n====\nQueries with high `mean_time` are your slowest individual operations. Even if not called frequently, they are prime candidates for optimization to improve application latency. Use `EXPLAIN (ANALYZE, BUFFERS)` to diagnose their execution plans.\n====\n")
    
    return "\n".join(adoc_content), structured_data
