from plugins.postgres.utils.qrylib.deep_query_analysis import (
    get_queries_by_total_time_query,
    get_queries_by_mean_time_query,
    get_queries_by_calls_query,
    get_hot_queries_query,
    get_write_intensive_queries_query
)

def get_weight():
    """Returns the importance score for this module."""
    return 3

def _run_sub_check(connector, adoc_content, structured_data, check_name, query_func, params):
    """Helper to run a sub-check and append its content."""
    try:
        query = query_func(connector) + " LIMIT %(limit)s;"
        formatted, raw = connector.execute_query(query, params=params, return_raw=True)
        
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo query data available for this view.\n====\n")
        else:
            adoc_content.append(formatted)
        
        structured_data[check_name] = raw
    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not perform analysis for {check_name}: {e}\n====\n")

def run_deep_query_analysis(connector, settings):
    """
    Provides a comprehensive analysis of query performance using pg_stat_statements.
    """
    adoc_content = ["=== Deep Query Analysis (pg_stat_statements)", "Provides multiple views of query performance from `pg_stat_statements`.\n"]
    structured_data = {}

    if not connector.has_pgstat:
        adoc_content.append("[NOTE]\n====\n`pg_stat_statements` extension is not enabled. Analysis cannot be performed.\n====\n")
        return "\n".join(adoc_content), structured_data
    
    params = {'limit': settings.get('row_limit', 10)}
    
    # Define all the sub-checks we want to run
    sub_checks = {
        "queries_by_total_time": ("Top Queries by Total Execution Time", get_queries_by_total_time_query, "Queries with high `total_time` contribute most to overall system load."),
        "queries_by_mean_time": ("Top Queries by Mean Execution Time", get_queries_by_mean_time_query, "Queries with high `mean_time` are your slowest individual operations and can impact latency."),
        "queries_by_calls": ("Top Queries by Call Count", get_queries_by_calls_query, "Frequently called queries are central to your application's workload."),
        "hot_queries": ("'Hot' Queries (by Buffer Hits)", get_hot_queries_query, "High `shared_blks_hit` indicates data is frequently read from cache, representing your application's hot data paths."),
        "write_intensive_queries": ("Top Write-Intensive Queries", get_write_intensive_queries_query, "These queries generate the most write activity (WAL or disk writes), impacting I/O performance.")
    }

    for key, (title, query_func, tip) in sub_checks.items():
        adoc_content.append(f"\n==== {title}")
        _run_sub_check(connector, adoc_content, structured_data, key, query_func, params)
        adoc_content.append(f"\n[TIP]\n====\n{tip}\n====\n")

    return "\n".join(adoc_content), structured_data
