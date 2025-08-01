# plugins/postgres/checks/top_io_queries.py

from plugins.postgres.utils.qrylib.top_io_queries import get_top_io_queries_query

def get_weight():
    """Return the base importance of the check."""
    return 7 # High I/O is a significant performance issue.

def run_top_io_queries(connector, settings):
    """
    Identifies queries consuming the most I/O time.
    """
    adoc_content = ["=== Top Queries by I/O Time", "Lists queries that spend the most time waiting for disk I/O, which is often a key bottleneck.\n"]
    structured_data = {}

    # First, check if pg_stat_statements and I/O timings are available at all
    if not connector.has_pgstat or not (connector.has_pgstat_legacy_io_time or connector.has_pgstat_new_io_time):
        adoc_content.append("[NOTE]\n====\nThe `pg_stat_statements` extension is not enabled or does not have I/O timing columns. This check cannot be performed.\n====\n")
        structured_data["top_io_queries"] = {"status": "skipped", "data": []}
        return "\n".join(adoc_content), structured_data
    
    query = get_top_io_queries_query(connector)
    params = (settings.get('row_limit', 10),)
    formatted, raw = connector.execute_query(query, params=params, return_raw=True)
    
    structured_data["top_io_queries"] = raw

    if "[ERROR]" in formatted:
        adoc_content.append(formatted)
    elif not raw:
        adoc_content.append("[NOTE]\n====\nNo I/O intensive queries were found in pg_stat_statements.\n====\n")
    else:
        adoc_content.append("[IMPORTANT]\n====\nThe following queries spend the most time on I/O operations. Consider optimizing these queries or improving the underlying storage performance.\n====\n")
        adoc_content.append(formatted)

    adoc_content.append("\n[TIP]\n====\nHigh I/O time often points to inefficient indexes, missing indexes, or full table scans on large tables. Use `EXPLAIN (ANALYZE, BUFFERS)` to investigate the execution plan for these queries.\n====\n")

    return "\n".join(adoc_content), structured_data
