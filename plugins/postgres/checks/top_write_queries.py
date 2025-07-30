from plugins.postgres.utils.qrylib.top_write_queries import get_top_write_queries_query

def get_weight():
    """Returns the importance score for this module."""
    return 5 # Symtom, not disease

def run_top_write_queries(connector, settings):
    """
    Identifies top write-intensive queries from pg_stat_statements, adapting to PostgreSQL versions.
    """
    adoc_content = ["=== Top Write-Intensive Queries", "Identifies queries generating significant write activity (high WAL generation or disk writes), which can impact I/O performance.\n"]
    structured_data = {}

    try:
        if not connector.has_pgstat:
            adoc_content.append("[NOTE]\n====\n`pg_stat_statements` extension is not enabled. Analysis cannot be performed.\n====\n")
            structured_data["top_write_queries"] = {"status": "not_applicable", "reason": "pg_stat_statements not enabled."}
            return "\n".join(adoc_content), structured_data

        query = get_top_write_queries_query(connector)
        params = {'limit': settings.get('row_limit', 10)}
        formatted, raw = connector.execute_query(query, params=params, return_raw=True)

        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["top_write_queries"] = {"status": "error", "data": raw}
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo significant write-intensive queries found in `pg_stat_statements`.\n====\n")
            structured_data["top_write_queries"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[NOTE]\n====\nThis section lists queries generating the most write activity. Due to normalization, table names may be parameterized. Manual inspection is recommended for correlation.\n====\n")
            adoc_content.append(formatted)
            structured_data["top_write_queries"] = {"status": "success", "data": raw}

    except Exception as e:
        error_msg = f"[ERROR]\n====\nCould not analyze top write queries: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["top_write_queries"] = {"status": "error", "error": str(e)}

    return "\n".join(adoc_content), structured_data
