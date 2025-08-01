from plugins.postgres.utils.qrylib.temp_files_analysis import get_temp_files_query

def get_weight():
    """Returns the importance score for this module."""
    return 3

def run_temp_files_analysis(connector, settings):
    """
    Identifies queries that are generating temporary files, indicating potential
    memory inefficiencies.
    """
    adoc_content = ["=== Temporary File Usage by Query", "Analyzes queries that spill to disk by creating temporary files, often due to insufficient `work_mem`.\n"]
    structured_data = {}

    try:
        if not connector.has_pgstat:
            adoc_content.append("[NOTE]\n====\n`pg_stat_statements` extension is not enabled. Analysis cannot be performed.\n====\n")
            structured_data["temp_files"] = {"status": "not_applicable", "reason": "pg_stat_statements not enabled"}
            return "\n".join(adoc_content), structured_data

        query = get_temp_files_query()
        params = {'limit': settings.get('row_limit', 10)}

        if settings.get('show_qry') == 'true':
            adoc_content.append("Temp file analysis query:")
            adoc_content.append(f"[,sql]\n----\n{query % params}\n----")

        formatted_result, raw_result = connector.execute_query(query, params=params, return_raw=True)

        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["temp_files"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nNo queries found using temporary files. This indicates efficient memory usage.\n====\n")
            structured_data["temp_files"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nThe following queries are spilling to disk, which can slow down performance. This is often caused by complex sorts or aggregations that do not fit in `work_mem`.\n====\n")
            adoc_content.append(formatted_result)
            structured_data["temp_files"] = {"status": "success", "data": raw_result}

    except Exception as e:
        error_msg = f"Failed during temp files analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["temp_files"] = {"status": "error", "details": str(e)}

    adoc_content.append("\n[TIP]\n====\nTo reduce temporary file usage, consider:\n1. **Increase `work_mem`**: Allocate more memory for sorts and hashes.\n2. **Optimize Queries**: Use `EXPLAIN ANALYZE` to identify index opportunities.\n3. **Restructure Queries**: Simplify complex queries with CTEs or subqueries.\n====\n")

    return "\n".join(adoc_content), structured_data
