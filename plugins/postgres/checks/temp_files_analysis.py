from plugins.postgres.utils.postgresql_version_compatibility import get_postgresql_version, validate_postgresql_version

def run_temp_files_analysis(connector, settings):
    """
    Identifies queries that are generating temporary files, which often indicates inefficient memory usage.
    """
    adoc_content = ["=== Temporary File Usage by Query", "Analyzes queries that spill to disk by creating temporary files. This typically happens when an operation (like a sort or hash) requires more memory than allocated `work_mem`.\n"]
    structured_data = {}

    try:
        if settings.get('has_pgstat') != 't':
            adoc_content.append("[NOTE]\n====\n`pg_stat_statements` extension is not enabled. Analysis cannot be performed.\n====\n")
            structured_data["temp_files"] = {"status": "not_applicable", "reason": "pg_stat_statements not enabled."}
            return "\n".join(adoc_content), structured_data

        compatibility = get_postgresql_version(connector.cursor, connector.execute_query)
        is_supported, error_msg = validate_postgresql_version(compatibility)
        if not is_supported:
            raise ValueError(error_msg)

        # This query joins pg_stat_statements with temp file stats to find the source queries.
        temp_files_query = """
            SELECT
                REPLACE(REPLACE(LEFT(pss.query, 100), E'\\n', ' '), '|', ' ') || '...' AS query,
                pss.calls,
                pg_size_pretty(pss.temp_blks_written * 8192) AS total_temp_written,
                pg_size_pretty(pss.temp_blks_read * 8192) AS total_temp_read
            FROM pg_stat_statements pss
            WHERE (pss.temp_blks_written > 0 OR pss.temp_blks_read > 0)
            ORDER BY (pss.temp_blks_written + pss.temp_blks_read) DESC
            LIMIT %(limit)s;
        """

        if settings.get('show_qry') == 'true':
            adoc_content.append("Temp file analysis query:")
            adoc_content.append("[,sql]\n----")
            adoc_content.append(temp_files_query % {'limit': settings.get('row_limit', 10)})
            adoc_content.append("----")

        params_for_query = {'limit': settings.get('row_limit', 10)}
        formatted_result, raw_result = connector.execute_query(temp_files_query, params=params_for_query, return_raw=True)

        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["temp_files"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nNo queries were found to be using temporary files. This is a healthy sign of efficient memory usage.\n====\n")
            structured_data["temp_files"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nThe following queries are spilling to disk, which can significantly slow down performance. This is often caused by sorting or hashing large datasets that exceed `work_mem`.\n====\n")
            adoc_content.append(formatted_result)
            structured_data["temp_files"] = {"status": "success", "data": raw_result}

    except Exception as e:
        error_msg = f"Failed during temp files analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["temp_files"] = {"status": "error", "details": error_msg}

    adoc_content.append("\n[TIP]\n====\nTo reduce temporary file usage, consider these options:\n1.  **Increase `work_mem`**: Raise `work_mem` for the session or globally to allow more memory for sorts and hashes.\n2.  **Optimize the Query**: Analyze the query plan (`EXPLAIN ANALYZE`) to see if a better index can eliminate the need for a large sort.\n3.  **Restructure the Query**: Break down complex queries into smaller steps or use CTEs.\n====\n")

    return "\n".join(adoc_content), structured_data
