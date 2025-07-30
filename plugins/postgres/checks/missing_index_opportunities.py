def run_missing_index_opportunities(connector, settings):
    """
    Identifies tables with a high number of sequential scans, suggesting
    potential opportunities for new indexes.
    """
    adoc_content = ["=== Missing Index Opportunities", "Identifies tables that are frequently read using inefficient sequential scans. Adding indexes to support the query patterns on these tables can dramatically improve performance.\n"]
    structured_data = {}

    # This query finds tables that are frequently scanned sequentially and are of a significant size.
    missing_idx_query = """
    SELECT
        schemaname AS schema_name,
        relname AS table_name,
        seq_scan AS sequential_scans,
        pg_size_pretty(pg_relation_size(relid)) AS table_size,
        n_live_tup AS live_rows
    FROM pg_stat_user_tables
    WHERE seq_scan > 1000 AND n_live_tup > 10000 -- Scanned > 1000 times and > 10,000 rows
    ORDER BY seq_scan DESC
    LIMIT %(limit)s;
    """

    try:
        if settings.get('show_qry') == 'true':
            adoc_content.append("Missing index opportunities query:")
            adoc_content.append("[,sql]\n----")
            adoc_content.append(missing_idx_query % {'limit': settings.get('row_limit', 10)})
            adoc_content.append("----")

        params = {'limit': settings.get('row_limit', 10)}
        formatted_result, raw_result = connector.execute_query(missing_idx_query, params=params, return_raw=True)

        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["missing_indexes"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nNo tables with a high number of sequential scans were found. This suggests existing indexes are being used effectively.\n====\n")
            structured_data["missing_indexes"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nThe following tables are frequently accessed using slow sequential scans. This indicates that queries hitting these tables are likely missing the appropriate indexes. Analyze the `WHERE` clauses of queries that access these tables to identify which columns to index.\n====\n")
            adoc_content.append(formatted_result)
            structured_data["missing_indexes"] = {"status": "success", "data": raw_result}

    except Exception as e:
        error_msg = f"Failed during missing index analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["missing_indexes"] = {"status": "error", "details": error_msg}

    adoc_content.append("\n[TIP]\n====\nTo find which queries are causing sequential scans on a specific table, you can filter `pg_stat_statements` for that table name. Once you identify a query, use `EXPLAIN` to confirm that an index on the `WHERE` clause columns would be beneficial.\n====\n")

    return "\n".join(adoc_content), structured_data
