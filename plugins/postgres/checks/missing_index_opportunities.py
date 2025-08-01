from plugins.postgres.utils.qrylib.missing_index_opportunities import get_missing_index_opportunities_query

def get_weight():
    """Returns the importance score for this module."""
    return 6

def run_missing_index_opportunities(connector, settings):
    """
    Identifies tables with a high number of sequential scans, suggesting
    potential opportunities for new indexes.
    """
    adoc_content = ["=== Missing Index Opportunities", "Identifies tables that are frequently read using inefficient sequential scans. Adding indexes to support the query patterns on these tables can dramatically improve performance.\n"]
    structured_data = {}

    try:
        query = get_missing_index_opportunities_query()
        params = {'limit': settings.get('row_limit', 10)}

        if settings.get('show_qry') == 'true':
            adoc_content.append("Missing index opportunities query:")
            adoc_content.append(f"[,sql]\n----\n{query % params}\n----")

        formatted_result, raw_result = connector.execute_query(query, params=params, return_raw=True)

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
        structured_data["missing_indexes"] = {"status": "error", "details": str(e)}

    adoc_content.append("\n[TIP]\n====\nTo find which queries are causing sequential scans on a specific table, you can filter `pg_stat_statements` for that table name. Once you identify a query, use `EXPLAIN` to confirm that an index on the `WHERE` clause columns would be beneficial.\n====\n")

    return "\n".join(adoc_content), structured_data
