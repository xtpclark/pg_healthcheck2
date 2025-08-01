from plugins.postgres.utils.qrylib.table_health_analysis import get_table_health_query

def get_weight():
    """Returns the importance score for this module."""
    return 8

def run_table_health_analysis(connector, settings):
    """
    Analyzes key table-level metrics, focusing on bloat estimation and scan
    efficiency to identify performance bottlenecks.
    """
    adoc_content = ["=== Table Health and Scan Efficiency", "Provides an analysis of table size, bloat, and scan patterns. High bloat or frequent sequential scans on large tables can severely degrade performance.\n"]
    structured_data = {}

    try:
        query = get_table_health_query()
        params = {'limit': settings.get('row_limit', 20)}

        if settings.get('show_qry'):
            adoc_content.append("Table health query:")
            adoc_content.append(f"[,sql]\n----\n{query % params}\n----")
            
        formatted_result, raw_result = connector.execute_query(query, params=params, return_raw=True)

        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["table_health_metrics"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nNo significant table health issues found for tables larger than 10MB.\n====\n")
            structured_data["table_health_metrics"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nReview the tables below for high bloat or low index scan usage. High `estimated_bloat` indicates wasted space that can slow down queries. A low `idx_scan_pct` on a frequently scanned table suggests missing or ineffective indexes.\n====\n")
            adoc_content.append(formatted_result)
            structured_data["table_health_metrics"] = {"status": "success", "data": raw_result}

    except Exception as e:
        error_msg = f"Failed during table health analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["table_health_metrics"] = {"status": "error", "details": str(e)}

    adoc_content.append("\n[TIP]\n====\n* **To fix bloat**: Run `VACUUM FULL <table_name>;` (requires an exclusive lock) or use the `pg_repack` extension for an online solution.\n* **To improve scan efficiency**: Analyze queries hitting tables with high `seq_scan` counts to identify opportunities for new indexes.\n====\n")

    return "\n".join(adoc_content), structured_data
