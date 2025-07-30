from plugins.postgres.utils.qrylib.aurora_stat_statements import (
    get_aurora_stat_statements_summary_query,
    get_aurora_stat_statements_details_query
)

def get_weight():
    """Returns the importance score for this module."""
    return 9 # Very high importance for detailed, CPU-specific query analysis.

def run_aurora_stat_statements(connector, settings):
    """
    Fetches per-node query statistics from the aurora_stat_statements extension,
    focusing on CPU-intensive queries.
    """
    adoc_content = ["=== Aurora Stat Statements Analysis (Per-Node CPU Usage)", "Provides detailed, per-node query statistics from the `aurora_stat_statements` extension, which is essential for identifying CPU-intensive queries on specific reader or writer instances.\n"]
    structured_data = {}
    params = {'limit': settings.get('row_limit', 10)}

    if not settings.get('is_aurora'):
        adoc_content.append("[NOTE]\n====\nThis check is for AWS Aurora environments only.\n====\n")
        structured_data["aurora_stat_statements"] = {"status": "skipped", "note": "Not an Aurora environment."}
        return "\n".join(adoc_content), structured_data

    try:
        # Get the version-aware queries
        summary_query = get_aurora_stat_statements_summary_query(connector)
        details_query = get_aurora_stat_statements_details_query(connector)

        # Check if the Aurora version is supported by the qrylib functions
        if not summary_query or not details_query:
            version = connector.version_info.get('version_string', 'N/A')
            adoc_content.append(f"[NOTE]\n====\nThis check is not supported on Aurora PostgreSQL version {version}. It requires version 14.9+, 15.4+, or 16+.\n====\n")
            structured_data["aurora_stat_statements"] = {"status": "not_applicable", "reason": f"Version {version} not supported."}
            return "\n".join(adoc_content), structured_data

        # Check if the aurora_stat_statements extension is enabled
        ext_check_query = "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'aurora_stat_statements');"
        _, ext_exists = connector.execute_query(ext_check_query, is_check=True, return_raw=True)

        if not (str(ext_exists).lower() in ['t', 'true']):
            adoc_content.append("[NOTE]\n====\nThe `aurora_stat_statements` extension is not enabled. No per-node CPU analysis is available.\n====\n")
            structured_data["aurora_stat_statements"] = {"status": "not_applicable", "reason": "Extension not enabled."}
            return "\n".join(adoc_content), structured_data

        # Run the summary query
        summary_formatted, summary_raw = connector.execute_query(summary_query, return_raw=True)
        adoc_content.append("==== Stat Statements Summary\n" + summary_formatted)
        structured_data["summary"] = {"status": "success", "data": summary_raw}

        # Run the detailed query for top CPU consumers
        details_formatted, details_raw = connector.execute_query(details_query, params=params, return_raw=True)
        adoc_content.append("\n==== Top Queries by CPU Time\n" + details_formatted)
        structured_data["top_by_cpu"] = {"status": "success", "data": details_raw}

    except Exception as e:
        error_msg = f"[ERROR]\n====\nCould not analyze aurora_stat_statements: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["aurora_stat_statements"] = {"status": "error", "error": str(e)}

    return "\n".join(adoc_content), structured_data
