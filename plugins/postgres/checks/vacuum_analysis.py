from plugins.postgres.utils.qrylib.vacuum_analysis import get_tables_needing_vacuum_or_analyze_query

def get_weight():
    """Returns the importance score for this module."""
    # Vacuuming is critical for performance and preventing bloat.
    return 9

def run_vacuum_analysis(connector, settings):
    """
    Identifies tables that have crossed their autovacuum or autoanalyze
    thresholds and are awaiting maintenance.
    """
    adoc_content = ["=== Vacuum and Analyze Candidates", "Identifies tables that have enough dead tuples or modifications to be candidates for the next autovacuum or autoanalyze run.\n"]
    structured_data = {}

    try:
        query = get_tables_needing_vacuum_or_analyze_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["vacuum_candidates"] = {"status": "error", "details": raw}
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo tables currently meet the threshold for autovacuum or autoanalyze. This indicates the autovacuum daemon is keeping up with database churn.\n====\n")
            structured_data["vacuum_candidates"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nThe following tables have a significant number of dead tuples or modifications and are awaiting cleanup by the autovacuum daemon. If these tables appear consistently, it may indicate that autovacuum is not running frequently enough or aggressively enough to keep up with the workload.\n====\n")
            adoc_content.append(formatted)
            structured_data["vacuum_candidates"] = {"status": "success", "data": raw}

    except Exception as e:
        error_msg = f"Failed during vacuum analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["vacuum_candidates"] = {"status": "error", "details": str(e)}

    adoc_content.append("\n[TIP]\n====\nIf tables are frequently backlogged, consider tuning autovacuum parameters. You can make autovacuum more aggressive by lowering the `autovacuum_vacuum_scale_factor` or `autovacuum_analyze_scale_factor` either globally or on a per-table basis.\n====\n")

    return "\n".join(adoc_content), structured_data
