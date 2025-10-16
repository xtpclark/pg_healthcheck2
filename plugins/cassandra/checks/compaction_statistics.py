from plugins.cassandra.utils.qrylib.compaction_queries import get_compaction_history_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 6  # Medium: Performance concerns related to compaction backlog


def run_compaction_statistics(connector, settings):
    """
    Performs the health check analysis for Cassandra compaction statistics.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = [
        "=== Compaction Statistics",
        "",
        "This check examines recent compaction history to identify potential backlog or high activity."
    ]
    structured_data = {}
    
    try:
        query = get_compaction_history_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        if "[ERROR]" in formatted:
            # Query execution failed
            adoc_content.append(formatted)
            structured_data["compaction_history"] = {"status": "error", "data": raw}
        elif not raw:
            # No recent compactions (healthy or low activity)
            adoc_content.append("[NOTE]")
            adoc_content.append("====")
            adoc_content.append("No recent compactions detected. Compaction activity is low.")
            adoc_content.append("====")
            structured_data["compaction_history"] = {"status": "success", "data": [], "count": 0}
        else:
            # Compactions found - report and analyze
            recent_count = len(raw)
            adoc_content.append(formatted)
            structured_data["compaction_history"] = {"status": "success", "data": raw, "count": recent_count}
            
            # Simple analysis: high count may indicate issues
            threshold = settings.get('compaction_recent_threshold', 20)
            if recent_count > threshold:
                adoc_content.append("[WARNING]")
                adoc_content.append("====")
                adoc_content.append(f"**High Compaction Activity:** {recent_count} recent compactions detected. This may indicate a compaction backlog, high write load, or inefficient compaction strategy leading to performance degradation.")
                adoc_content.append("====")
            else:
                adoc_content.append("[NOTE]")
                adoc_content.append("====")
                adoc_content.append(f"Normal compaction activity with {recent_count} recent operations.")
                adoc_content.append("====")
            
            adoc_content.append("")
            adoc_content.append("==== Recommendations")
            adoc_content.append("[TIP]")
            adoc_content.append("====")
            adoc_content.append("* Monitor compaction using `nodetool compactionstats` for pending tasks.")
            adoc_content.append("* If backlog persists, consider tuning compaction strategy (e.g., LeveledCompactionStrategy for read-heavy workloads).")
            adoc_content.append("* Ensure sufficient disk space and I/O capacity to handle compaction.")
            adoc_content.append("====")
            
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCompaction statistics check failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["compaction_history"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data