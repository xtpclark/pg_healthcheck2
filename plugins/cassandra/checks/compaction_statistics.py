# This check now uses the nodetool query to get real-time compaction stats
from plugins.cassandra.utils.qrylib.nodetool_queries import get_nodetool_compaction_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 7  # High: A large compaction backlog is a significant operational issue.


def run_compaction_statistics(connector, settings):
    """
    Analyzes nodetool compactionstats to identify compaction backlogs.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = [
        "=== Compaction Statistics (Nodetool)",
        "",
        "This check uses `nodetool compactionstats` to analyze the number of pending and active compactions."
    ]
    structured_data = {}
    
    try:
        # Request the nodetool command through the connector abstraction
        query = get_nodetool_compaction_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            # Add a hint about SSH configuration if Paramiko is the issue
            if "Paramiko" in formatted or "SSH" in formatted:
                 adoc_content.append("\n[IMPORTANT]\n====\nThis check requires SSH access to a Cassandra node. Ensure `ssh_host`, `ssh_user`, and `ssh_key_file` are correctly configured.\n====\n")
            structured_data["compaction_stats"] = {"status": "error", "data": raw}
            return "\n".join(adoc_content), structured_data

        # The connector's parser returns a dict with 'pending_tasks' and 'active_compactions'
        pending_tasks = raw.get('pending_tasks', 0)
        active_compactions = raw.get('active_compactions', [])
        
        structured_data["compaction_stats"] = {
            "status": "success",
            "data": {
                "pending_tasks": pending_tasks,
                "active_compactions_count": len(active_compactions),
                "active_compactions": active_compactions
            }
        }
        
        # --- Analysis ---
        pending_threshold = settings.get('compaction_pending_threshold', 100)
        
        if pending_tasks > pending_threshold:
            issues_found = True
            adoc_content.append(f"[CRITICAL]\n====\n**High Compaction Backlog:** {pending_tasks} pending compactions detected, exceeding the threshold of {pending_threshold}. This indicates the node cannot keep up with write load, which can lead to severe performance degradation and read timeouts.\n====\n")
        else:
            issues_found = False
            adoc_content.append(f"[NOTE]\n====\nCompaction backlog is healthy with {pending_tasks} pending tasks.\n====\n")

        # Report on active compactions if any are running
        if active_compactions:
            adoc_content.append("\n==== Active Compactions")
            adoc_content.append(formatted) # The connector already formatted this table for us
        
        if issues_found:
            adoc_content.append("\n==== Recommendations")
            adoc_content.append("[TIP]\n====\n")
            adoc_content.append("* **Investigate Write Load:** A high number of pending tasks is often caused by a sustained high write load.\n")
            adoc_content.append("* **Check I/O Capacity:** Ensure the node has sufficient disk I/O to process compactions. Monitor `iostat` on the node.\n")
            adoc_content.append("* **Review Compaction Strategy:** The current compaction strategy may not be suitable for the workload. Consider alternatives like LeveledCompactionStrategy for read-heavy workloads if applicable.\n")
            adoc_content.append("====\n")
            
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCompaction statistics check failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["compaction_stats"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
