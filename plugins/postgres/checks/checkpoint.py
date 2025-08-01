from plugins.postgres.utils.qrylib.checkpoint import get_checkpoint_query

def get_weight():
    """Returns the importance score for this module."""
    # Checkpoint tuning is a key performance area.
    return 7

def run_checkpoint_analysis(connector, settings):
    """
    Analyzes checkpoint statistics to identify potential I/O bottlenecks or
    misconfigurations related to WAL size.
    """
    adoc_content = ["=== Checkpoint Analysis", "Analyzes checkpoint frequency. Frequent checkpoints requested by the system (as opposed to timed checkpoints) can indicate an undersized `max_wal_size`.\n"]
    structured_data = {}

    try:
        query = get_checkpoint_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["checkpoint_stats"] = {"status": "error", "details": raw}
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo checkpoint statistics were found.\n====\n")
            structured_data["checkpoint_stats"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nIdeally, most checkpoints should be `timed` (driven by `checkpoint_timeout`) rather than `req` (requested). A high number of requested checkpoints suggests that the WAL is filling up too quickly, forcing a checkpoint. This can lead to bursts of I/O and inconsistent performance.\n====\n")
            adoc_content.append(formatted)
            structured_data["checkpoint_stats"] = {"status": "success", "data": raw}

    except Exception as e:
        error_msg = f"Failed during checkpoint analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["checkpoint_stats"] = {"status": "error", "details": str(e)}

    adoc_content.append("\n[TIP]\n====\nIf you see a high percentage of requested checkpoints, the primary tuning parameter to adjust is `max_wal_size`. Increasing it allows more WAL files to be kept before forcing a checkpoint, smoothing out I/O performance.\n====\n")

    return "\n".join(adoc_content), structured_data
