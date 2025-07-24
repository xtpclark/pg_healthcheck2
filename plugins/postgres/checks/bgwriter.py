from plugins.postgres.utils.postgresql_version_compatibility import get_bgwriter_query

def run_bgwriter_analysis(connector, settings):
    """
    Analyzes bgwriter and checkpointer statistics.
    """
    adoc_content = ["=== BGWriter and Checkpoint Analysis"]
    structured_data = {}

    try:
        query = get_bgwriter_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["bgwriter_stats"] = {"status": "error", "data": raw}
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo bgwriter/checkpointer statistics found.\n====\n")
            structured_data["bgwriter_stats"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nFrequent checkpoints (`checkpoints_req`) can indicate an undersized `max_wal_size`. Ideally, checkpoints should be time-based (`checkpoints_timed`).\n====\n")
            adoc_content.append(formatted)
            structured_data["bgwriter_stats"] = {"status": "success", "data": raw}

    except Exception as e:
        error_msg = f"[ERROR]\n====\nCould not analyze bgwriter/checkpointer stats: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["bgwriter_stats"] = {"status": "error", "error": str(e)}

    return "\n".join(adoc_content), structured_data
