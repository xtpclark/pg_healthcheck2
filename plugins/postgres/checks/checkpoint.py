# Correct: Import the stateless query builder from the central utility
from plugins.postgres.utils.qrylib.checkpoint import get_checkpoint_query

def get_weight():
    """Returns the importance score for this module."""
    return 5 

def run_checkpoint_analysis(connector, settings):
    """
    Analyzes checkpoint performance metrics from pg_stat_bgwriter.
    """
    adoc_content = ["=== Checkpoint Analysis"]
    # 1. Initialize a dictionary to hold all structured data for this check
    structured_data = {}

    try:
        # Get the correct, version-aware query
        checkpoint_query = get_checkpoint_query(connector)
        
        # The connector returns both formatted text and raw data
        formatted, raw = connector.execute_query(checkpoint_query, return_raw=True)
        
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            # 2. Populate structured data with an error status
            structured_data["checkpoint_stats"] = {"status": "error", "data": raw}
        elif not raw:
            adoc_content.append("[NOTE]\n====\nCould not retrieve checkpoint statistics.\n====\n")
            # 2. Populate structured data for the "no results" case
            structured_data["checkpoint_stats"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nFrequent checkpoints can indicate an undersized `max_wal_size`. Checkpoints should primarily be time-based (`checkpoints_timed`), not requested (`checkpoints_req`).\n====\n")
            adoc_content.append(formatted)
            # 2. Populate structured data with the raw results on success
            structured_data["checkpoint_stats"] = {"status": "success", "data": raw}
            
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCould not analyze checkpoints: {e}\n====\n"
        adoc_content.append(error_msg)
        # 2. Populate structured data in case of a fatal exception
        structured_data["checkpoint_stats"] = {"status": "error", "error": str(e)}

    # 3. ALWAYS return the tuple: (AsciiDoc string, structured data dictionary)
    return "\n".join(adoc_content), structured_data
