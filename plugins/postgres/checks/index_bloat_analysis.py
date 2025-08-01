from plugins.postgres.utils.qrylib.index_bloat_analysis import (index_bloat_query)

def get_weight():
    """Returns the importance score for this module."""
    # Performance issue
    return 7

def run_index_bloat_analysis(connector, settings):
    """
    Estimates the amount of bloat in B-Tree indexes, which can negatively
    affect scan performance and consume excess disk space.
    """
    adoc_content = ["=== B-Tree Index Bloat Analysis", "Estimates wasted space in B-Tree indexes using a detailed statistical model. Bloated indexes are larger and less efficient, which can slow down query performance.\n"]
    structured_data = {}

    try:
        query = index_bloat_query(connector)
        
        # MODIFIED: Removed the params dictionary
        # MODIFIED: Removed the params argument from the function call
        formatted, raw = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["index_bloat"] = {"status": "error", "details": raw}
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo significant B-Tree index bloat was detected. This is a sign of healthy index maintenance.\n====\n")
            structured_data["index_bloat"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nThe following indexes appear to be bloated. Bloat increases the size of an index and can slow down scans, as more pages need to be read from disk. Rebuilding bloated indexes can reclaim space and improve performance.\n====\n")
            adoc_content.append(formatted)
            structured_data["index_bloat"] = {"status": "success", "data": raw}

    except Exception as e:
        error_msg = f"Failed during index bloat analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["index_bloat"] = {"status": "error", "details": str(e)}

    adoc_content.append("\n[TIP]\n====\nTo fix index bloat, you can rebuild the affected index using `REINDEX INDEX CONCURRENTLY <index_name>;`. The `CONCURRENTLY` option allows the index to be rebuilt without blocking reads or writes to the table, making it suitable for production environments.\n====\n")

    return "\n".join(adoc_content), structured_data
