from plugins.postgres.utils.qrylib.high_insert_tables import get_high_insert_tables_query

def get_weight():
    """Returns the importance score for this module."""
    return 3 # High importance for a critical, point-in-time issue.

def run_high_insert_tables(connector, settings):
    """
    Identifies tables with a high rate of inserts since the last stats reset.
    """
    adoc_content = ["=== Tables with High Insert Activity", "Identifies tables experiencing a high volume of new row insertions, which can be a source of WAL generation and table bloat.\n"]
    structured_data = {}
    
    try:
        min_threshold = settings.get('min_tup_ins_threshold', 100000)
        query = get_high_insert_tables_query(connector)
        params = {
            'limit': settings.get('row_limit', 10),
            'min_tup_ins_threshold': min_threshold
        }
        
        formatted, raw = connector.execute_query(query, params=params, return_raw=True)

        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["high_insert_tables"] = {"status": "error", "data": raw}
        elif not raw:
            adoc_content.append(f"[NOTE]\n====\nNo tables found with more than {min_threshold:,} tuples inserted.\n====\n")
            structured_data["high_insert_tables"] = {"status": "success", "data": []}
        else:
            adoc_content.append(f"[IMPORTANT]\n====\nTables with high insert rates (`n_tup_ins` > {min_threshold:,}) may require more aggressive autovacuum settings or `fillfactor` tuning to manage bloat.\n====\n")
            adoc_content.append(formatted)
            structured_data["high_insert_tables"] = {"status": "success", "data": raw}

    except Exception as e:
        error_msg = f"[ERROR]\n====\nCould not analyze tables for high inserts: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["high_insert_tables"] = {"status": "error", "error": str(e)}

    return "\n".join(adoc_content), structured_data
