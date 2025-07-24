from plugins.mysql.utils.mysql_version_compatibility import get_processlist_query

def run_processlist_analysis(connector, settings):
    """
    Analyzes the MySQL process list to identify long-running queries or
    a high number of active connections.
    """
    adoc_content = ["=== MySQL Process List Analysis", "Provides a snapshot of the current database connections and their states.\n"]
    structured_data = {}
    
    try:
        # 1. Get the correct, version-aware query from the compatibility module
        query = get_processlist_query(connector)
        
        # 2. Execute the query
        formatted, raw_result = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["processlist_details"] = {"status": "error", "details": raw_result}
            structured_data["processlist_summary"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nNo active processes found.\n====\n")
            structured_data["processlist_details"] = {"status": "success", "data": []}
            structured_data["processlist_summary"] = {"status": "success", "data": {"total_processes": 0, "long_running_processes": 0}}
        else:
            # 3. Create a summary for the AI analysis
            long_running_threshold_seconds = 60
            long_running_processes = [p for p in raw_result if p.get('TIME', 0) > long_running_threshold_seconds]
            
            summary_data = {
                "total_processes": len(raw_result),
                "long_running_processes_count": len(long_running_processes),
                "long_running_threshold_seconds": long_running_threshold_seconds
            }
            structured_data["processlist_summary"] = {"status": "success", "data": summary_data}
            structured_data["processlist_details"] = {"status": "success", "data": raw_result}

            # 4. Format the AsciiDoc report
            if long_running_processes:
                adoc_content.append(f"[WARNING]\n====\nFound {len(long_running_processes)} process(es) running longer than {long_running_threshold_seconds} seconds. Long-running queries can hold locks and consume significant resources.\n====\n")
            
            adoc_content.append(formatted)

    except Exception as e:
        error_msg = f"Failed during process list analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["processlist_analysis_error"] = {"status": "error", "details": str(e)}

    # 5. Return both AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
