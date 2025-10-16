from plugins.cassandra.utils.qrylib.node_info_queries import get_node_info_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 3  # Low: Informational check for node details


def run_check_node_info(connector, settings):
    """
    Performs the health check analysis for Cassandra node information.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = []
    structured_data = {}
    
    adoc_content.append("=== Node Information")
    adoc_content.append("")
    
    try:
        query = get_node_info_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        if "[ERROR]" in formatted:
            # Query execution failed
            adoc_content.append(formatted)
            structured_data["node_info"] = {"status": "error", "data": raw}
        elif not raw:
            # No data (unlikely for system.local)
            adoc_content.append("[NOTE]")
            adoc_content.append("====")
            adoc_content.append("No node information available.")
            adoc_content.append("====")
            structured_data["node_info"] = {"status": "success", "data": []}
        else:
            # Data found - informational
            adoc_content.append("[NOTE]")
            adoc_content.append("====")
            adoc_content.append("Node details retrieved successfully.")
            adoc_content.append("====")
            adoc_content.append("")
            adoc_content.append("==== Node Details")
            adoc_content.append(formatted)
            
            # Extract version for potential use
            if raw and len(raw) > 0:
                version = raw[0].get('release_version', 'Unknown')
                structured_data["node_info"] = {
                    "status": "success",
                    "data": raw,
                    "version": version
                }
            else:
                structured_data["node_info"] = {"status": "success", "data": raw}
    
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCheck failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["node_info"] = {"status": "error", "details": str(e)}
    
    # Recommendations
    adoc_content.append("\n==== Recommendations")
    adoc_content.append("[TIP]")
    adoc_content.append("====")
    adoc_content.append("* **Version Check:** Ensure the release_version is up-to-date and supported.")
    adoc_content.append("* **Cluster Verification:** Use `nodetool status` to verify node UP/DOWN status, load, and token ownership (not available via CQL).")
    adoc_content.append("* **Network Config:** Confirm listen_address and broadcast_address are correctly set for your network topology.")
    adoc_content.append("====")
    
    # Additional monitoring note
    adoc_content.append("\n==== Additional Monitoring")
    adoc_content.append("[NOTE]")
    adoc_content.append("====")
    adoc_content.append("For comprehensive node and cluster health:")
    adoc_content.append("")
    adoc_content.append("* **Node Status:** `nodetool status` - Shows UN/DN state, load, and tokens")
    adoc_content.append("* **Info:** `nodetool info` - Detailed node information including uptime and heap usage")
    adoc_content.append("")
    adoc_content.append("These require shell access to Cassandra nodes.")
    adoc_content.append("====")
    
    return "\n".join(adoc_content), structured_data