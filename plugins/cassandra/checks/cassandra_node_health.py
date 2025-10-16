from plugins.cassandra.utils.qrylib.node_health_queries import (
    get_local_node_query,
    get_peers_query
)


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High: Node health impacts availability


def run_cassandra_node_health(connector, settings):
    """
    Performs the health check analysis for Cassandra node health.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = []  # MUST be a list
    structured_data = {}  # MUST be a dict
    
    adoc_content.append("=== Cassandra Node Health")
    adoc_content.append("")
    
    try:
        # Query local node info
        local_query = get_local_node_query(connector)
        local_formatted, local_raw = connector.execute_query(local_query, return_raw=True)
        
        # Query peers for gossip health
        peers_query = get_peers_query(connector)
        peers_formatted, peers_raw = connector.execute_query(peers_query, return_raw=True)
        
        if "[ERROR]" in local_formatted or not local_raw:
            # Node query failed - node may be down
            adoc_content.append("[CRITICAL]")
            adoc_content.append("====")
            adoc_content.append("**Immediate Action Required:** Cannot retrieve local node information. The node may be down or unresponsive.")
            adoc_content.append("====")
            adoc_content.append(local_formatted)
            structured_data["local_node"] = {"status": "error", "data": local_raw or []}
            
        else:
            # Local node is accessible
            local_node = local_raw[0] if local_raw else {}
            
            # Check peers for isolation
            peer_count = len(peers_raw) if peers_raw else 0
            isolation_threshold = settings.get('min_peers', 1)
            
            issues = []
            if peer_count < isolation_threshold:
                issues.append(f"Low peer count ({peer_count}); node may be isolated from cluster.")
            
            if issues:
                adoc_content.append("[WARNING]")
                adoc_content.append("====")
                adoc_content.append("**Action Required:** Potential node health issues detected:")
                for issue in issues:
                    adoc_content.append(f"- {issue}")
                adoc_content.append("====")
                adoc_content.append(local_formatted)
                adoc_content.append(peers_formatted)
                structured_data["local_node"] = {"status": "warning", "data": local_raw}
                structured_data["peers"] = {"status": "warning", "data": peers_raw, "count": peer_count}
            else:
                adoc_content.append("[NOTE]")
                adoc_content.append("====")
                adoc_content.append("Node is healthy: Accessible, stable uptime, and connected to peers.")
                adoc_content.append("====")
                adoc_content.append(local_formatted)
                adoc_content.append(peers_formatted)
                structured_data["local_node"] = {"status": "success", "data": local_raw}
                structured_data["peers"] = {"status": "success", "data": peers_raw, "count": peer_count}
        
        adoc_content.append("")
        adoc_content.append("==== Recommendations")
        adoc_content.append("[TIP]")
        adoc_content.append("====")
        adoc_content.append("* Monitor node load and uptime regularly using nodetool status.")
        adoc_content.append("* Ensure gossip protocol is functioning by verifying peer connections.")
        adoc_content.append("* If isolated, check network connectivity and cassandra.yaml settings (listen_address, rpc_address).")
        adoc_content.append("====")
        
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCheck failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["error"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data