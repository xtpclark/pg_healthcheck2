from plugins.cassandra.utils.qrylib.qry_cluster_connectivity import get_local_query, get_peers_query
from plugins.common.check_helpers import format_check_header, safe_execute_query, format_recommendations


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 7  # High - connectivity issues impact availability


def run_cluster_connectivity_check(connector, settings):
    """
    Verifies cluster connectivity by querying node statuses in system.local and system.peers tables.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Cluster Connectivity Analysis (CQL)",
        "Verifying cluster topology and peer connectivity using system.local and system.peers tables."
    )
    structured_data = {}
    
    try:
        # Get local node info
        local_query = get_local_query(connector)
        success_local, formatted_local, raw_local = safe_execute_query(connector, local_query, "system.local query")
        
        if not success_local:
            adoc_content.append(formatted_local)
            structured_data["local"] = {"status": "error", "data": raw_local}
            return "\n".join(adoc_content), structured_data
        
        local_info = raw_local[0] if raw_local else {}
        
        # Get peers info (version-aware)
        peers_query = get_peers_query(connector)
        success_peers, formatted_peers, raw_peers = safe_execute_query(connector, peers_query, "system.peers query")
        
        if not success_peers:
            adoc_content.append(formatted_peers)
            structured_data["peers"] = {"status": "error", "data": raw_peers}
            return "\n".join(adoc_content), structured_data
        
        peers = raw_peers if raw_peers else []
        
        # Analysis
        issues = []
        
        # Check if local node has valid addresses
        listen_addr = local_info.get('listen_address')
        if not listen_addr:
            issues.append("Local node listen_address is not configured.")
        
        # Check number of peers
        if len(peers) == 0:
            issues.append("No peers detected. This may indicate a single-node cluster or connectivity issues.")
        else:
            # Check version consistency
            local_version = local_info.get('release_version', '')
            version_mismatch = [p for p in peers if p.get('release_version', '') != local_version]
            if version_mismatch:
                issues.append(f"Version mismatch: {len(version_mismatch)} peers have different Cassandra version than local node ({local_version}).")
        
        # Add formatted outputs
        adoc_content.append(formatted_local)
        adoc_content.append(formatted_peers)
        
        if issues:
            adoc_content.append("[WARNING]\n====\n" + "\n".join(f"* {issue}" for issue in issues) + "\n====\n")
            
            recommendations = [
                "Verify cassandra.yaml configuration: ensure seeds list includes reachable nodes",
                "Check network connectivity: ping between nodes and verify firewall rules allow Cassandra ports (7000, 9042)",
                "Run 'nodetool status' to confirm all nodes are Up/Normal",
                "If version mismatch, plan upgrade to consistent versions across cluster"
            ]
            adoc_content.extend(format_recommendations(recommendations))
            
            status_result = "warning"
        else:
            adoc_content.append("[NOTE]\n====\nCluster connectivity appears healthy: local node configured and peers detected with consistent versions.\n====\n")
            status_result = "success"
        
        structured_data = {
            "status": status_result,
            "local": {"status": "success", "data": local_info},
            "peers": {"status": "success", "data": peers},
            "peer_count": len(peers),
            "issues_count": len(issues)
        }
        
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCluster connectivity check failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["connectivity"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data