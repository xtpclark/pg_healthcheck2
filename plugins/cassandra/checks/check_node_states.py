from plugins.cassandra.utils.qrylib.qry_check_node_states import get_node_states_query
from plugins.common.check_helpers import (
    require_ssh,
    format_check_header,
    format_recommendations,
    safe_execute_query
)

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 9  # Critical - node availability

def run_check_node_states(connector, settings):
    """
    Performs node states verification using nodetool status.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Node States Verification (Nodetool Status)",
        "Cross-verifying cluster node states using `nodetool status` to ensure all nodes are Up and Normal.",
        requires_ssh=True
    )
    structured_data = {}
    
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "nodetool status")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["check_result"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    query = get_node_states_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "Nodetool status")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["node_states"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    nodes = raw if isinstance(raw, list) else []
    
    if not nodes:
        adoc_content.append("[NOTE]\n====\nNo node data returned from nodetool status.\n====\n")
        structured_data["node_states"] = {"status": "success", "data": []}
        return "\n".join(adoc_content), structured_data
    
    unhealthy_nodes = [node for node in nodes if node.get('status') != 'U' or node.get('state') != 'N']
    
    if unhealthy_nodes:
        adoc_content.append(
            f"[CRITICAL]\n====\n**{len(unhealthy_nodes)} node(s)** not in healthy UN (Up/Normal) state detected. "
            "This may indicate availability issues or ongoing cluster changes.\n====\n"
        )
        adoc_content.append(formatted)
        
        recommendations = [
            f"Investigate node(s): {[node.get('address', 'unknown') for node in unhealthy_nodes]}",
            "Check Cassandra logs: tail -f /var/log/cassandra/system.log on affected nodes",
            "Verify network connectivity: nodetool gossipinfo",
            "If nodes are down, attempt restart: systemctl restart cassandra"
        ]
        adoc_content.extend(format_recommendations(recommendations))
        
        status_result = "critical"
    else:
        adoc_content.append(
            f"[NOTE]\n====\nAll {len(nodes)} nodes are in healthy UN (Up/Normal) state.\n====\n"
        )
        adoc_content.append(formatted)
        status_result = "success"
    
    structured_data["node_states"] = {
        "status": status_result,
        "data": nodes,
        "total_nodes": len(nodes),
        "unhealthy_count": len(unhealthy_nodes)
    }
    
    return "\n".join(adoc_content), structured_data