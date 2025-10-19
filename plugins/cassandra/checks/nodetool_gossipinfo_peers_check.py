from plugins.cassandra.utils.qrylib.qry_nodetool_gossipinfo_peers import get_nodetool_gossipinfo_query
from plugins.common.check_helpers import (
    require_ssh,
    format_check_header,
    format_recommendations,
    safe_execute_query
)

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High - peer communication issues affect cluster health

def run_nodetool_gossipinfo_peers_check(connector, settings):
    """
    Performs the health check analysis.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings (main config, not connector settings)
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Peer Gossip Information Analysis (Nodetool)",
        "Inspecting peer states and schema consistency using `nodetool gossipinfo`.",
        requires_ssh=True
    )
    structured_data = {}
    
    # Check SSH availability using helper
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "nodetool gossipinfo")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["gossipinfo"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    # Execute check using safe helper
    query = get_nodetool_gossipinfo_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "Nodetool gossipinfo")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["gossipinfo"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    # Analyze results
    peers = raw if isinstance(raw, list) else []
    
    if not peers:
        adoc_content.append("[NOTE]\n====\nNo peer data returned.\n====\n")
        structured_data["gossipinfo"] = {"status": "success", "data": []}
        return "\n".join(adoc_content), structured_data
    
    # Find unhealthy peers and check schema consistency
    unhealthy_peers = [p for p in peers if p.get('status', '') != 'UN']
    schema_versions = [p.get('schema_version', '') for p in peers if p.get('schema_version')]
    schema_mismatch = len(set(filter(None, schema_versions))) > 1
    
    # Add formatted output
    adoc_content.append(formatted)
    
    issues = False
    if unhealthy_peers:
        issues = True
        adoc_content.append(
            f"[CRITICAL]\n====\n"
            f"**{len(unhealthy_peers)} peer(s)** not in UN (Up/Normal) state. "
            "This indicates communication or node issues.\n"
            "====\n"
        )
        adoc_content.append("==== Unhealthy Peers")
        adoc_content.append("|===\n|Address|Status|DC")
        for peer in unhealthy_peers:
            adoc_content.append(
                f"|{peer.get('address', 'N/A')}|{peer.get('status', 'N/A')}|{peer.get('dc', 'N/A')}"
            )
        adoc_content.append("|===\n")
    
    if schema_mismatch:
        issues = True
        adoc_content.append(
            "[WARNING]\n====\n"
            "Schema version mismatch detected across peers. "
            "This can cause operation failures.\n"
            "====\n"
        )
        unique_schemas = list(set(filter(None, schema_versions)))
        adoc_content.append(f"Unique schema versions: {', '.join(unique_schemas)}")
    
    if not issues:
        adoc_content.append(
            f"[NOTE]\n====\n"
            f"All {len(peers)} peers are healthy (UN state) with consistent schema.\n"
            "====\n"
        )
    
    # Recommendations if issues
    if issues:
        recommendations = [
            "Verify inter-node network connectivity (check ports 7000 TCP, 7001 SSL)",
            "Review Cassandra logs (/var/log/cassandra/system.log) on affected nodes",
            "Check for firewall or security group blocks between nodes",
            "If schema mismatch, ensure all schema changes are applied and gossip has propagated",
            "Run 'nodetool status' to cross-verify node states"
        ]
        adoc_content.extend(format_recommendations(recommendations))
    
    status_result = "critical" if unhealthy_peers else "warning" if schema_mismatch else "success"
    
    structured_data["gossipinfo"] = {
        "status": status_result,
        "data": peers,
        "total_peers": len(peers),
        "unhealthy_count": len(unhealthy_peers),
        "schema_mismatch": schema_mismatch,
        "unhealthy_peer_addresses": [p.get('address') for p in unhealthy_peers]
    }
    
    return "\n".join(adoc_content), structured_data