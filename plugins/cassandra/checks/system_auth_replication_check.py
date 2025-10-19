from plugins.cassandra.utils.qrylib.qry_system_auth_replication import (
    get_local_dc_query,
    get_peers_query,
    get_system_auth_replication_query
)
from plugins.common.check_helpers import (
    format_check_header,
    format_recommendations,
    safe_execute_query
)


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High - authentication availability


def run_system_auth_replication_check(connector, settings):
    """
    Verifies that system_auth keyspace uses NetworkTopologyStrategy and is replicated to all datacenters.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "System Auth Keyspace Replication Analysis",
        "Verifying that system_auth uses NetworkTopologyStrategy and is replicated across all datacenters."
    )
    structured_data = {}
    
    try:
        # Get local DC
        local_query = get_local_dc_query(connector)
        success_local, formatted_local, raw_local = safe_execute_query(
            connector, local_query, "Local DC query"
        )
        if not success_local:
            adoc_content.append(formatted_local)
            structured_data["local_dc"] = {"status": "error", "data": raw_local}
            return "\n".join(adoc_content), structured_data
        
        local_dc = raw_local[0].get('data_center') if raw_local else None
        
        # Get peers DCs
        peers_query = get_peers_query(connector)
        success_peers, formatted_peers, raw_peers = safe_execute_query(
            connector, peers_query, "Peers DC query"
        )
        if not success_peers:
            adoc_content.append(formatted_peers)
            structured_data["peers_dc"] = {"status": "error", "data": raw_peers}
            return "\n".join(adoc_content), structured_data
        
        dcs_from_peers = [p.get('data_center') for p in raw_peers if p.get('data_center')]
        all_dcs = set([local_dc] + dcs_from_peers) if local_dc else set(dcs_from_peers)
        
        # Get system_auth replication
        rep_query = get_system_auth_replication_query(connector)
        success_rep, formatted_rep, raw_rep = safe_execute_query(
            connector, rep_query, "System auth replication query"
        )
        if not success_rep:
            adoc_content.append(formatted_rep)
            structured_data["replication"] = {"status": "error", "data": raw_rep}
            return "\n".join(adoc_content), structured_data
        
        if not raw_rep:
            adoc_content.append("[WARNING]\n====\nsystem_auth keyspace not found.\n====\n")
            structured_data["replication"] = {"status": "error", "data": []}
            return "\n".join(adoc_content), structured_data
        
        rep_info = raw_rep[0]
        rep = rep_info.get('replication', {})
        class_name = rep.get('class', '')
        durable_writes = rep_info.get('durable_writes', False)
        
        # Analyze
        issues = []
        missing_dcs = []
        if class_name != 'org.apache.cassandra.locator.NetworkTopologyStrategy':
            issues.append("Uses non-NetworkTopologyStrategy: " + class_name)
        else:
            missing_dcs = [dc for dc in all_dcs if int(rep.get(dc, '0')) == 0]
            if missing_dcs:
                issues.append("Missing replication in DCs: " + ', '.join(missing_dcs))
        
        if issues:
            adoc_content.append(f"[WARNING]\n====\n**Configuration Issue:** {'; '.join(issues)}\n====\n")
            
            if len(all_dcs) > 0:
                rep_map_parts = ["'class': 'NetworkTopologyStrategy'"]
                for dc in sorted(all_dcs):
                    rep_map_parts.append(f"'{dc}': 1")
                rep_map = ', '.join(rep_map_parts)
                alter_cmd = f"ALTER KEYSPACE system_auth WITH replication = {{{rep_map}}};"
                recommendations = [
                    alter_cmd,
                    "After altering, run: nodetool repair system_auth -full",
                    "Verify with: SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = 'system_auth';"
                ]
            else:
                recommendations = [
                    "Configure NetworkTopologyStrategy with appropriate datacenters (RF=1 for auth).",
                    "Run 'nodetool repair system_auth' after changes to ensure consistency."
                ]
            adoc_content.extend(format_recommendations(recommendations))
            
            status = "critical" if missing_dcs else "warning"
        else:
            adoc_content.append("[NOTE]\n====\nSystem auth keyspace is properly configured with NetworkTopologyStrategy and replicated to all datacenters.\n====\n")
            status = "success"
        
        adoc_content.append(formatted_rep)
        
        structured_data["replication"] = {
            "status": status,
            "data": rep_info,
            "replication_class": class_name,
            "all_datacenters": list(all_dcs),
            "missing_datacenters": missing_dcs,
            "durable_writes": durable_writes
        }
        
    except Exception as e:
        error_msg = f"[ERROR]\n====\nReplication check failed: {str(e)}\n====\n"
        adoc_content.append(error_msg)
        structured_data["replication"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
