from plugins.cassandra.utils.qrylib.qry_network_connections import get_network_connections_query
from plugins.common.check_helpers import (
    require_ssh,
    format_check_header,
    format_recommendations,
    safe_execute_query
)

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 5  # Medium - network issues affect performance


def run_network_connections_check(connector, settings):
    """
    Analyzes network connection statistics using netstat.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Network Connection Statistics (Netstat)",
        "Checking network statistics using `netstat -s` for errors and anomalies.",
        requires_ssh=True
    )
    structured_data = {}
    
    # Check SSH availability
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "shell commands")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["network_stats"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    # Execute shell command
    query = get_network_connections_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "netstat -s")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["network_stats"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    # Parse shell output
    output = raw.get('output', '') if isinstance(raw, dict) else str(raw)
    if not output:
        adoc_content.append("[NOTE]\n====\nNo network statistics data returned.\n====\n")
        structured_data["network_stats"] = {"status": "success", "data": {}}
        return "\n".join(adoc_content), structured_data
    
    lines = output.strip().split('\n')
    stats = {}
    error_keywords = ['errors', 'dropped', 'discarded', 'failed']
    total_errors = 0
    key_stats = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # Extract simple key-value like "Xxx: YYY"
        if ':' in line:
            parts = line.split(':', 1)
            if len(parts) == 2:
                key = parts[0].strip()
                value = parts[1].strip()
                stats[key] = value
                # Check for errors
                for keyword in error_keywords:
                    if keyword in key.lower() and any(char.isdigit() for char in value):
                        try:
                            err_count = int(''.join(c for c in value if c.isdigit()))
                            total_errors += err_count
                        except ValueError:
                            pass
                key_stats.append(f"{key}: {value}")
    
    # Add formatted output (first few lines for brevity)
    adoc_content.append(formatted)
    
    # Analyze
    threshold = 100  # Arbitrary threshold for total errors
    if total_errors > threshold:
        adoc_content.append(
            f"[WARNING]\n====\n"
            f"High network errors detected: {total_errors} total errors/discards/failures. "
            f"This may indicate network issues affecting Cassandra communication.\n"
            "====\n"
        )
        
        # Table of key stats
        adoc_content.append("\n==== Key Network Statistics")
        adoc_content.append("|===")
        adoc_content.append("|Statistic|Value")
        for stat in key_stats[:20]:  # Limit to first 20
            parts = stat.split(':', 1)
            if len(parts) == 2:
                adoc_content.append(f"|{parts[0].strip()}|{parts[1].strip()}")
        adoc_content.append("|===\n")
        
        recommendations = [
            "Investigate network hardware: check cables, switches, and NICs for faults",
            "Monitor network traffic with 'tcpdump' or 'wireshark' on Cassandra ports (7000, 9042)",
            "Verify firewall rules allow Cassandra inter-node communication",
            "Check OS network settings: ensure MTU is consistent across nodes",
            "If errors persist, consider packet loss testing with 'ping -c 1000 <other_node>'"
        ]
        adoc_content.extend(format_recommendations(recommendations))
        status_result = "warning"
    else:
        adoc_content.append(
            f"[NOTE]\n====\n"
            f"Network statistics look healthy: {total_errors} total errors (below threshold).\n"
            "====\n"
        )
        status_result = "success"
    
    structured_data["network_stats"] = {
        "status": status_result,
        "data": stats,
        "total_errors": total_errors,
        "error_keywords_found": len([k for k in stats if any(ek in k.lower() for ek in error_keywords)])
    }
    
    return "\n".join(adoc_content), structured_data