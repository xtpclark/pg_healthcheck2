from plugins.cassandra.utils.qrylib.qry_data_directory_disk_space import get_data_directory_disk_space_query
from plugins.common.check_helpers import (
    require_ssh,
    format_check_header,
    format_recommendations,
    safe_execute_query
)

def get_weight():
    return 7  # High - disk space exhaustion risk

def run_data_directory_disk_space_check(connector, settings):
    """
    Checks disk space for Cassandra data directory using df -h.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Data Directory Disk Space Check (Shell)",
        "Checking disk space for Cassandra data directory (/var/lib/cassandra) using `df -h`.",
        requires_ssh=True
    )
    structured_data = {}
    
    data_dir = '/var/lib/cassandra'  # Default Cassandra data directory
    
    # Check SSH availability
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "shell commands")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["disk_usage"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    # Execute shell command
    query = get_data_directory_disk_space_query(connector, data_dir)
    success, formatted, raw = safe_execute_query(connector, query, "df -h data directory")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["disk_usage"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    # Parse shell output
    output = raw.get('output', '') if isinstance(raw, dict) else str(raw)
    if not output or raw.get('exit_code', 1) != 0:
        adoc_content.append("[NOTE]\n====\nNo valid disk usage data returned.\n====\n")
        structured_data["disk_usage"] = {"status": "success", "data": {}}
        return "\n".join(adoc_content), structured_data
    
    lines = output.strip().split('\n')
    if len(lines) < 2:
        adoc_content.append("[NOTE]\n====\nInsufficient output from df command.\n====\n")
        structured_data["disk_usage"] = {"status": "error", "data": output}
        return "\n".join(adoc_content), structured_data
    
    # Parse the data line
    parts = lines[1].split()
    if len(parts) < 6:
        adoc_content.append("[ERROR]\n====\nFailed to parse df output.\n====\n")
        structured_data["disk_usage"] = {"status": "error", "data": output}
        return "\n".join(adoc_content), structured_data
    
    filesystem = parts[0]
    size = parts[1]
    used = parts[2]
    avail = parts[3]
    use_pct_str = parts[4]
    mount = parts[5]
    
    try:
        use_pct = int(use_pct_str.rstrip('%'))
    except ValueError:
        use_pct = 0
    
    disk_info = {
        'filesystem': filesystem,
        'size': size,
        'used': used,
        'available': avail,
        'use_percent': use_pct,
        'mount_point': mount,
        'data_dir': data_dir
    }
    
    # Add formatted output
    adoc_content.append(formatted)
    
    # Analyze usage
    recommendations = []
    status = "success"
    if use_pct > 90:
        adoc_content.append(
            f"[CRITICAL]\n====\n"
            f"Cassandra data directory at **{use_pct}%** usage - critically low space! Immediate action required.\n"
            "====\n"
        )
        recommendations = [
            "Run 'nodetool clearsnapshot' to remove old snapshots",
            "Check for large tombstone tables and consider TTL policies",
            "Archive or delete old/unneeded data files",
            "Expand storage capacity urgently to prevent write failures"
        ]
        status = "critical"
    elif use_pct > 80:
        adoc_content.append(
            f"[WARNING]\n====\n"
            f"Cassandra data directory at **{use_pct}%** usage - high utilization detected. Proactive measures recommended.\n"
            "====\n"
        )
        recommendations = [
            "Monitor disk growth rate and plan for expansion",
            "Run 'nodetool cleanup' on partitions with old data",
            "Review snapshot retention and clean up periodically",
            'Check for temporary files: \'find /var/lib/cassandra -name "*tmp*" -delete\''
        ]
        status = "warning"
    else:
        adoc_content.append(
            f"[NOTE]\n====\n"
            f"Cassandra data directory usage is healthy at **{use_pct}%** ({avail} available).\n"
            "====\n"
        )
    
    if recommendations:
        adoc_content.extend(format_recommendations(recommendations))
    
    structured_data["disk_usage"] = {
        "status": status,
        "data": disk_info,
        "use_percent": use_pct
    }
    
    return "\n".join(adoc_content), structured_data