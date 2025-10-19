from plugins.cassandra.utils.qrylib.qry_cassandra_process import get_cassandra_process_query
from plugins.common.check_helpers import (
    require_ssh,
    format_check_header,
    format_recommendations,
    safe_execute_query
)

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 10  # Critical - service availability

def run_cassandra_process_check(connector, settings):
    """
    Verifies if the Cassandra process is running using ps command.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Cassandra Process Status (Shell)",
        "Verifying if the Cassandra Java process is running using `ps aux`.",
        requires_ssh=True
    )
    structured_data = {}
    
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "shell commands")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["process_status"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    query = get_cassandra_process_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "ps aux for Cassandra")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["process_status"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    output = raw.get('output', '') if isinstance(raw, dict) else str(raw)
    
    if not output.strip():
        adoc_content.append(
            "[CRITICAL]\n====\n"
            "**Cassandra process not found!** The service appears to be stopped or crashed.\n"
            "====\n"
        )
        adoc_content.append(formatted)
        
        recommendations = [
            "Check Cassandra logs: 'tail -f /var/log/cassandra/system.log' for errors",
            "Attempt to start the service: 'systemctl start cassandra' or '/etc/init.d/cassandra start'",
            "Verify Java installation and JAVA_HOME environment variable",
            "Check system resources: ensure sufficient memory and disk space",
            "Review cassandra.yaml configuration for startup issues"
        ]
        adoc_content.extend(format_recommendations(recommendations))
        
        status_result = "critical"
        processes = []
    else:
        adoc_content.append(
            "[NOTE]\n====\n"
            "Cassandra process is running successfully.\n"
            "====\n"
        )
        adoc_content.append(formatted)
        
        # Parse basic process info (e.g., PID, user, memory)
        lines = output.strip().split('\n')
        processes = []
        for line in lines:
            if 'cassandra' in line.lower() and 'grep' not in line:
                parts = line.split()
                if len(parts) >= 11:
                    processes.append({
                        'user': parts[0],
                        'pid': parts[1],
                        'cpu_percent': parts[2],
                        'mem_percent': parts[3],
                        'command': ' '.join(parts[10:])
                    })
        
        status_result = "success"
    
    structured_data["process_status"] = {
        "status": status_result,
        "data": processes,
        "process_count": len(processes),
        "is_running": len(processes) > 0
    }
    
    return "\n".join(adoc_content), structured_data