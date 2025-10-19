from plugins.cassandra.utils.qrylib.qry_system_log_errors import get_system_log_errors_query
from plugins.common.check_helpers import (
    require_ssh,
    format_check_header,
    format_recommendations,
    safe_execute_query
)

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High - log errors indicate operational issues

def run_system_log_errors_check(connector, settings):
    """
    Checks for recent errors in Cassandra system.log using tail and grep.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Cassandra System Log Error Analysis",
        "Scanning recent entries in /var/log/cassandra/system.log for errors, exceptions, and warnings.",
        requires_ssh=True
    )
    structured_data = {}
    
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "shell commands for log analysis")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["log_check"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    query = get_system_log_errors_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "system.log error scan")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["log_check"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    output = raw.get('output', '') if isinstance(raw, dict) else str(raw)
    error_lines = [line.strip() for line in output.strip().split('\n') if line.strip()]
    error_count = len(error_lines)
    
    if error_count == 0:
        adoc_content.append(
            "[NOTE]\n====\n"
            "No recent errors, exceptions, or warnings found in system.log.\n====\n"
        )
        status_result = "success"
    else:
        adoc_content.append(
            f"[WARNING]\n====\n"
            f"**{error_count} recent error/warning entries** detected in system.log. "
            f"This may indicate ongoing issues requiring investigation.\n====\n"
        )
        adoc_content.append(formatted)
        
        recent_errors = error_lines[-10:]  # Show last 10
        adoc_content.append("\n==== Recent Error Entries")
        for line in recent_errors:
            adoc_content.append(f"* {line}")
        
        recommendations = [
            "Review full logs: SSH to node and run 'tail -f /var/log/cassandra/system.log'",
            "Search for specific errors: 'grep -i 'exception' /var/log/cassandra/system.log'",
            "Check for patterns: Look for repeated errors indicating resource issues or misconfiguration",
            "If errors persist, increase logging level temporarily in logback.xml for more details",
            "Correlate with nodetool status and tpstats to identify if errors relate to load or compaction"
        ]
        adoc_content.extend(format_recommendations(recommendations))
        
        status_result = "warning"
    
    structured_data["log_check"] = {
        "status": status_result,
        "error_count": error_count,
        "recent_errors": error_lines[-20:] if error_count > 0 else []  # Store last 20 for rules
    }
    
    return "\n".join(adoc_content), structured_data