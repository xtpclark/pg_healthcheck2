from plugins.cassandra.utils.qrylib.qry_cpu_load_average import get_cpu_load_average_query

from plugins.common.check_helpers import (
    require_ssh,
    format_check_header,
    format_recommendations,
    safe_execute_query
)

import re

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High - CPU load impacts performance


def run_cpu_load_average_check(connector, settings):
    """
    Analyzes CPU load average using uptime command.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "CPU Load Average Analysis (Uptime)",
        "Checking system load average using `uptime` command.",
        requires_ssh=True
    )
    structured_data = {}
    
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "shell commands")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["cpu_load"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    query = get_cpu_load_average_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "uptime command")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["cpu_load"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    output = raw.get('output', '') if isinstance(raw, dict) else str(raw)
    
    if not output:
        adoc_content.append("[NOTE]\n====\nNo uptime data returned.\n====\n")
        structured_data["cpu_load"] = {"status": "success", "data": {}}
        return "\n".join(adoc_content), structured_data
    
    # Parse uptime output
    load_match = re.search(r'load average:\s*([\d.]+),\s*([\d.]+),\s*([\d.]+)', output)
    if not load_match:
        adoc_content.append("[WARNING]\n====\nCould not parse load average from uptime output.\n====\n")
        adoc_content.append(formatted)
        structured_data["cpu_load"] = {"status": "warning", "data": {"output": output}}
        return "\n".join(adoc_content), structured_data
    
    load1, load5, load15 = map(float, load_match.groups())
    loads = {"1min": load1, "5min": load5, "15min": load15}
    max_load = max(load1, load5, load15)
    
    adoc_content.append(formatted)
    
    recommendations = []
    if max_load > 5:
        adoc_content.append("[CRITICAL]\n====\n**High CPU Load Detected:** Maximum load average of {:.2f} exceeds critical threshold (5.0). Immediate investigation required.\n====\n".format(max_load))
        status_result = "critical"
        recommendations = [
            "Identify high-CPU processes using 'top -c' or 'htop' and terminate unnecessary ones.",
            "Check Cassandra thread pools with 'nodetool tpstats' for bottlenecks.",
            "Review recent application changes that may have increased CPU usage.",
            "Consider vertical scaling (more CPU cores) or horizontal scaling (add nodes to cluster)."
        ]
    elif max_load > 2:
        adoc_content.append("[WARNING]\n====\n**Elevated CPU Load:** Maximum load average of {:.2f} exceeds warning threshold (2.0). Monitor closely.\n====\n".format(max_load))
        status_result = "warning"
        recommendations = [
            "Monitor CPU usage trends using system monitoring tools.",
            "Optimize Cassandra compaction strategy if compactions are CPU-intensive.",
            "Check for inefficient CQL queries or hot partitions causing CPU spikes.",
            "Ensure adequate cooling and hardware resources for the node."
        ]
    else:
        adoc_content.append("[NOTE]\n====\n**Healthy CPU Load:** All load averages are below warning threshold. System is handling load well.\n====\n")
        adoc_content.append("\n==== Load Averages")
        adoc_content.append("|===\n|Time Window|Load Average")
        for period, load in loads.items():
            adoc_content.append(f"|{period}|{load:.2f}")
        adoc_content.append("|===\n")
        status_result = "success"
    
    if recommendations:
        adoc_content.extend(format_recommendations(recommendations))
    
    structured_data["cpu_load"] = {
        "status": status_result,
        "data": loads,
        "max_load": max_load
    }
    
    return "\n".join(adoc_content), structured_data