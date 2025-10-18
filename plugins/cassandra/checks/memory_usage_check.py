from plugins.cassandra.utils.qrylib.qry_memory_usage import get_memory_usage_query
from plugins.common.check_helpers import (
    require_ssh,
    format_check_header,
    format_recommendations,
    safe_execute_query
)

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High - memory exhaustion risks

def run_memory_usage_check(connector, settings):
    """
    Analyzes available memory on the Cassandra server using 'free -m' command.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Memory Usage Analysis (Shell)",
        "Checking available memory using `free -m` command.",
        requires_ssh=True
    )
    structured_data = {}
    
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "shell commands")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["memory_usage"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    query = get_memory_usage_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "free -m command")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["memory_usage"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    output = raw.get('output', '') if isinstance(raw, dict) else str(raw)
    if not output:
        adoc_content.append("[NOTE]\n====\nNo memory data returned.\n====\n")
        structured_data["memory_usage"] = {"status": "success", "data": {}}
        return "\n".join(adoc_content), structured_data
    
    lines = output.strip().split('\n')
    mem_line = None
    for line in lines:
        if line.startswith('Mem:'):
            mem_line = line
            break
    
    if not mem_line:
        adoc_content.append("[ERROR]\n====\nFailed to parse memory output.\n====\n")
        structured_data["memory_usage"] = {"status": "error", "data": output}
        return "\n".join(adoc_content), structured_data
    
    parts = mem_line.split()
    if len(parts) >= 7:
        total = int(parts[1])
        used = int(parts[2])
        free = int(parts[3])
        available = int(parts[6])
    else:
        total = used = free = available = 0
    
    memory_data = {
        'total_mb': total,
        'used_mb': used,
        'free_mb': free,
        'available_mb': available,
        'available_percent': round((available / total * 100) if total > 0 else 0, 2)
    }
    
    adoc_content.append(formatted)
    
    threshold_mb = 512
    threshold_percent = 10
    low_memory = available < threshold_mb or memory_data['available_percent'] < threshold_percent
    
    if low_memory:
        adoc_content.append(
            f"[WARNING]\n====\n"
            f"Low available memory detected: {available} MB ({memory_data['available_percent']}%) "
            f"below thresholds ({threshold_mb} MB or {threshold_percent}%).\n"
            "====\n"
        )
        
        recommendations = [
            "Identify and kill unnecessary processes using 'top' or 'htop'",
            "Check for memory leaks in Cassandra by reviewing recent logs",
            "Increase JVM heap size in cassandra-env.sh if appropriate (monitor GC logs)",
            "Add more RAM to the server or tune off-heap memory usage",
            "Review running queries and reduce concurrent requests if overloaded"
        ]
        adoc_content.extend(format_recommendations(recommendations))
        status = "warning"
    else:
        adoc_content.append(
            f"[NOTE]\n====\n"
            f"Adequate memory available: {available} MB ({memory_data['available_percent']}%).\n"
            "====\n"
        )
        status = "success"
    
    structured_data["memory_usage"] = {
        "status": status,
        "data": memory_data,
        "low_memory": low_memory
    }
    
    return "\n".join(adoc_content), structured_data