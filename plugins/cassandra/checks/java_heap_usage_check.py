from plugins.cassandra.utils.qrylib.qry_java_heap_usage import get_java_heap_usage_query
from plugins.common.check_helpers import (
    require_ssh,
    format_check_header,
    format_recommendations,
    safe_execute_query
)

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 7  # High - memory issues impact performance


def run_java_heap_usage_check(connector, settings):
    """
    Analyzes Java heap usage for the Cassandra process using nodetool info.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Java Heap Usage Analysis (Nodetool)",
        "Checking JVM heap memory usage for the Cassandra process using `nodetool info`.",
        requires_ssh=True
    )
    structured_data = {}
    
    # Check SSH availability using helper
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "nodetool commands")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["heap_usage"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    # Execute nodetool info using safe helper
    query = get_java_heap_usage_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "Nodetool info")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["heap_usage"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    # Parse heap info from raw output (assuming dict with 'Heap Memory (MB)': 'used / committed / max')
    used = 0.0
    committed = 0.0
    max_heap = 1.0  # Avoid division by zero
    heap_str = raw.get('Heap Memory (MB)', '') if isinstance(raw, dict) else str(raw)
    
    if isinstance(heap_str, str) and '/' in heap_str:
        parts = [p.strip() for p in heap_str.split('/')]
        if len(parts) >= 2:
            try:
                used = float(parts[0])
                committed = float(parts[1])
                if len(parts) > 2:
                    max_heap = float(parts[2])
                else:
                    max_heap = committed
            except ValueError:
                pass
    
    usage_percent = (used / max_heap * 100) if max_heap > 0 else 0
    
    if usage_percent == 0:
        adoc_content.append("[NOTE]\n====\nNo heap usage data available.\n====\n")
        structured_data["heap_usage"] = {"status": "unknown", "data": {}}
        return "\n".join(adoc_content), structured_data
    
    # Add formatted output
    adoc_content.append(formatted)
    
    if usage_percent > 80:
        adoc_content.append(
            f"[WARNING]\n====\n"
            f"Java heap usage is {usage_percent:.1f}% (used: {used:.1f} MB / max: {max_heap:.1f} MB), "
            f"exceeding 80% threshold. High usage may cause GC pauses and performance degradation.\n====\n"
        )
        
        recommendations = [
            "Increase -Xmx JVM heap size in cassandra-env.sh (e.g., to 8G for 16GB RAM nodes)",
            "Enable GC logging: Add -XX:+PrintGCDetails -Xloggc:/var/log/cassandra/gc.log to JVM_OPTS",
            "Tune garbage collector parameters if using G1GC (default): adjust MaxGCPauseMillis",
            "Monitor for memory leaks in UDFs, drivers, or custom code",
            "Consider node hardware upgrade if heap is already maximized"
        ]
        adoc_content.extend(format_recommendations(recommendations))
        status_result = "warning"
    else:
        adoc_content.append(
            f"[NOTE]\n====\n"
            f"Java heap usage is healthy at {usage_percent:.1f}% (used: {used:.1f} MB / max: {max_heap:.1f} MB).\n====\n"
        )
        status_result = "success"
    
    structured_data["heap_usage"] = {
        "status": status_result,
        "data": {
            "used_mb": used,
            "committed_mb": committed,
            "max_mb": max_heap,
            "usage_percent": round(usage_percent, 1)
        }
    }
    
    return "\n".join(adoc_content), structured_data