from plugins.cassandra.utils.qrylib.qry_gcstats import get_gcstats_query
from plugins.common.check_helpers import (
    require_ssh,
    format_check_header,
    format_recommendations,
    safe_execute_query
)

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 7  # High - GC issues impact performance

def run_gcstats_check(connector, settings):
    """
    Analyzes garbage collection statistics using nodetool gcstats.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Garbage Collection Statistics (Nodetool)",
        "Analyzing GC activity and heap usage using `nodetool gcstats`.",
        requires_ssh=True
    )
    structured_data = {}
    
    ssh_ok, skip_msg, skip_data = require_ssh(connector, "nodetool gcstats")
    if not ssh_ok:
        adoc_content.append(skip_msg)
        structured_data["gcstats"] = skip_data
        return "\n".join(adoc_content), structured_data
    
    query = get_gcstats_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "Nodetool gcstats")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["gcstats"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    pools = raw.get('pools', []) if isinstance(raw, dict) else []
    
    if not pools:
        adoc_content.append("[NOTE]\n====\nNo GC pool data returned.\n====\n")
        structured_data["gcstats"] = {"status": "success", "data": []}
        return "\n".join(adoc_content), structured_data
    
    high_gc_pools = [p for p in pools if p.get('pause_time', 0) > 0.5 or p.get('usage_percent', 0) > 80]
    
    adoc_content.append(formatted)
    
    if high_gc_pools:
        adoc_content.append(
            f"[WARNING]\n====\n"
            f"**{len(high_gc_pools)} GC pool(s)** showing high activity or usage detected.\n"
            "This may indicate memory pressure or inefficient GC tuning.\n"
            "====\n"
        )
        
        recommendations = [
            "Review JVM heap settings in cassandra-env.sh - consider increasing -Xmx if memory is sufficient",
            "Monitor application write patterns; high mutation rates can increase GC pressure",
            "Enable GC logging with -XX:+PrintGCDetails and analyze logs for pause time patterns",
            "Consider switching GC algorithm (e.g., G1GC to CMS) based on workload characteristics",
            "Check for memory leaks in custom code or drivers interacting with Cassandra"
        ]
        adoc_content.extend(format_recommendations(recommendations))
        status_result = "warning"
    else:
        adoc_content.append(
            f"[NOTE]\n====\n"
            f"All {len(pools)} GC pools show normal activity and usage.\n"
            "====\n"
        )
        status_result = "success"
    
    structured_data["gcstats"] = {
        "status": status_result,
        "data": pools,
        "total_pools": len(pools),
        "high_gc_count": len(high_gc_pools),
        "max_pause_time": max([p.get('pause_time', 0) for p in pools]) if pools else 0,
        "max_usage_percent": max([p.get('usage_percent', 0) for p in pools]) if pools else 0
    }
    
    return "\n".join(adoc_content), structured_data