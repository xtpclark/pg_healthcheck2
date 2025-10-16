from plugins.kafka.utils.qrylib.isr_queries import get_describe_topics_query


def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 9  # Critical for data consistency and replication health


def run_check_isr_health(connector, settings):
    """
    Performs the ISR health check analysis.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = [
        "=== In-Sync Replicas (ISR) Health Check",
        ""
    ]
    structured_data = {}
    
    try:
        query = get_describe_topics_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        if "[ERROR]" in formatted:
            # Query execution failed
            adoc_content.append(formatted)
            structured_data["isr_health"] = {"status": "error", "data": raw}
        else:
            # raw is list of topic dicts
            raw = raw or []
            
            # Calculate aggregates
            under_replicated_count = sum(t.get('under_replicated_partitions', 0) for t in raw)
            isr_shrink_count = sum(t.get('isr_shrinks', 0) for t in raw)
            
            issues = False
            
            if under_replicated_count > 0:
                adoc_content.append(f"[CRITICAL]\n====\n**Critical Replication Issue:** {under_replicated_count} under-replicated partitions detected across topics. This poses a direct risk to data durability and availability.\n====\n")
                issues = True
            
            if isr_shrink_count > 0:
                adoc_content.append(f"[WARNING]\n====\n**ISR Instability Detected:** {isr_shrink_count} ISR shrinks observed across topics. This may indicate temporary replication issues or broker instability.\n====\n")
                issues = True
            
            if not issues:
                adoc_content.append("[NOTE]\n====\nAll partitions have healthy ISR status. No under-replicated partitions or ISR shrinks detected.\n====\n")
            
            adoc_content.append(formatted)
            
            structured_data["isr_health"] = {
                "status": "success",
                "data": raw,
                "under_replicated_total": under_replicated_count,
                "isr_shrinks_total": isr_shrink_count
            }
            
            # Always include recommendations
            adoc_content.append("\n==== Recommendations")
            adoc_content.append("[TIP]\n====\n")
            adoc_content.append("* Ensure all Kafka brokers are operational and can communicate for replication.")
            adoc_content.append("* Verify network connectivity and resolve any latency issues between brokers.")
            adoc_content.append("* Monitor broker logs for replication errors or disk-related problems.")
            adoc_content.append("* For production environments, maintain a replication factor of at least 3.")
            adoc_content.append("====\n")
    
    except Exception as e:
        error_msg = f"[ERROR]\n====\nISR health check failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["isr_health"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data