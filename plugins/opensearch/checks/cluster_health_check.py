def run_cluster_health_check(connector, settings):
    """Retrieves and formats the health status of the OpenSearch cluster."""
    adoc_content = ["=== OpenSearch Cluster Health"]
    structured_data = {}
    try:
        health = connector.client.cluster.health()
        
        # --- AsciiDoc Formatting Logic ---
        table = ['[cols="2,1",options="header"]', '|===', '| Metric | Value']
        for key, value in health.items():
            readable_key = key.replace('_', ' ').title()
            table.append(f"| {readable_key} | {value}")
        table.append('|===')
        
        # Add a note about the cluster status
        status = health.get('status', 'unknown')
        if status == 'green':
            adoc_content.append("[NOTE]\n====\nCluster status is GREEN. All shards are allocated.\n====\n")
        elif status == 'yellow':
            adoc_content.append("[WARNING]\n====\nCluster status is YELLOW. All primary shards are allocated, but one or more replica shards are not. The cluster is functional but at risk of data loss.\n====\n")
        else:
            adoc_content.append("[CRITICAL]\n====\nCluster status is RED. At least one primary shard is not allocated. The cluster is non-operational and some data is unavailable.\n====\n")
            
        adoc_content.append('\n'.join(table))
        structured_data["cluster_health"] = {"status": "success", "data": health}

    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not retrieve cluster health: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
