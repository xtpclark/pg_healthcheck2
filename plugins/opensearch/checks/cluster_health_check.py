def run_cluster_health_check(connector, settings):
    """Retrieves the health status of the OpenSearch cluster."""
    adoc_content = ["=== OpenSearch Cluster Health"]
    structured_data = {}
    try:
        health = connector.client.cluster.health()
        adoc_content.append(str(health)) # Basic formatting
        structured_data["cluster_health"] = {"status": "success", "data": health}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not retrieve cluster health: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
