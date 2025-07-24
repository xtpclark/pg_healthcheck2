def run_broker_metadata_check(connector, settings):
    """Retrieves metadata about the brokers in the cluster."""
    adoc_content = ["=== Kafka Broker Metadata"]
    structured_data = {}
    try:
        brokers = connector.admin_client.describe_cluster()['brokers']
        adoc_content.append(str(brokers)) # Basic formatting
        structured_data["broker_metadata"] = {"status": "success", "data": brokers}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not retrieve broker metadata: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
