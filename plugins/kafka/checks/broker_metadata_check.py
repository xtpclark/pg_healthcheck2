def run_broker_metadata_check(connector, settings):
    """Retrieves and formats metadata about the brokers in the cluster."""
    adoc_content = ["=== Kafka Broker Metadata"]
    structured_data = {}
    try:
        brokers = connector.admin_client.describe_cluster()['brokers']
        
        if not brokers:
            adoc_content.append("[NOTE]\n====\nNo broker metadata found.\n====\n")
            structured_data["broker_metadata"] = {"status": "success", "data": []}
        else:
            # --- AsciiDoc Formatting Logic ---
            table = ['[cols="1,2,1",options="header"]', '|===', '| Node ID | Host | Port']
            for broker in brokers:
                node_id = broker.get('node_id', 'N/A')
                host = broker.get('host', 'N/A')
                port = broker.get('port', 'N/A')
                table.append(f"| {node_id} | {host} | {port}")
            table.append('|===')
            adoc_content.append('\n'.join(table))
            structured_data["broker_metadata"] = {"status": "success", "data": brokers}

    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not retrieve broker metadata: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
