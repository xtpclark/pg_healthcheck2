def run_nodetool_status_check(connector, settings):
    """Retrieves the status of nodes in the cluster, similar to 'nodetool status'."""
    adoc_content = ["=== Cassandra Node Status"]
    structured_data = {}
    try:
        query = "SELECT broadcast_address, rack, status, state, load, owns, host_id FROM system.peers"
        formatted, raw = connector.execute_query(query, return_raw=True)
        adoc_content.append(formatted)
        structured_data["node_status"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not retrieve node status: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
