def run_system_metrics_check(connector, settings):
    """Retrieves a snapshot of current system metrics."""
    adoc_content = ["=== ClickHouse System Metrics"]
    structured_data = {}
    try:
        query = "SELECT * FROM system.metrics"
        formatted, raw = connector.execute_query(query, return_raw=True)
        adoc_content.append(formatted)
        structured_data["system_metrics"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not retrieve system metrics: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
