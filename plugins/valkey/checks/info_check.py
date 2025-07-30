def run_info_check(connector, settings):
    """Retrieves and formats the output of the INFO command."""
    adoc_content = ["=== Valkey Server Information"]
    structured_data = {}
    try:
        info = connector.client.info()
        
        # --- AsciiDoc Formatting Logic ---
        table = ['[cols="2,1",options="header"]', '|===', '| Metric | Value']
        # We'll just show a few key metrics for this example
        key_metrics = ['valkey_version', 'tcp_port', 'uptime_in_days', 'connected_clients', 'used_memory_human']
        for key in key_metrics:
            if key in info:
                readable_key = key.replace('_', ' ').title()
                table.append(f"| {readable_key} | {info[key]}")
        table.append('|===')
        
        adoc_content.append('\n'.join(table))
        structured_data["server_info"] = {"status": "success", "data": info}

    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not retrieve server info: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
