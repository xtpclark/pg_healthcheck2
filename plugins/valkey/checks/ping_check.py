def run_ping_check(connector, settings):
    """Performs a simple PING check to verify connectivity."""
    adoc_content = ["=== Valkey Connection Check"]
    structured_data = {}
    try:
        response = connector.client.ping()
        adoc_content.append(f"PING response: {response}")
        structured_data["ping"] = {"status": "success", "data": {'response': response}}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not ping Valkey server: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
