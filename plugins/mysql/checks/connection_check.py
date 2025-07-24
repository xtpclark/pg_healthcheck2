def run_connection_check(connector, settings):
    """
    Performs a simple connection check and retrieves basic version info.
    """
    adoc_content = ["=== MySQL Connection Check"]
    structured_data = {}
    
    try:
        # The connector already has the version info
        version = connector.version_info.get('version_string', 'Unknown')
        adoc_content.append(f"Successfully connected to MySQL Server Version: {version}")
        structured_data["version_info"] = connector.version_info
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not retrieve version info: {e}\n====\n")

    return "\n".join(adoc_content), structured_data
