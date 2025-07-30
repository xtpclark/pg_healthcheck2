from plugins.postgres.utils.qrylib.extensions_update_check import get_available_extensions_query

def get_weight():
    """Returns the importance score for this module."""
    return 4 # Core configuration, highest importance

def run_extensions_update_check(connector, settings):
    """
    Checks for installed extensions that have available updates.
    """
    adoc_content = ["=== Available Extension Updates"]
    structured_data = {}

    try:
        query = get_available_extensions_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["extension_updates"] = {"status": "error", "data": raw}
        elif not raw:
            adoc_content.append("[NOTE]\n====\nAll installed extensions are up to date.\n====\n")
            structured_data["extension_updates"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nOutdated extensions can have security vulnerabilities or bugs. Use `ALTER EXTENSION extension_name UPDATE;` to update them.\n====\n")
            adoc_content.append(formatted)
            structured_data["extension_updates"] = {"status": "success", "data": raw}

    except Exception as e:
        error_msg = f"[ERROR]\n====\nCould not check for extension updates: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["extension_updates"] = {"status": "error", "error": str(e)}

    return "\n".join(adoc_content), structured_data
