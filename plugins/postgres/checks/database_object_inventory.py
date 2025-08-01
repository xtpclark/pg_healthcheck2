from plugins.postgres.utils.qrylib.object_inventory import get_object_inventory_query

def get_weight():
    """Returns the importance score for this module."""
    return 5

def run_database_object_inventory_query(connector, settings=None):
    """
    Collects a comprehensive inventory of all database objects.
    The full list can be excluded from the text report for brevity.
    """
    adoc_content = ["=== Database Object Inventory", "A list of all objects, used for tracking schema changes."]
    structured_data = {}
    settings = settings or {} # Ensure settings is a dict to prevent errors on .get()

    try:
        query = get_object_inventory_query(connector)
        formatted_table, raw_data = connector.execute_query(query, return_raw=True)

        # First, handle a potential query error
        if "[ERROR]" in formatted_table:
            adoc_content.append(formatted_table)
            structured_data["database_object_inventory"] = {"status": "error", "data": raw_data}
            return "\n".join(adoc_content), structured_data

        # If the query was successful, always populate the structured data
        structured_data["database_object_inventory"] = {"status": "success", "data": raw_data}

        # Now, conditionally add the full list to the text report based on the setting
        if settings.get('include_object_inventory_in_report', False):
            if not raw_data:
                adoc_content.append("\n[NOTE]\n====\nNo database objects found in user schemas.\n====\n")
            else:
                adoc_content.append(f"\n{formatted_table}")
        else:
            # If omitting from the report, add a note explaining why
            adoc_content.append("\n\n[NOTE]\n====\nThe full object inventory has been omitted from this text report for brevity but is available in the structured JSON output.\n====\n")

    except Exception as e:
        error_msg = f"\n[ERROR]\n====\nCould not retrieve object inventory: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["database_object_inventory"] = {"status": "error", "error": str(e)}

    return "\n".join(adoc_content), structured_data
