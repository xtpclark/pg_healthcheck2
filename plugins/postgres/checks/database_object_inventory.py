from plugins.postgres.utils.qrylib.object_inventory import get_object_inventory_query

def get_weight():
    """Returns the importance score for this module."""
    return 5

def run_object_inventory(connector, settings=None):
    """
    Collects a comprehensive inventory of all database objects to track
    additions and removals over time.
    """
    adoc_content = ["=== Database Object Inventory", "A list of all objects, used for tracking schema changes."]
    structured_data = {}

    try:
        query = get_object_inventory_query(connector)
        
        # Execute the query, getting both the pre-formatted AsciiDoc table
        # and the raw data for structured findings.
        formatted_table, raw_data = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted_table:
            adoc_content.append(formatted_table)
            structured_data["database_object_inventory"] = {"status": "error", "data": raw_data}
        elif not raw_data:
            adoc_content.append("\n[NOTE]\n====\nNo database objects found in user schemas.\n====\n")
            structured_data["database_object_inventory"] = {"status": "success", "data": []}
        else:
            # For structured findings, we want the raw, detailed list.
            structured_data["database_object_inventory"] = {"status": "success", "data": raw_data}
            
            # For the AsciiDoc report, we use the pre-formatted table from the connector.
            adoc_content.append(f"\n{formatted_table}")

    except Exception as e:
        error_msg = f"\n[ERROR]\n====\nCould not retrieve object inventory: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["database_object_inventory"] = {"status": "error", "error": str(e)}

    return "\n".join(adoc_content), structured_data
