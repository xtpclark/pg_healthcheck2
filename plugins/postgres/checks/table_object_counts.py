from plugins.postgres.utils.qrylib.table_object_counts import get_object_counts_query

def get_weight():
    """Returns the importance score for this module."""
    return 10 # Core configuration, highest importance


def run_table_object_counts(connector, settings):
    """
    Counts various types of PostgreSQL database objects to provide an
    overview of the database structure and complexity.
    """
    adoc_content = ["=== Database Object Counts", "Provides a summary count of various database object types to give a high-level view of the database's complexity.\n"]
    structured_data = {}

    try:
        # Get the single, consolidated query from the compatibility module
        object_counts_query = get_object_counts_query(connector)

        if settings.get('show_qry') == 'true':
            adoc_content.append("Database object counts query:")
            adoc_content.append("[,sql]\n----")
            adoc_content.append(object_counts_query)
            adoc_content.append("----")

        # Execute the query
        formatted_result, raw_result = connector.execute_query(object_counts_query, return_raw=True)

        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["object_counts_summary"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nCould not retrieve database object counts.\n====\n")
            structured_data["object_counts_summary"] = {"status": "success", "data": {}}
        else:
            # The raw_result is a list with a single dictionary, e.g., [{'tables': 10, 'indexes': 15}]
            counts_data = raw_result[0]
            
            # --- Format for AsciiDoc Report ---
            adoc_content.append("Summary of Database Objects")
            table = ['[cols="2,1",options="header"]', '|===', '| Object Type | Count']
            for obj_type, count in counts_data.items():
                # Make the key more readable for the report
                readable_type = obj_type.replace('_', ' ').title()
                table.append(f"| {readable_type} | {count}")
            table.append('|===')
            adoc_content.append('\n'.join(table))
            
            # --- Store the raw dictionary as the summary for AI Analysis ---
            structured_data["object_counts_summary"] = {"status": "success", "data": counts_data}

    except Exception as e:
        error_msg = f"Failed during object count analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["object_counts_summary"] = {"status": "error", "details": str(e)}

    adoc_content.append("\n[TIP]\n====\n"
                       "Monitoring object counts provides a high-level view of database complexity and growth. "
                       "A sudden increase in certain object types might indicate application changes or unexpected behavior. "
                       "High numbers of indexes should prompt a review of their necessity to avoid performance overhead on write operations.\n"
                       "====\n")

    return "\n".join(adoc_content), structured_data
