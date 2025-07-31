from plugins.postgres.utils.qrylib.superuser_reserved_conns import get_superuser_reserved

def get_weight():
    """Returns the importance score for this module."""
    return 5 # Core availability setting

def run_superuser_reserved(connector, settings):
    """Checks the number of connections reserved for superusers.

    This check queries the `superuser_reserved_connections` setting
    from `pg_settings` to ensure that a specific number of connections
    are held back for administrative access, which is crucial for
    emergency troubleshooting.

    Args:
        connector (object): The database connector object used to execute queries.
        settings (dict): A dictionary of settings for the check, such as `show_qry`.

    Returns:
        tuple: A tuple where the first element is the AsciiDoc report string
               and the second is the structured dictionary of findings.
    """
    adoc_content = ["=== Superuser Reserved Connections", "Ensures that connections are reserved for superuser accounts to maintain administrative access."]
    structured_data = {}
    data_key = "superuser_reserved"

    try:
        query = get_superuser_reserved(connector)
        title = "Superuser Reserved Connections"
        
        adoc_content.append(f"\n==== {title}")

        if settings.get('show_qry'):
            adoc_content.append("\n[,sql]\n----\n" + query + "\n----")

        formatted_result, raw_result = connector.execute_query(query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(formatted_result)
            # Check if raw_result is not empty and contains the expected data
            if raw_result and isinstance(raw_result, list) and len(raw_result) > 0:
                # **FIX**: Access the value by column name 'setting' instead of index [0]
                reserved_conns_value = raw_result[0]['setting']
                structured_data[data_key] = {
                    "status": "success",
                    "data": {
                        "superuser_reserved_connections": int(reserved_conns_value)
                    }
                }
            else:
                error_msg = "Query did not return the expected data for superuser_reserved_connections."
                adoc_content.append(f"\n[WARNING]\n====\n{error_msg}\n====\n")
                structured_data[data_key] = {"status": "error", "details": error_msg}

    except Exception as e:
        # This block should no longer be triggered by this specific error
        adoc_content.append(f"\n[ERROR]\n====\nCould not execute check for 'Superuser Reserved Connections': {e}\n====\n")
        structured_data[data_key] = {"status": "error", "details": str(e)}

    return "\n".join(adoc_content), structured_data
