from plugins.postgres.utils.qrylib.missing_primary_keys import get_missing_primary_keys_query

def get_weight():
    """Returns the importance score for this module."""
    # Data integrity is a high-priority concern.
    return 8

def run_missing_primary_keys(connector, settings):
    """
    Identifies tables that are missing a Primary Key, which is essential
    for data integrity and efficient row identification.
    """
    adoc_content = ["=== Missing Primary Keys Analysis", "Checks for tables lacking a Primary Key. Every table should have a primary key to uniquely identify rows, which is critical for data integrity, replication, and performance.\n"]
    structured_data = {}

    try:
        query = get_missing_primary_keys_query()

        if settings.get('show_qry') == 'true':
            adoc_content.append("Missing primary key query:")
            adoc_content.append(f"[,sql]\n----\n{query}\n----")

        formatted_result, raw_result = connector.execute_query(query, return_raw=True)

        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["no_pk_tables"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nAll regular tables have a Primary Key. This is excellent for data integrity.\n====\n")
            structured_data["no_pk_tables"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nThe following tables are missing a Primary Key. This can lead to duplicate rows and makes it difficult for applications and logical replication tools to reliably identify and modify specific rows.\n====\n")
            adoc_content.append(formatted_result)
            structured_data["no_pk_tables"] = {"status": "success", "data": raw_result}

    except Exception as e:
        error_msg = f"Failed during primary key analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["no_pk_tables"] = {"status": "error", "details": str(e)}

    adoc_content.append("\n[TIP]\n====\nTo add a primary key, use the `ALTER TABLE <table_name> ADD PRIMARY KEY (<column_name>);` command. If no single column is unique, you can create a composite primary key. If no natural key exists, consider adding an identity or serial column.\n====\n")

    return "\n".join(adoc_content), structured_data
