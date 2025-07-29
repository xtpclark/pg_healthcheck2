from plugins.postgres.utils.qrylib.postgres_overview import (
    get_version_query,
    get_database_size_query,
    get_uptime_query,
    get_key_config_query
)

def get_weight():
    """Returns the importance score for this module."""
    return 10 # Core configuration, highest importance

def run_postgres_overview(connector, settings):
    """
    Provides an overview of the PostgreSQL database, including version, uptime, size, and key configuration settings.
    """
    adoc_content = ["=== PostgreSQL Overview", "Provides a high-level overview of the database instance."]
    structured_data = {}

    queries_to_run = [
        ("Database Version", get_version_query, "version_info"),
        ("Database Size", get_database_size_query, "database_size"),
        ("Uptime", get_uptime_query, "uptime"),
        ("Key Configuration Settings", get_key_config_query, "key_config")
    ]

    for title, query_func, data_key in queries_to_run:
        try:
            query = query_func(connector)
            adoc_content.append(f"\n==== {title}")

            if settings.get('show_qry'):
                adoc_content.append("\n[,sql]\n----\n" + query + "\n----")

            formatted_result, raw_result = connector.execute_query(query, return_raw=True)
            
            if "[ERROR]" in formatted_result:
                adoc_content.append(formatted_result)
                structured_data[data_key] = {"status": "error", "details": raw_result}
            else:
                adoc_content.append(formatted_result)
                structured_data[data_key] = {"status": "success", "data": raw_result}
        
        except Exception as e:
            adoc_content.append(f"\n[ERROR]\n====\nCould not execute check for '{title}': {e}\n====\n")
            structured_data[data_key] = {"status": "error", "details": str(e)}

    return "\n".join(adoc_content), structured_data
