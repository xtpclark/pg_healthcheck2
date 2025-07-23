def run_pub_sub(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes logical replication setup, including publications and subscriptions, to ensure data consistency and performance.
    """
    adoc_content = ["Analyzes logical replication setup, including publications and subscriptions, to ensure data consistency and performance.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Logical replication queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete FROM pg_publication;")
        adoc_content.append("SELECT subname, subenabled, subslotname, subconninfo FROM pg_subscription WHERE subdbid = (SELECT oid FROM pg_database WHERE datname = %(database)s);")
        adoc_content.append("SELECT subname, received_lsn, latest_end_lsn, latest_end_time FROM pg_stat_subscription WHERE subid = (SELECT oid FROM pg_database WHERE datname = %(database)s);")
        adoc_content.append("----")

    queries = [
        (
            "Publications", 
            "SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete FROM pg_publication;", 
            True,
            "publications" # Data key
        ),
        (
            "Subscriptions", 
            "SELECT subname, subenabled, subslotname, subconninfo FROM pg_subscription WHERE subdbid = (SELECT oid FROM pg_database WHERE datname = %(database)s);", 
            True,
            "subscriptions" # Data key
        ),
        (
            "Subscription Status", 
            "SELECT subname, received_lsn, latest_end_lsn, latest_end_time FROM pg_stat_subscription WHERE subid = (SELECT oid FROM pg_database WHERE datname = %(database)s);", 
            True,
            "subscription_status" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = {'database': settings['database']} if '%(database)s' in query else None
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\nEnsure publications and subscriptions are correctly configured for logical replication. Monitor subscription status for lag (received_lsn vs. latest_end_lsn). For Aurora, consider read replicas for high availability instead of logical replication if applicable.\n====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nAWS RDS Aurora primarily uses physical replication. Logical replication may require additional configuration and is less common in Aurora setups.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

