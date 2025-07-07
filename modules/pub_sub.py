def run_pub_sub(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== Logical Replication (Publications and Subscriptions)", "Analyzes logical replication setup, including publications and subscriptions, to ensure data consistency and performance."]
    
    if settings['show_qry'] == 'true':
        content.append("Logical replication queries:")
        content.append("[,sql]\n----")
        content.append("SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete FROM pg_publication;")
        content.append("SELECT subname, subenabled, subslotname, subconninfo FROM pg_subscription WHERE subdbid = (SELECT oid FROM pg_database WHERE datname = %(database)s);")
        content.append("SELECT subname, received_lsn, latest_end_lsn, latest_end_time FROM pg_stat_subscription WHERE subid = (SELECT oid FROM pg_database WHERE datname = %(database)s);")
        content.append("----")

    queries = [
        ("Publications", "SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete FROM pg_publication;", True),
        ("Subscriptions", "SELECT subname, subenabled, subslotname, subconninfo FROM pg_subscription WHERE subdbid = (SELECT oid FROM pg_database WHERE datname = %(database)s);", True),
        ("Subscription Status", "SELECT subname, received_lsn, latest_end_lsn, latest_end_time FROM pg_stat_subscription WHERE subid = (SELECT oid FROM pg_database WHERE datname = %(database)s);", True)
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
            continue
        params = {'database': settings['database']} if '%(database)s' in query else None
        result = execute_query(query, params=params)
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"{title}\n{result}")
        else:
            content.append(title)
            content.append(result)
    
    content.append("[TIP]\n====\nEnsure publications and subscriptions are correctly configured for logical replication. Monitor subscription status for lag (received_lsn vs. latest_end_lsn). For Aurora, consider read replicas for high availability instead of logical replication if applicable.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nAWS RDS Aurora primarily uses physical replication. Logical replication may require additional configuration and is less common in Aurora setups.\n====")
    
    return "\n".join(content)
