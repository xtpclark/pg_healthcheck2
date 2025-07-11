def run_rds_upgrade(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Provides recommendations and checks related to AWS RDS/Aurora PostgreSQL upgrades.
    """
    adoc_content = ["Provides recommendations and checks related to AWS RDS/Aurora PostgreSQL upgrades."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Upgrade considerations are primarily textual advice.")
        adoc_content.append("----")

    # This module primarily provides static recommendations based on general best practices.
    # In a more advanced setup, it could analyze the 'all_structured_findings'
    # to provide more tailored recommendations, or integrate with AWS APIs.

    adoc_content.append("[TIP]\n====\n"
                   "Regularly review new major and minor versions of PostgreSQL and Aurora for new features, "
                   "performance improvements, and security patches. Plan upgrades carefully, testing application "
                   "compatibility in a staging environment. Consider blue/green deployments or logical replication "
                   "for minimal downtime upgrades. Always back up your database before a major upgrade.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS and Aurora simplify major and minor version upgrades. "
                       "For major version upgrades, consider using a blue/green deployment or creating a new "
                       "read replica from your existing instance, upgrading the replica, and then promoting it. "
                       "Monitor `UpgradeReadiness` events in CloudWatch. "
                       "Ensure your application is compatible with the target PostgreSQL version.\n"
                       "====\n")
    else:
        adoc_content.append("[NOTE]\n====\n"
                       "This section primarily focuses on AWS RDS/Aurora upgrade considerations. "
                       "For self-hosted PostgreSQL, upgrade processes involve different steps, "
                       "such as `pg_upgrade` for major versions or in-place updates for minor versions. "
                       "Always consult PostgreSQL documentation for specific version upgrade paths.\n"
                       "====\n")
    
    # For this module, structured data might be simple, indicating its status.
    structured_data["upgrade_recommendations_status"] = {"status": "success", "note": "General upgrade recommendations provided."}

    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

