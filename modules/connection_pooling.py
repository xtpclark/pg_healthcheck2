def run_connection_pooling(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes connection pooling statistics (e.g., PgBouncer) to optimize
    connection management and reduce overhead.
    """
#    adoc_content = ["=== Connection Pooling Analysis", "Analyzes connection pooling statistics to optimize connection management."]
    adoc_content = ["Analyzes connection pooling statistics to optimize connection management.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Check if pgbouncer_cmd is configured in settings
    pgbouncer_configured = 'pgbouncer_cmd' in settings and settings['pgbouncer_cmd']

    if settings['show_qry'] == 'true' and pgbouncer_configured:
        adoc_content.append("PgBouncer statistics commands:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SHOW STATS;")
        adoc_content.append("SHOW CLIENTS;")
        adoc_content.append("SHOW SERVERS;")
        adoc_content.append("----")
    elif settings['show_qry'] == 'true' and not pgbouncer_configured:
        adoc_content.append("PgBouncer commands are not shown as 'pgbouncer_cmd' is not configured in config.yaml.")
        adoc_content.append("----")

    queries = [
        (
            "PgBouncer Pool Statistics", 
            "SHOW STATS;", 
            pgbouncer_configured, # Condition: Only if pgbouncer_cmd is configured
            "pgbouncer_stats" # Data key
        ),
        (
            "PgBouncer Client Connections", 
            "SHOW CLIENTS;", 
            pgbouncer_configured, # Condition: Only if pgbouncer_cmd is configured
            "pgbouncer_clients" # Data key
        ),
        (
            "PgBouncer Server Connections", 
            ""
            "SHOW SERVERS;", 
            pgbouncer_configured, # Condition: Only if pgbouncer_cmd is configured
            "pgbouncer_servers" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            note_msg = "PgBouncer command not configured or query not applicable."
            adoc_content.append(f"{title}\n[NOTE]\n====\n{note_msg}\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": note_msg}
            continue
        
        # Execute PgBouncer command (using execute_pgbouncer helper)
        # Note: execute_pgbouncer returns a string, not raw data like execute_query
        # We'll parse its string output into structured data here.
        pgbouncer_output_string = execute_pgbouncer(query)
        
        # Parse PgBouncer output (simple parsing for common SHOW commands)
        parsed_data = []
        status = "success"
        details = None

        if "[ERROR]" in pgbouncer_output_string:
            status = "error"
            details = pgbouncer_output_string.strip()
            adoc_content.append(f"{title}\n[ERROR]\n====\nPgBouncer command failed: {details}\n====\n")
        elif not pgbouncer_output_string.strip():
            status = "success" # No error, but no data
            details = "No data returned from PgBouncer."
            adoc_content.append(f"{title}\n[NOTE]\n====\n{details}\n====\n")
        else:
            # Basic parsing for SHOW STATS, SHOW CLIENTS, SHOW SERVERS
            lines = pgbouncer_output_string.strip().split('\n')
            if len(lines) > 1:
                headers = [h.strip() for h in lines[0].strip('|').split('|')]
                for line in lines[1:]:
                    if line.strip() and line.startswith('|'):
                        values = [v.strip() for v in line.strip('|').split('|')]
                        if len(headers) == len(values):
                            row_dict = {}
                            for i, header in enumerate(headers):
                                try:
                                    # Attempt to convert to int/float if possible
                                    if '.' in values[i]:
                                        row_dict[header] = float(values[i])
                                    else:
                                        row_dict[header] = int(values[i])
                                except ValueError:
                                    row_dict[header] = values[i]
                            parsed_data.append(row_dict)
            
            if not parsed_data:
                status = "success"
                details = "PgBouncer command returned no structured data."
                adoc_content.append(f"{title}\n[NOTE]\n====\n{details}\n====\n")
            else:
                # Re-format for AsciiDoc table display
                adoc_table = ['|===', '|' + '|'.join(headers)]
                for row_dict in parsed_data:
                    adoc_table.append('|' + '|'.join(str(row_dict[h]) for h in headers))
                adoc_table.append('|===')
                adoc_content.append(title)
                adoc_content.append('\n'.join(adoc_table))

        structured_data[data_key] = {"status": status, "data": parsed_data if parsed_data else details}
    
    adoc_content.append("[TIP]\n====\n"
                   "Connection pooling (e.g., PgBouncer) is crucial for managing database connections efficiently, "
                   "reducing overhead from frequent connection/disconnection, and limiting the number of active backend connections. "
                   "Monitor `active_connections`, `waiting_connections`, and `total_requests` to ensure your pooler is effectively handling load. "
                   "Tune pooler settings (`pool_size`, `reserve_pool_size`, `server_lifetime`) for optimal performance.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "For AWS RDS Aurora, consider using Amazon RDS Proxy as a fully managed connection pooler. "
                       "RDS Proxy can significantly improve application scalability and resilience by managing connection lifecycle, "
                       "especially for serverless or highly concurrent applications. "
                       "It also integrates with AWS Secrets Manager for credential management.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

