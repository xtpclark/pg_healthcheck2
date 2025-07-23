def run_hba_rules_audit(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Audits pg_hba.conf rules for security best practices and identifies potential security risks.
    This module analyzes the current HBA configuration and provides recommendations for improvement.
    """
    adoc_content = ["=== HBA Rules Security Audit", "Analyzes pg_hba.conf rules for security best practices and potential risks.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Get PostgreSQL version for compatibility
    version_query = "SELECT version();"
    version_result, _ = execute_query(version_query, return_raw=True)
    
    # Extract version number for compatibility checks
    pg_version = None
    if version_result and not isinstance(version_result, str):
        try:
            version_str = version_result[0]['version'] if isinstance(version_result, list) and version_result else str(version_result)
            # Extract version number (e.g., "PostgreSQL 15.3" -> 15)
            import re
            version_match = re.search(r'PostgreSQL (\d+)', version_str)
            if version_match:
                pg_version = int(version_match.group(1))
        except (IndexError, AttributeError, ValueError):
            pg_version = 13  # Default to minimum supported version
    
    # Check if pg_hba_file_rules view is available
    check_hba_view_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.views 
            WHERE table_name = 'pg_hba_file_rules'
        );
    """
    
    hba_view_available = False
    try:
        formatted_check_result, raw_check_result = execute_query(check_hba_view_query, return_raw=True)
        if raw_check_result and isinstance(raw_check_result, list) and len(raw_check_result) > 0:
            hba_view_available = raw_check_result[0].get('exists', False)
    except:
        hba_view_available = False
    
    # Define queries for HBA analysis based on availability
    if hba_view_available:
        # Try to get column information for pg_hba_file_rules
        column_check_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'pg_hba_file_rules' 
            ORDER BY ordinal_position;
        """
        
        try:
            formatted_col_result, raw_col_result = execute_query(column_check_query, return_raw=True)
            available_columns = [col['column_name'] for col in raw_col_result] if raw_col_result else []
            
            # Build query based on available columns
            select_columns = []
            if 'type' in available_columns:
                select_columns.append('type')
            if 'database' in available_columns:
                select_columns.append('database')
            if 'user' in available_columns:
                select_columns.append('user')
            if 'address' in available_columns:
                select_columns.append('address')
            if 'method' in available_columns:
                select_columns.append('method')
                select_columns.append("""
                    CASE 
                        WHEN method = 'trust' THEN 'CRITICAL'
                        WHEN method = 'password' THEN 'HIGH'
                        WHEN method = 'md5' THEN 'MEDIUM'
                        WHEN method = 'scram-sha-256' THEN 'LOW'
                        WHEN method = 'peer' THEN 'LOW'
                        WHEN method = 'ident' THEN 'MEDIUM'
                        WHEN method = 'cert' THEN 'LOW'
                        WHEN method = 'pam' THEN 'MEDIUM'
                        ELSE 'UNKNOWN'
                    END as risk_level
                """)
                select_columns.append("""
                    CASE 
                        WHEN method = 'trust' THEN 'No authentication required - extremely dangerous'
                        WHEN method = 'password' THEN 'Password sent in plain text'
                        WHEN method = 'md5' THEN 'MD5 hashed password (deprecated)'
                        WHEN method = 'scram-sha-256' THEN 'SCRAM-SHA-256 (recommended)'
                        WHEN method = 'peer' THEN 'OS-level authentication'
                        WHEN method = 'ident' THEN 'OS username matching (deprecated)'
                        WHEN method = 'cert' THEN 'SSL certificate authentication'
                        WHEN method = 'pam' THEN 'PAM authentication'
                        ELSE 'Unknown authentication method'
                    END as description
                """)
            
            if select_columns:
                hba_rules_query = f"""
                    SELECT {', '.join(select_columns)}
                    FROM pg_hba_file_rules 
                    WHERE type IN ('host', 'hostssl', 'hostnossl', 'local')
                    ORDER BY type, database, user;
                """
            else:
                # Fallback to basic query
                hba_rules_query = """
                    SELECT type, database, user, address
                    FROM pg_hba_file_rules 
                    WHERE type IN ('host', 'hostssl', 'hostnossl', 'local')
                    ORDER BY type, database, user;
                """
                
        except:
            # Fallback to basic query
            hba_rules_query = """
                SELECT type, database, user, address
                FROM pg_hba_file_rules 
                WHERE type IN ('host', 'hostssl', 'hostnossl', 'local')
                ORDER BY type, database, user;
            """
    else:
        # pg_hba_file_rules not available, provide alternative analysis
        adoc_content.append("[NOTE]\n====\n")
        adoc_content.append("**pg_hba_file_rules view not available**\n")
        adoc_content.append("The pg_hba_file_rules view is not available in this PostgreSQL installation. ")
        adoc_content.append("This view requires PostgreSQL 9.5+ and appropriate permissions.\n")
        adoc_content.append("====\n")
        
        # Provide manual analysis guidance instead
        adoc_content.append("=== Manual HBA Analysis Required")
        adoc_content.append("\nSince automated HBA analysis is not available, please manually review your pg_hba.conf file:\n")
        adoc_content.append("\n[TIP]\n====\n**Manual HBA Review Checklist:**\n")
        adoc_content.append("* Check for `trust` authentication methods (critical risk)\n")
        adoc_content.append("* Look for `password` or `md5` methods (deprecated)\n")
        adoc_content.append("* Verify SSL enforcement with `hostssl` entries\n")
        adoc_content.append("* Review overly permissive rules (e.g., `all` databases/users)\n")
        adoc_content.append("* Check for specific IP restrictions vs. wide ranges\n")
        adoc_content.append("====\n")
        
        structured_data["hba_rules_analysis"] = {"status": "manual_required", "reason": "pg_hba_file_rules not available"}
        
        # Skip to SSL configuration analysis
        hba_rules_query = None
    
    # Query for SSL configuration
    ssl_config_query = """
        SELECT 
            name, 
            setting, 
            unit, 
            short_desc 
        FROM pg_settings 
        WHERE name IN ('ssl', 'ssl_cert_file', 'ssl_key_file', 'ssl_ca_file', 'ssl_crl_file')
        ORDER BY name;
    """
    
    # Query for connection statistics by authentication method
    auth_stats_query = """
        SELECT 
            auth_method,
            COUNT(*) as connection_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM (
            SELECT 
                CASE 
                    WHEN ssl = 't' THEN 'SSL'
                    ELSE 'Non-SSL'
                END as auth_method
            FROM pg_stat_ssl ssl
            JOIN pg_stat_activity act ON ssl.pid = act.pid
            WHERE act.datname = %(database)s
        ) auth_methods
        GROUP BY auth_method
        ORDER BY connection_count DESC;
    """
    
    if settings['show_qry'] == 'true':
        adoc_content.append("HBA rules audit queries:")
        adoc_content.append("[,sql]\n----")
        if hba_rules_query:
            adoc_content.append(hba_rules_query)
        adoc_content.append(ssl_config_query)
        adoc_content.append(auth_stats_query)
        adoc_content.append("----")

    # Execute HBA rules analysis if query is available
    if hba_rules_query:
        params_for_hba = None
        formatted_hba_result, raw_hba_result = execute_query(hba_rules_query, params=params_for_hba, return_raw=True)
        
        if "[ERROR]" in formatted_hba_result:
            adoc_content.append(f"HBA Rules Analysis\n{formatted_hba_result}")
            structured_data["hba_rules_analysis"] = {"status": "error", "details": raw_hba_result}
        else:
            adoc_content.append("HBA Rules Analysis")
            adoc_content.append(formatted_hba_result)
            structured_data["hba_rules_analysis"] = {"status": "success", "data": raw_hba_result}
            
            # Analyze the results for security recommendations
            if raw_hba_result and isinstance(raw_hba_result, list):
                critical_issues = []
                high_issues = []
                medium_issues = []
                
                for rule in raw_hba_result:
                    risk_level = rule.get('risk_level', 'UNKNOWN')
                    if risk_level == 'CRITICAL':
                        critical_issues.append(f"{rule.get('type', 'unknown')} {rule.get('database', 'unknown')} {rule.get('user', 'unknown')} using {rule.get('method', 'unknown')}")
                    elif risk_level == 'HIGH':
                        high_issues.append(f"{rule.get('type', 'unknown')} {rule.get('database', 'unknown')} {rule.get('user', 'unknown')} using {rule.get('method', 'unknown')}")
                    elif risk_level == 'MEDIUM':
                        medium_issues.append(f"{rule.get('type', 'unknown')} {rule.get('database', 'unknown')} {rule.get('user', 'unknown')} using {rule.get('method', 'unknown')}")
                
                if critical_issues:
                    adoc_content.append("\n[WARNING]\n====\n**CRITICAL SECURITY ISSUES FOUND:**\n")
                    for issue in critical_issues:
                        adoc_content.append(f"* {issue}\n")
                    adoc_content.append("====\n")
                
                if high_issues:
                    adoc_content.append("\n[CAUTION]\n====\n**HIGH RISK ISSUES FOUND:**\n")
                    for issue in high_issues:
                        adoc_content.append(f"* {issue}\n")
                    adoc_content.append("====\n")
                
                if medium_issues:
                    adoc_content.append("\n[NOTE]\n====\n**MEDIUM RISK ISSUES FOUND:**\n")
                    for issue in medium_issues:
                        adoc_content.append(f"* {issue}\n")
                    adoc_content.append("====\n")

    # Execute SSL configuration analysis
    formatted_ssl_result, raw_ssl_result = execute_query(ssl_config_query, return_raw=True)
    
    if "[ERROR]" in formatted_ssl_result:
        adoc_content.append(f"SSL Configuration\n{formatted_ssl_result}")
        structured_data["ssl_configuration"] = {"status": "error", "details": raw_ssl_result}
    else:
        adoc_content.append("=== SSL Configuration")
        adoc_content.append(formatted_ssl_result)
        structured_data["ssl_configuration"] = {"status": "success", "data": raw_ssl_result}

    # Execute authentication statistics
    params_for_auth = {'database': settings['database']}
    formatted_auth_result, raw_auth_result = execute_query(auth_stats_query, params=params_for_auth, return_raw=True)
    
    if "[ERROR]" in formatted_auth_result:
        adoc_content.append(f"Authentication Statistics\n{formatted_auth_result}")
        structured_data["authentication_statistics"] = {"status": "error", "details": raw_auth_result}
    else:
        adoc_content.append("=== Authentication Statistics")
        adoc_content.append(formatted_auth_result)
        structured_data["authentication_statistics"] = {"status": "success", "data": raw_auth_result}

    # Add security recommendations
    adoc_content.append("\n==== Security Recommendations")
    
    adoc_content.append("\n[TIP]\n====\n**HBA Security Best Practices:**\n")
    adoc_content.append("* **Use `scram-sha-256` authentication** instead of `md5` or `password`\n")
    adoc_content.append("* **Avoid `trust` authentication** - it requires no password\n")
    adoc_content.append("* **Use `hostssl`** instead of `host` for encrypted connections\n")
    adoc_content.append("* **Restrict database access** by specifying exact databases/users\n")
    adoc_content.append("* **Use SSL certificates** for client authentication when possible\n")
    adoc_content.append("* **Limit `all` database/user entries** to specific IP ranges\n")
    adoc_content.append("====\n")
    
    adoc_content.append("\n[WARNING]\n====\n**Common Security Issues to Check:**\n")
    adoc_content.append("* **`trust` method**: Allows connections without passwords\n")
    adoc_content.append("* **`password` method**: Sends passwords in plain text\n")
    adoc_content.append("* **`md5` method**: Uses deprecated MD5 hashing\n")
    adoc_content.append("* **`ident` method**: Deprecated, use `peer` instead\n")
    adoc_content.append("* **Overly permissive rules**: `all` databases/users with wide IP ranges\n")
    adoc_content.append("* **Missing SSL enforcement**: `host` instead of `hostssl`\n")
    adoc_content.append("====\n")
    
    if settings['is_aurora'] == 'true':
        adoc_content.append("\n[NOTE]\n====\n**AWS RDS Aurora Considerations:**\n")
        adoc_content.append("* Aurora uses AWS security groups for network-level access control\n")
        adoc_content.append("* HBA rules are managed through RDS parameter groups\n")
        adoc_content.append("* SSL is typically enabled by default in Aurora\n")
        adoc_content.append("* Consider using IAM database authentication for additional security\n")
        adoc_content.append("====\n")
    
    # Add version-specific recommendations
    if pg_version and pg_version >= 14:
        adoc_content.append("\n[NOTE]\n====\n**PostgreSQL 14+ Security Features:**\n")
        adoc_content.append("* **SCRAM-SHA-256** is the default authentication method\n")
        adoc_content.append("* **Enhanced SSL/TLS** support with better cipher selection\n")
        adoc_content.append("* **Client certificate authentication** improvements\n")
        adoc_content.append("====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
