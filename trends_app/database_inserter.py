"""
Database insertion module for health check submissions.

This module provides self-contained database insertion logic for the trends_app
submission API. It does NOT depend on output_handlers/trend_shipper.py, making
trends_app fully independent.

This is essentially a copy of the core database insertion logic from trend_shipper.py,
adapted to work within the Flask app context.
"""

import json
import re
import psycopg2
from datetime import datetime
from flask import current_app


def insert_health_check(db_config, target_info, findings_json,
                       structured_findings, adoc_content, analysis_results,
                       api_key_id=None, submitted_from_ip=None):
    """
    Insert health check data with full metadata extraction and triggered rules.

    This is the main entry point for database insertion from the submission API.
    It handles:
    - Company lookup/creation
    - Metadata extraction (version, cluster, nodes)
    - pgcrypto encryption of findings
    - Triggered rules insertion
    - API key usage tracking

    Args:
        db_config (dict): PostgreSQL connection parameters
        target_info (dict): Target system information (host, port, db_type, etc.)
        findings_json (str): JSON string of structured findings
        structured_findings (dict): Parsed findings dictionary
        adoc_content (str): AsciiDoc report content
        analysis_results (dict): Analysis results with triggered rules
        api_key_id (int, optional): ID of the API key used for submission
        submitted_from_ip (str, optional): IP address of the submitter

    Returns:
        int: The inserted run_id

    Raises:
        Exception: If database insertion fails
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        current_app.logger.info(
            f"Inserting health check for {target_info.get('db_type')}:"
            f"{target_info.get('host')}:{target_info.get('port')}"
        )

        # 1. Get or create company
        company_name = target_info.get('company_name', 'Default Company')
        cursor.execute("SELECT get_or_create_company(%s);", (company_name,))
        company_id = cursor.fetchone()[0]

        # 2. Extract metadata from findings
        db_version = _extract_db_version(structured_findings)
        db_version_major, db_version_minor = _parse_version_components(db_version)
        cluster_name = _extract_cluster_name(target_info, structured_findings)
        node_count = _extract_node_count(structured_findings)
        infrastructure_metadata = _extract_infrastructure_metadata(structured_findings)

        # 3. Extract execution context
        context = structured_findings.get('execution_context', {})
        run_by_user = context.get('run_by_user', 'api_submission')
        run_from_host = context.get('run_from_host', 'api')
        tool_version = context.get('tool_version', '2.1.0')
        prompt_template_name = structured_findings.get('prompt_template_name')

        # 4. Extract AI execution context
        ai_context = context.get('ai_execution_metrics')
        ai_context_json = json.dumps(ai_context) if ai_context else None

        # 5. Extract health score from analysis results
        health_score = None
        if analysis_results:
            health_score = analysis_results.get('health_score')

        # 6. Encrypt findings using pgcrypto
        encrypted_findings = _encrypt_findings_pgcrypto(cursor, findings_json)

        # 7. Insert main health check run
        cursor.execute("""
            INSERT INTO health_check_runs (
                company_id,
                db_technology,
                target_host,
                target_port,
                target_db_name,
                findings,
                encryption_mode,
                report_adoc,
                run_by_user,
                run_from_host,
                tool_version,
                prompt_template_name,
                ai_execution_context,
                db_version,
                db_version_major,
                db_version_minor,
                cluster_name,
                node_count,
                infrastructure_metadata,
                health_score,
                submitted_via_api_key_id,
                submitted_from_ip,
                run_timestamp,
                run_date
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, NOW(), CURRENT_DATE
            )
            RETURNING id;
        """, (
            company_id,
            target_info.get('db_type', 'unknown'),
            target_info.get('host', 'unknown'),
            target_info.get('port', 0),
            target_info.get('database', 'unknown'),
            encrypted_findings,  # pgcrypto encrypted
            'pgcrypto',
            adoc_content,
            run_by_user,
            run_from_host,
            tool_version,
            prompt_template_name,
            ai_context_json,
            db_version,
            db_version_major,
            db_version_minor,
            cluster_name,
            node_count,
            json.dumps(infrastructure_metadata) if infrastructure_metadata else None,
            health_score,
            api_key_id,
            submitted_from_ip
        ))

        run_id = cursor.fetchone()[0]
        current_app.logger.info(f"Inserted health check run with ID: {run_id}")

        # 8. Insert triggered rules (CRITICAL for trend analysis)
        # Extract rules from critical_issues, high_priority_issues, medium_priority_issues
        if analysis_results:
            rules_stored = _insert_triggered_rules_from_analysis(
                cursor, run_id, analysis_results
            )
            if rules_stored > 0:
                current_app.logger.info(
                    f"Stored {rules_stored} triggered rules for run {run_id}"
                )

        conn.commit()
        return run_id

    except Exception as e:
        if conn:
            conn.rollback()
        current_app.logger.error(f"Database insertion failed: {e}", exc_info=True)
        raise

    finally:
        if conn:
            conn.close()


def _encrypt_findings_pgcrypto(cursor, findings_json):
    """
    Encrypt findings using PostgreSQL pgcrypto extension.

    Args:
        cursor: Database cursor
        findings_json (str): JSON string to encrypt

    Returns:
        bytes: Encrypted data suitable for insertion into bytea/text column
    """
    cursor.execute(
        "SELECT pgp_sym_encrypt(%s::text, get_encryption_key());",
        (findings_json,)
    )
    return cursor.fetchone()[0]


def _extract_db_version(structured_findings):
    """
    Extract database version from structured findings.

    Looks in common locations across different database technologies.

    Args:
        structured_findings (dict): The structured findings dictionary

    Returns:
        str: The database version string, or None if not found
    """
    version_locations = [
        ('db_metadata', 'version'),
        ('db_version', None),
        ('database_version', None),
        ('version', None),
        ('server_info', 'version'),
        ('cluster_info', 'version'),
        ('system_info', 'version'),
    ]

    for location in version_locations:
        if len(location) == 2 and location[1]:
            # Nested lookup
            parent = structured_findings.get(location[0], {})
            if isinstance(parent, dict):
                version = parent.get(location[1])
                if version:
                    return str(version)
        else:
            # Top-level lookup
            version = structured_findings.get(location[0])
            if version:
                return str(version)

    return None


def _parse_version_components(version_string):
    """
    Parse version string into major and minor components.

    Args:
        version_string (str): Version string like "16.3", "4.1.5", "25.1.2.15"

    Returns:
        tuple: (major, minor) version numbers, or (None, None) if parsing fails
    """
    if not version_string:
        return None, None

    # Extract numeric components using regex
    match = re.match(r'(\d+)\.(\d+)', str(version_string))
    if match:
        try:
            return int(match.group(1)), int(match.group(2))
        except ValueError:
            pass

    return None, None


def _extract_cluster_name(target_info, structured_findings):
    """
    Extract cluster name from target info or findings.

    Args:
        target_info (dict): Target system information
        structured_findings (dict): The structured findings dictionary

    Returns:
        str: The cluster name, or None if not found
    """
    # Check target_info first
    cluster_name = target_info.get('cluster_name')
    if cluster_name:
        return cluster_name

    # Check structured findings
    name_locations = [
        ('db_metadata', 'cluster_name'),
        ('cluster_name', None),
        ('cluster_info', 'name'),
        ('cluster_info', 'cluster_name'),
        ('system_info', 'cluster_name'),
    ]

    for location in name_locations:
        if len(location) == 2 and location[1]:
            parent = structured_findings.get(location[0], {})
            if isinstance(parent, dict):
                name = parent.get(location[1])
                if name:
                    return str(name)
        else:
            name = structured_findings.get(location[0])
            if name:
                return str(name)

    return None


def _extract_node_count(structured_findings):
    """
    Extract node count from structured findings.

    Args:
        structured_findings (dict): The structured findings dictionary

    Returns:
        int: The number of nodes, or None if not found
    """
    count_locations = [
        ('db_metadata', 'nodes'),
        ('node_count', None),
        ('nodes', None),
        ('cluster_info', 'node_count'),
        ('cluster_info', 'nodes'),
        ('topology', 'node_count'),
    ]

    for location in count_locations:
        if len(location) == 2 and location[1]:
            parent = structured_findings.get(location[0], {})
            if isinstance(parent, dict):
                count = parent.get(location[1])
                if count is not None:
                    try:
                        return int(count)
                    except (ValueError, TypeError):
                        pass
        else:
            count = structured_findings.get(location[0])
            if count is not None:
                try:
                    return int(count)
                except (ValueError, TypeError):
                    pass

    return None


def _extract_infrastructure_metadata(structured_findings):
    """
    Extract infrastructure metadata for the infrastructure_metadata JSONB column.

    This includes things like cloud provider, region, instance types, etc.

    Args:
        structured_findings (dict): The structured findings dictionary

    Returns:
        dict: Infrastructure metadata, or None if not found
    """
    metadata = {}

    # Check for common infrastructure fields
    infra_fields = [
        'cloud_provider',
        'cloud_region',
        'instance_type',
        'availability_zone',
        'environment',
        'deployment_type',
    ]

    for field in infra_fields:
        if field in structured_findings:
            metadata[field] = structured_findings[field]

    # Check nested locations
    if 'db_metadata' in structured_findings:
        db_meta = structured_findings['db_metadata']
        if isinstance(db_meta, dict):
            for field in infra_fields:
                if field in db_meta and field not in metadata:
                    metadata[field] = db_meta[field]

    if 'infrastructure' in structured_findings:
        infra = structured_findings['infrastructure']
        if isinstance(infra, dict):
            metadata.update(infra)

    return metadata if metadata else None


def _insert_triggered_rules_from_analysis(cursor, run_id, analysis_results):
    """
    Insert triggered rules from analysis_results into health_check_triggered_rules table.

    This function extracts the triggered rules from the analysis_results dictionary
    (output from generate_dynamic_prompt()) and stores them for trend analysis.

    Args:
        cursor: Database cursor
        run_id (int): The health check run ID
        analysis_results (dict): Analysis results containing:
            - critical_issues (list)
            - high_priority_issues (list)
            - medium_priority_issues (list)

    Returns:
        int: Total number of rules stored
    """
    if not analysis_results:
        return 0

    total_stored = 0
    insert_sql = """
        INSERT INTO health_check_triggered_rules (
            run_id,
            rule_config_name,
            metric_name,
            severity_level,
            severity_score,
            reasoning,
            recommendations,
            triggered_data
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    # Process critical issues
    for issue in analysis_results.get('critical_issues', []):
        try:
            cursor.execute(insert_sql, (
                run_id,
                issue.get('rule_config_name', 'unknown'),
                issue.get('metric', 'unknown'),
                'critical',
                issue.get('analysis', {}).get('score', 10),
                issue.get('analysis', {}).get('reasoning', ''),
                json.dumps(issue.get('analysis', {}).get('recommendations', [])),
                json.dumps(issue.get('data', {}))
            ))
            total_stored += 1
        except Exception as e:
            current_app.logger.warning(
                f"Failed to store critical issue for run {run_id}: {e}"
            )

    # Process high priority issues
    for issue in analysis_results.get('high_priority_issues', []):
        try:
            cursor.execute(insert_sql, (
                run_id,
                issue.get('rule_config_name', 'unknown'),
                issue.get('metric', 'unknown'),
                'high',
                issue.get('analysis', {}).get('score', 7),
                issue.get('analysis', {}).get('reasoning', ''),
                json.dumps(issue.get('analysis', {}).get('recommendations', [])),
                json.dumps(issue.get('data', {}))
            ))
            total_stored += 1
        except Exception as e:
            current_app.logger.warning(
                f"Failed to store high priority issue for run {run_id}: {e}"
            )

    # Process medium priority issues
    for issue in analysis_results.get('medium_priority_issues', []):
        try:
            cursor.execute(insert_sql, (
                run_id,
                issue.get('rule_config_name', 'unknown'),
                issue.get('metric', 'unknown'),
                'medium',
                issue.get('analysis', {}).get('score', 5),
                issue.get('analysis', {}).get('reasoning', ''),
                json.dumps(issue.get('analysis', {}).get('recommendations', [])),
                json.dumps(issue.get('data', {}))
            ))
            total_stored += 1
        except Exception as e:
            current_app.logger.warning(
                f"Failed to store medium priority issue for run {run_id}: {e}"
            )

    return total_stored


def _insert_triggered_rules(cursor, run_id, triggered_rules):
    """
    DEPRECATED: Use _insert_triggered_rules_from_analysis() instead.

    This function expected a pre-formatted 'triggered_rules' list but
    generate_dynamic_prompt() returns critical_issues/high_priority_issues/medium_priority_issues.

    Args:
        cursor: Database cursor
        run_id (int): The health check run ID
        triggered_rules (list): List of triggered rule dictionaries
    """
    if not triggered_rules:
        return

    current_app.logger.info(f"Inserting {len(triggered_rules)} triggered rules for run {run_id}")

    for rule in triggered_rules:
        try:
            cursor.execute("""
                INSERT INTO health_check_triggered_rules (
                    run_id,
                    rule_config_name,
                    metric_name,
                    severity_level,
                    severity_score,
                    reasoning,
                    recommendations,
                    triggered_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                run_id,
                rule.get('rule_config_name', 'unknown'),
                rule.get('metric_name', 'unknown'),
                rule.get('severity_level', 'medium'),
                rule.get('severity_score', 5),
                rule.get('reasoning'),
                json.dumps(rule.get('recommendations', [])),
                json.dumps(rule.get('triggered_data', {}))
            ))
        except Exception as e:
            current_app.logger.error(
                f"Failed to insert triggered rule: {rule.get('rule_config_name')}: {e}"
            )
            # Continue with other rules even if one fails
            continue

    current_app.logger.info(f"Successfully inserted triggered rules for run {run_id}")
