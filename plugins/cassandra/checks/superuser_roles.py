from plugins.cassandra.utils.qrylib.qry_superuser_roles import get_superuser_roles_query
from plugins.common.check_helpers import format_check_header, format_recommendations, safe_execute_query
import logging

logger = logging.getLogger(__name__)

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High - security concern

def run_superuser_roles_check(connector, settings):
    """
    Analyzes superuser roles in Cassandra authentication.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Superuser Roles Analysis",
        "Querying system_auth.roles to identify all superuser accounts for security review."
    )
    structured_data = {}
    
    # Version check for Cassandra 3.x or higher
    if not hasattr(connector, 'version_info'):
        adoc_content.append("[WARNING]\n====\nVersion information not available. Authentication checks may not apply.\n====\n")
        structured_data["superuser_roles"] = {"status": "skipped", "reason": "No version info"}
        return "\n".join(adoc_content), structured_data
    
    major_version = connector.version_info.get('major_version', 0)
    if major_version < 3:
        adoc_content.append("[NOTE]\n====\nCassandra version {major_version}.x detected. Internal authentication (system_auth.roles) is not available in versions below 3.0. Enable PasswordAuthenticator for role-based auth in 3.x+.\n====\n".format(major_version=major_version))
        structured_data["superuser_roles"] = {"status": "skipped", "reason": "Version < 3.0", "version": major_version}
        return "\n".join(adoc_content), structured_data
    
    # Proceed with query for 3.x+
    query = get_superuser_roles_query(connector)
    success, formatted, raw = safe_execute_query(connector, query, "Superuser roles query")
    
    if not success:
        adoc_content.append(formatted)
        structured_data["superuser_roles"] = {"status": "error", "data": raw}
        return "\n".join(adoc_content), structured_data
    
    # Debug raw data structure
    logger.debug(f"Raw query result: {raw}")
    for role in raw:
        logger.debug(f"Row: {role}, Type: {type(role)}")
    
    # Normalize and validate superuser roles
    superusers = []
    for role in raw:
        if not isinstance(role, dict):
            logger.error(f"Unexpected row format: {role}")
            continue
        # Try multiple key variations for 'role'
        role_key = next((k for k in ['role', 'Role', 'ROLE', 'role_name'] if k in role), None)
        if role_key is None:
            logger.error(f"No 'role' key found in row: {role}")
            continue
        if role.get('is_superuser', False):
            superusers.append({'role': role[role_key], 'is_superuser': role.get('is_superuser', False)})
    
    if not superusers:
        adoc_content.append("[NOTE]\n====\nNo superuser roles found. This may indicate authentication is disabled or no custom superusers exist.\n====\n")
        recommendations = [
            "Ensure at least one superuser role exists for administrative tasks.",
            "Use 'CREATE ROLE <role> WITH SUPERUSER = true' to create a superuser if needed.",
            "Verify role permissions with 'LIST ROLES;' to confirm access."
        ]
        adoc_content.extend(format_recommendations(recommendations))
        structured_data["superuser_roles"] = {"status": "success", "data": [], "superuser_count": 0, "version": major_version}
        return "\n".join(adoc_content), structured_data
    
    # Report all superusers
    adoc_content.append(formatted)
    adoc_content.append("\n==== Superuser Roles Found")
    adoc_content.append("|===\n|Role Name")
    for su in superusers:
        adoc_content.append(f"|{su['role']}")
    adoc_content.append("|===\n")
    
    # Always recommend review
    recommendations = [
        "Review all superuser roles for least privilege principle: revoke superuser from accounts that don't need full admin access",
        "Use GRANT/REVOKE to assign specific permissions instead of superuser status",
        "Enable audit logging if available (cassandra.yaml: enabled: true) to track superuser actions",
        "Regularly audit role memberships: LIST ROLES; LIST PERMISSIONS ON ALL BY <role>",
        "Consider rotating passwords for superuser accounts and using strong, unique credentials"
    ]
    adoc_content.extend(format_recommendations(recommendations))
    
    status = "warning" if len(superusers) > 1 else "success"
    structured_data["superuser_roles"] = {
        "status": status,
        "data": [su['role'] for su in superusers],
        "superuser_count": len(superusers),
        "version": major_version
    }
    
    return "\n".join(adoc_content), structured_data
