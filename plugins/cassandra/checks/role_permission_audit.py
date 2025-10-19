from plugins.cassandra.utils.qrylib.qry_role_permission_audit import get_non_superuser_roles_query, get_role_permissions_query
from plugins.common.check_helpers import format_check_header, safe_execute_query, format_recommendations

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8  # High - security audit

def run_role_permission_audit(connector, settings):
    """
    Performs the role permission audit analysis.
    
    Args:
        connector: Database connector with execute_query() method
        settings: Dictionary of configuration settings (main config, not connector settings)
    
    Returns:
        tuple: (asciidoc_report_string, structured_data_dict)
    """
    adoc_content = format_check_header(
        "Role Permission Audit",
        "Querying system_auth.role_permissions to list permissions for non-superuser roles."
    )
    structured_data = {}
    
    # Get non-superuser roles
    query_roles = get_non_superuser_roles_query(connector)
    success_roles, formatted_roles, raw_roles = safe_execute_query(
        connector, query_roles, "Non-superuser roles query"
    )
    
    if not success_roles:
        adoc_content.append(formatted_roles)
        structured_data["roles"] = {"status": "error", "data": raw_roles}
        return "\n".join(adoc_content), structured_data
    
    non_super_roles = [row.get('role_name') for row in raw_roles]
    
    if not non_super_roles:
        adoc_content.append("[NOTE]\n====\nNo non-superuser roles found. All roles have superuser privileges.\n====\n")
        structured_data["permissions"] = {"status": "info", "data": [], "count": 0}
        return "\n".join(adoc_content), structured_data
    
    # Get all role permissions
    query_perms = get_role_permissions_query(connector)
    success_perms, formatted_perms, raw_perms = safe_execute_query(
        connector, query_perms, "Role permissions query"
    )
    
    if not success_perms:
        adoc_content.append(formatted_perms)
        structured_data["permissions"] = {"status": "error", "data": raw_perms}
        return "\n".join(adoc_content), structured_data
    
    # Filter permissions for non-superuser roles
    non_super_perms = [p for p in raw_perms if p.get('role_name') in non_super_roles]
    
    adoc_content.append(formatted_roles)
    adoc_content.append(formatted_perms)
    
    if not non_super_perms:
        adoc_content.append("[NOTE]\n====\nNon-superuser roles have no explicit permissions granted.\n====\n")
        status = "success"
    else:
        adoc_content.append(
            f"[WARNING]\n====\n"
            f"{len(non_super_perms)} permission(s) granted to {len(non_super_roles)} non-superuser role(s). "
            "Review for least privilege principle.\n====\n"
        )
        
        # List permissions in a table
        adoc_content.append("\n==== Non-Superuser Permissions")
        adoc_content.append("|===\n|Role|Resource|Permission")
        for perm in non_super_perms:
            adoc_content.append(
                f"|{perm.get('role_name')}|{perm.get('resource')}|{perm.get('permission')}"
            )
        adoc_content.append("|===\n")
        
        recommendations = [
            "Review each non-superuser role's permissions against application requirements.",
            "Revoke unnecessary permissions using: REVOKE permission resource FROM role_name",
            "Use GRANT to add only required permissions following least privilege.",
            "Consider creating role hierarchies if complex access patterns are needed.",
            "Periodically audit role permissions as part of security maintenance."
        ]
        adoc_content.extend(format_recommendations(recommendations))
        
        status = "warning"
    
    structured_data["roles"] = {
        "status": "success",
        "non_super_count": len(non_super_roles),
        "names": non_super_roles
    }
    structured_data["permissions"] = {
        "status": status,
        "data": non_super_perms,
        "count": len(non_super_perms)
    }
    
    return "\n".join(adoc_content), structured_data