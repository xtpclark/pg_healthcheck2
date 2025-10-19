"""Role permission audit queries for Cassandra."""

__all__ = [
    'get_non_superuser_roles_query',
    'get_role_permissions_query'
]

def get_non_superuser_roles_query(connector):
    """
    Returns query for non-superuser roles.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT role AS role_name
    FROM system_auth.roles
    WHERE is_superuser = false
    ALLOW FILTERING;
    """

def get_role_permissions_query(connector):
    """
    Returns query for all role permissions.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT role AS role_name, resource, permission
    FROM system_auth.role_permissions;
    """