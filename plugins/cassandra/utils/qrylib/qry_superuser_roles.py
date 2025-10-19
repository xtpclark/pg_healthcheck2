"""Superuser roles queries for Cassandra."""

__all__ = [
    'get_superuser_roles_query'
]

def get_superuser_roles_query(connector):
    """
    Returns CQL query for superuser roles from system_auth.roles.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    # Version check handled in check module; query is the same for 3.x+
    return """
    SELECT role, is_superuser
    FROM system_auth.roles;
    """