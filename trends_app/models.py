# trends_app/models.py
from flask_login import UserMixin
import psycopg2
import psycopg2.extras

# Module-level cache for technology name mapping
_TECH_MAP_CACHE = None

def clear_tech_map_cache():
    """
    Clear the cached technology mapping.
    Call this after adding/updating technologies in admin UI.
    """
    global _TECH_MAP_CACHE
    _TECH_MAP_CACHE = None


def _load_tech_map():
    """
    Load technology code-to-description mapping from database.
    Cached at module level to avoid repeated database queries.
    Returns dict mapping lowercase codes to display names for privilege names.
    """
    global _TECH_MAP_CACHE

    if _TECH_MAP_CACHE is not None:
        return _TECH_MAP_CACHE

    try:
        from .utils import load_trends_config
        config = load_trends_config()
        db_config = config.get('database')

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Use stored procedure to fetch all technologies
        cursor.execute("SELECT * FROM fetchtechtypes(FALSE)")
        technologies = cursor.fetchall()
        conn.close()

        # Build mapping: code (lowercase) -> descrip (for privilege name)
        # Also handle common aliases
        tech_map = {}
        for tech in technologies:
            code = tech['code'].lower()
            descrip = tech['descrip']

            # Primary mapping
            tech_map[code] = descrip

            # Common aliases
            if code == 'postgres':
                tech_map['postgresql'] = descrip
            elif code == 'valkey':
                tech_map['redis'] = 'Redis'  # Valkey privilege might be called Redis

        _TECH_MAP_CACHE = tech_map
        return tech_map

    except Exception as e:
        # Fallback to hardcoded map if database unavailable
        # (e.g., during initialization before database is set up)
        return {
            'postgres': 'PostgreSQL',
            'postgresql': 'PostgreSQL',
            'kafka': 'Kafka',
            'cassandra': 'Cassandra',
            'opensearch': 'OpenSearch',
            'clickhouse': 'ClickHouse',
            'mongodb': 'MongoDB',
            'mysql': 'MySQL',
            'valkey': 'Valkey',
            'redis': 'Redis'
        }


class User(UserMixin):
    """User model for authentication and privilege management."""
    def __init__(self, user_id, username, is_admin, password_change_required, accessible_companies=None, privileges=None):
        self.id = user_id
        self.username = username
        self.is_admin = is_admin
        self.password_change_required = password_change_required
        self.accessible_companies = accessible_companies or []
        self.privileges = privileges or set()

    def has_privilege(self, privilege_name):
        """Checks if a user has a specific privilege."""
        return self.is_admin or privilege_name in self.privileges

    def can_edit_report(self, db_technology=None):
        """
        Check if user can edit reports for a specific technology.

        Args:
            db_technology: Technology name (postgres, kafka, cassandra, etc.) or None for generic

        Returns:
            bool: True if user has edit permission
        """
        # Check universal edit privileges
        if self.has_privilege('EditReports') or self.has_privilege('EditAllTechnologyReports'):
            return True

        # Check technology-specific privilege
        if db_technology:
            # Load technology name mapping from database (cached)
            tech_map = _load_tech_map()
            tech_name = tech_map.get(db_technology.lower(), db_technology.capitalize())
            tech_priv = f'Edit{tech_name}Reports'
            if self.has_privilege(tech_priv):
                return True

        return False

    def can_download_report(self):
        """Check if user can download/export reports."""
        return self.has_privilege('DownloadReports')

    def can_delete_report(self):
        """Check if user can delete reports."""
        return self.has_privilege('DeleteReports')

    def can_share_report(self):
        """Check if user can share reports with other users."""
        return self.has_privilege('ShareReports')
