# trends_app/models.py
from flask_login import UserMixin

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
            # Normalize technology names
            tech_map = {
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
