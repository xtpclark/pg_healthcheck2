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
