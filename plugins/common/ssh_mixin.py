"""
SSH support mixin for database connectors.

Provides standard methods for SSH-enabled connectors to check
SSH availability and provide consistent error messages.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class SSHSupportMixin:
    """
    Mixin class that provides SSH support detection methods.
    
    Any connector that uses SSHConnectionManager should inherit this mixin
    to provide consistent SSH capability checking.
    
    Example:
        class CassandraConnector(SSHSupportMixin):
            def __init__(self, settings):
                self.ssh_manager = SSHConnectionManager(settings) if settings.get('ssh_host') else None
    """
    
    def has_ssh_support(self) -> bool:
        """
        Check if SSH operations are supported and available.
        
        Returns:
            bool: True if SSH is configured and connected, False otherwise
        """
        if not hasattr(self, 'ssh_manager'):
            return False
        
        return self.ssh_manager is not None and self.ssh_manager.is_connected()
    
    def get_ssh_skip_message(self, operation_name: Optional[str] = None) -> tuple:
        """
        Generate a standard skip message for checks that require SSH.
        
        Args:
            operation_name: Optional name of the operation (e.g., 'nodetool', 'shell commands')
        
        Returns:
            tuple: (adoc_message, structured_data_dict) suitable for returning from a check
        """
        op_text = f" for {operation_name}" if operation_name else ""
        
        adoc_message = (
            "[IMPORTANT]\n"
            "====\n"
            f"This check requires SSH access{op_text}.\n\n"
            "Configure the following in your settings:\n\n"
            "* `ssh_host`: Hostname or IP address of the database server\n"
            "* `ssh_user`: SSH username\n"
            "* `ssh_key_file` OR `ssh_password`: Authentication method\n\n"
            "Optional:\n\n"
            "* `ssh_port`: SSH port (default: 22)\n"
            "* `ssh_timeout`: Connection timeout in seconds (default: 10)\n"
            "====\n"
        )
        
        structured_data = {
            "status": "skipped",
            "reason": "SSH not configured",
            "required_settings": ["ssh_host", "ssh_user", "ssh_key_file or ssh_password"]
        }
        
        return adoc_message, structured_data
    
    def get_ssh_settings_info(self) -> dict:
        """
        Get information about SSH configuration (safe for logging/debugging).
        
        Returns:
            dict: SSH configuration info with sensitive data masked
        """
        if not hasattr(self, 'settings'):
            return {'configured': False}
        
        settings = self.settings
        
        return {
            'configured': bool(settings.get('ssh_host')),
            'host': settings.get('ssh_host', 'Not configured'),
            'user': settings.get('ssh_user', 'Not configured'),
            'port': settings.get('ssh_port', 22),
            'auth_method': 'key' if settings.get('ssh_key_file') else ('password' if settings.get('ssh_password') else 'none'),
            'timeout': settings.get('ssh_timeout', 10),
            'connected': self.has_ssh_support()
        }
