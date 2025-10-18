"""
SSH connection management for database health checks.

This module provides a reusable SSH connection manager that can be used
by any database connector that needs to execute remote commands.
"""

import logging
from typing import Optional, Dict, Tuple

try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False

logger = logging.getLogger(__name__)


class SSHConnectionManager:
    """
    Manages SSH connections for remote command execution.
    
    Features:
    - Connection pooling (reuses connections)
    - Automatic reconnection on failure
    - Support for key-based and password authentication
    - Connection timeout handling
    
    Example:
        ssh_manager = SSHConnectionManager(settings)
        ssh_manager.connect()
        output, stderr, exit_code = ssh_manager.execute_command("df -h")
        ssh_manager.disconnect()
    """
    
    def __init__(self, settings: Dict):
        """
        Initialize SSH connection manager.
        
        Args:
            settings: Dictionary with SSH configuration:
                - ssh_host: Hostname or IP address
                - ssh_user: SSH username
                - ssh_key_file: Path to private key (optional)
                - ssh_password: SSH password (optional)
                - ssh_timeout: Connection timeout in seconds (default: 10)
                - ssh_port: SSH port (default: 22)
        """
        if not PARAMIKO_AVAILABLE:
            raise ImportError(
                "Paramiko library is required for SSH support. "
                "Install it with: pip install paramiko"
            )
        
        self.settings = settings
        self.client: Optional[paramiko.SSHClient] = None
        self._validate_settings()
    
    def _validate_settings(self):
        """Validates that required SSH settings are present."""
        required = ['ssh_host', 'ssh_user']
        missing = [key for key in required if not self.settings.get(key)]
        
        if missing:
            raise ValueError(
                f"Missing required SSH settings: {', '.join(missing)}. "
                f"Required: ssh_host, ssh_user. "
                f"Optional: ssh_key_file or ssh_password"
            )
        
        # Require at least one authentication method
        if not self.settings.get('ssh_key_file') and not self.settings.get('ssh_password'):
            raise ValueError(
                "Either ssh_key_file or ssh_password must be configured for SSH authentication"
            )
    
    def connect(self) -> None:
        """
        Establishes SSH connection to the remote host.
        
        Raises:
            ConnectionError: If connection fails
        """
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            connect_args = {
                'hostname': self.settings['ssh_host'],
                'username': self.settings['ssh_user'],
                'port': self.settings.get('ssh_port', 22),
                'timeout': self.settings.get('ssh_timeout', 10),
            }
            
            # Add authentication
            if self.settings.get('ssh_key_file'):
                connect_args['key_filename'] = self.settings['ssh_key_file']
            elif self.settings.get('ssh_password'):
                connect_args['password'] = self.settings['ssh_password']
            
            self.client.connect(**connect_args)
            logger.info(f"SSH connection established to {self.settings['ssh_host']}")
            
        except Exception as e:
            logger.error(f"SSH connection failed: {e}")
            raise ConnectionError(f"Could not establish SSH connection: {e}")
    
    def disconnect(self) -> None:
        """Closes the SSH connection."""
        if self.client:
            try:
                self.client.close()
                logger.info("SSH connection closed")
            except Exception as e:
                logger.warning(f"Error closing SSH connection: {e}")
            finally:
                self.client = None
    
    def is_connected(self) -> bool:
        """
        Checks if SSH connection is active.
        
        Returns:
            bool: True if connection is active, False otherwise
        """
        if not self.client:
            return False
        
        transport = self.client.get_transport()
        return transport is not None and transport.is_active()
    
    def ensure_connected(self) -> None:
        """Ensures connection is active, reconnects if necessary."""
        if not self.is_connected():
            logger.info("SSH connection lost, reconnecting...")
            self.connect()
    
    def execute_command(self, command: str, timeout: Optional[int] = None) -> Tuple[str, str, int]:
        """
        Executes a command on the remote host.
        
        Args:
            command: Shell command to execute
            timeout: Optional command timeout in seconds
        
        Returns:
            Tuple of (stdout, stderr, exit_code)
        
        Raises:
            ConnectionError: If not connected
            RuntimeError: If command execution fails
        """
        if not self.is_connected():
            raise ConnectionError("SSH connection not established. Call connect() first.")
        
        try:
            # Execute command
            stdin, stdout, stderr = self.client.exec_command(
                command,
                timeout=timeout or self.settings.get('ssh_timeout', 10)
            )
            
            # Read output
            exit_code = stdout.channel.recv_exit_status()
            stdout_text = stdout.read().decode('utf-8')
            stderr_text = stderr.read().decode('utf-8')
            
            logger.debug(f"Command executed: {command}")
            logger.debug(f"Exit code: {exit_code}")
            
            return stdout_text, stderr_text, exit_code
            
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            raise RuntimeError(f"Failed to execute command '{command}': {e}")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
