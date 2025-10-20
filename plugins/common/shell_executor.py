"""
Shell command execution with support for multiple operation types.

This module provides a unified interface for executing:
- Standard shell commands (df, ps, free, etc.)
- Database-specific tools (nodetool, redis-cli, etc.)
- Custom command parsers and formatters
"""

import json
import logging
from typing import Dict, Any, Tuple, Optional, List

from .ssh_handler import SSHConnectionManager
from .output_formatters import AsciiDocFormatter

logger = logging.getLogger(__name__)


class ShellExecutor:
    """
    Executes shell commands and database-specific tools via SSH.
    
    Supports:
    - {"operation": "shell", "command": "df -h"}
    - {"operation": "nodetool", "command": "status"}
    - {"operation": "redis-cli", "command": "INFO"}
    - Custom operations via extensible parsers
    
    Example:
        executor = ShellExecutor(ssh_manager)
        result = executor.execute('{"operation": "shell", "command": "df -h"}')
    """
    
    # Commands that are always safe to execute
    SAFE_COMMANDS = {
        'df', 'free', 'ps', 'uptime', 'w', 'top', 'vmstat', 'iostat',
        'netstat', 'ss', 'lsof', 'dmesg', 'journalctl', 'systemctl',
        'nodetool', 'cqlsh', 'redis-cli', 'mongo', 'mongosh',
        'du', 'ls', 'find', 'grep', 'awk', 'sed', 'cat', 'tail', 'head',
        'wc', 'sort', 'uniq', 'hostname', 'uname', 'whoami', 'id'
    }
    
    # Commands that legitimately may return no output
    EMPTY_OK_COMMANDS = {
        'find', 'grep', 'locate', 'ls', 'awk', 'sed', 'lsof'
    }
    
    def __init__(self, ssh_manager: SSHConnectionManager, 
                 formatter: Optional[AsciiDocFormatter] = None,
                 allow_unsafe_commands: bool = False):
        """
        Initialize shell executor.
        
        Args:
            ssh_manager: Configured SSHConnectionManager instance
            formatter: Optional custom formatter (defaults to AsciiDocFormatter)
            allow_unsafe_commands: If True, disable command sanitization (DANGEROUS!)
        """
        self.ssh = ssh_manager
        self.formatter = formatter or AsciiDocFormatter()
        self.allow_unsafe = allow_unsafe_commands
        
        if allow_unsafe_commands:
            logger.warning(
                "⚠️  Command sanitization DISABLED! "
                "All commands will be executed without validation. "
                "Only use in trusted environments."
            )
        
        self.operation_handlers = {
            'shell': self._execute_shell,
            'nodetool': self._execute_nodetool,
            'redis-cli': self._execute_redis_cli,
        }
    
    def register_operation(self, operation_name: str, handler_func):
        """
        Register a custom operation handler.
        
        Args:
            operation_name: Name of the operation (e.g., 'mysql-admin')
            handler_func: Function(command: str) -> Tuple[str, Any]
        """
        self.operation_handlers[operation_name] = handler_func
        logger.info(f"Registered custom operation handler: {operation_name}")
    
    def _sanitize_command(self, command: str) -> str:
        """
        Basic command sanitization to prevent injection.
        
        Args:
            command: Command to sanitize
        
        Returns:
            Sanitized command
        
        Raises:
            ValueError: If command contains dangerous patterns
        """
        if self.allow_unsafe:
            return command
        
        if not command or not command.strip():
            raise ValueError("Command cannot be empty")
        
        # Extract command name (first word)
        cmd_parts = command.strip().split()
        cmd_name = cmd_parts[0] if cmd_parts else ''
        
        # Check if command is in safe list
        if cmd_name in self.SAFE_COMMANDS:
            return command
        
        # Check for absolute paths to safe commands
        if '/' in cmd_name:
            # Extract basename from path
            basename = cmd_name.split('/')[-1]
            if basename in self.SAFE_COMMANDS:
                return command
        
        # Block dangerous shell metacharacters
        dangerous_patterns = [';', '&&', '||', '`', '$', '$(', '${']
        
        for pattern in dangerous_patterns:
            if pattern in command:
                logger.warning(f"Potentially dangerous command rejected: {command[:50]}...")
                raise ValueError(
                    f"Command contains potentially dangerous pattern '{pattern}'. "
                    f"If this is a legitimate command, add '{cmd_name}' to SAFE_COMMANDS "
                    f"or initialize ShellExecutor with allow_unsafe_commands=True."
                )
        
        # If we get here, command is not in safe list but has no obvious injection patterns
        # Log it but allow it
        logger.info(f"Allowing non-whitelisted but safe-looking command: {cmd_name}")
        return command
    
    def execute(self, query: str, return_raw: bool = False) -> Tuple[str, Any]:
        """
        Executes a command based on JSON query format.
        
        Args:
            query: JSON string with operation and command
            return_raw: If True, returns (formatted, raw_data)
        
        Returns:
            Formatted output string or (formatted, raw_data) tuple
        
        Raises:
            ValueError: If query format is invalid
            RuntimeError: If command execution fails
        """
        try:
            # Parse JSON query
            query_obj = json.loads(query)
            operation = query_obj.get('operation')
            command = query_obj.get('command')
            
            if not operation:
                raise ValueError("Query must include 'operation' field")
            
            if not command:
                raise ValueError("Query must include 'command' field")
            
            # Get appropriate handler
            handler = self.operation_handlers.get(operation)
            if not handler:
                raise ValueError(
                    f"Unknown operation: {operation}. "
                    f"Available: {', '.join(self.operation_handlers.keys())}"
                )
            
            # Execute command
            self.ssh.ensure_connected()
            formatted, raw_data = handler(command)
            
            return (formatted, raw_data) if return_raw else formatted
            
        except json.JSONDecodeError as e:
            error_msg = f"[ERROR]\n====\nInvalid JSON query: {str(e)}\n====\n"
            logger.error(f"JSON decode error: {e}")
            error_data = {'error': str(e), 'query': query[:200]}
            return (error_msg, error_data) if return_raw else error_msg
        
        except Exception as e:
            error_msg = f"[ERROR]\n====\nCommand execution failed: {str(e)}\n====\n"
            logger.error(f"Command execution error: {e}", exc_info=True)
            error_data = {'error': str(e)}
            return (error_msg, error_data) if return_raw else error_msg

    def _execute_shell(self, command: str) -> Tuple[str, Dict]:
        """
        Executes a standard shell command.
        
        Args:
            command: Shell command to execute
        
        Returns:
            Tuple of (formatted_output, raw_data)
        """
        # Sanitize command
        command = self._sanitize_command(command)
        
        stdout, stderr, exit_code = self.ssh.execute_command(command)
        
        if exit_code != 0 and not stdout:
            raise RuntimeError(f"Command failed with exit code {exit_code}: {stderr}")
        
        # Safely extract command name
        cmd_parts = command.strip().split() if command else []
        cmd_name = cmd_parts[0].lower() if cmd_parts else ''
        
        # Handle paths in command name
        if '/' in cmd_name:
            cmd_name = cmd_name.split('/')[-1]
        
        # Commands that legitimately may return no results
        is_empty_ok = cmd_name in self.EMPTY_OK_COMMANDS
        
        if not stdout or not stdout.strip():
            if is_empty_ok:
                # Empty result is normal for these commands (e.g., no files found)
                note_msg = (
                    "[NOTE]\n====\n"
                    "No results found (this may be normal - e.g., no matching files/processes).\n"
                    "====\n"
                )
            else:
                logger.warning(f"Empty output from command: {command[:50]}...")
                note_msg = "[NOTE]\n====\nNo output from command.\n====\n"
            
            raw_data = {
                'command': command,
                'output': '',
                'stderr': stderr if stderr else None,
                'exit_code': exit_code
            }
            
            return note_msg, raw_data
        
        # Format output
        formatted = self.formatter.format_shell_output(command, stdout)
        
        raw_data = {
            'command': command,
            'output': stdout,
            'stderr': stderr if stderr else None,
            'exit_code': exit_code
        }
        
        return formatted, raw_data
    
    def _execute_nodetool(self, command: str) -> Tuple[str, Any]:
        """
        Executes a Cassandra nodetool command.
        
        Args:
            command: Nodetool command (e.g., 'status', 'tpstats')
        
        Returns:
            Tuple of (formatted_output, parsed_data)
        """
        full_command = f"nodetool {command}"
        stdout, stderr, exit_code = self.ssh.execute_command(full_command)
        
        if exit_code != 0:
            raise RuntimeError(f"Nodetool command failed: {stderr}")
        
        if not stdout or not stdout.strip():
            return "[NOTE]\n====\nNo output from nodetool.\n====\n", {}
        
        # Parse nodetool output (delegate to parser)
        from .parsers import NodetoolParser
        parser = NodetoolParser()
        parsed_data = parser.parse(command, stdout)
        
        # Format output
        if isinstance(parsed_data, list):
            formatted = self.formatter.format_table(parsed_data)
        elif isinstance(parsed_data, dict):
            formatted = self.formatter.format_literal(str(parsed_data))
        else:
            formatted = str(parsed_data)
        
        return formatted, parsed_data
    
    def _execute_redis_cli(self, command: str) -> Tuple[str, Any]:
        """
        Executes a redis-cli command.
        
        Args:
            command: Redis command (e.g., 'INFO', 'DBSIZE')
        
        Returns:
            Tuple of (formatted_output, parsed_data)
        """
        full_command = f"redis-cli {command}"
        stdout, stderr, exit_code = self.ssh.execute_command(full_command)
        
        if exit_code != 0:
            raise RuntimeError(f"Redis CLI command failed: {stderr}")
        
        # Parse redis-cli output
        raw_data = {
            'command': command,
            'output': stdout
        }
        
        formatted = self.formatter.format_literal(stdout)
        
        return formatted, raw_data
