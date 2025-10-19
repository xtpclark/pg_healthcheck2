"""
Common utilities shared across all database plugins.

This module provides reusable components for:
- SSH connection management
- Shell command execution
- Output formatting (AsciiDoc tables, etc.)
- Common parsers (nodetool, df, ps, etc.)
- Check helpers and utilities
"""

from .ssh_handler import SSHConnectionManager
from .shell_executor import ShellExecutor
from .output_formatters import AsciiDocFormatter
from .parsers import NodetoolParser, ShellCommandParser
from .ssh_mixin import SSHSupportMixin
from .check_helpers import require_ssh, format_check_header, format_recommendations, safe_execute_query

__all__ = [
    'SSHConnectionManager',
    'ShellExecutor',
    'AsciiDocFormatter',
    'NodetoolParser',
    'ShellCommandParser',
    'SSHSupportMixin',
    'require_ssh',
    'format_check_header',
    'format_recommendations',
    'safe_execute_query',
]
