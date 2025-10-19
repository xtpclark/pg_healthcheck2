"""
Output formatting utilities for health check results.

Provides consistent formatting across all database plugins:
- AsciiDoc tables
- Literal blocks
- Shell command output detection and formatting
"""

import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class AsciiDocFormatter:
    """
    Formats data into AsciiDoc markup for reports.
    
    Supports:
    - Tables from list of dicts
    - Shell command output (detects tabular vs literal)
    - Literal blocks for raw text
    """
    
    # Commands that typically produce tabular output
    TABULAR_COMMANDS = [
        'df', 'ps', 'free', 'top', 'netstat', 'ss', 'lsof',
        'iostat', 'vmstat', 'mpstat', 'sar'
    ]
    
    def format_table(self, data: List[Dict[str, Any]]) -> str:
        """
        Formats a list of dictionaries as an AsciiDoc table.
        
        Args:
            data: List of dicts where keys are column names
        
        Returns:
            AsciiDoc table string
        """
        if not data:
            return "[NOTE]\n====\nNo data to display.\n====\n"
        
        # Get columns from first row
        columns = list(data[0].keys())
        
        # Build table
        table_lines = ['|===']
        table_lines.append('|' + '|'.join(columns))
        
        for row in data:
            row_values = [str(row.get(col, '')) for col in columns]
            # Escape pipe characters
            row_values = [v.replace('|', '\\|') for v in row_values]
            table_lines.append('|' + '|'.join(row_values))
        
        table_lines.append('|===')
        
        return '\n'.join(table_lines)
    
    def format_shell_output(self, command: str, output: str) -> str:
        """
        Formats shell command output intelligently.
        
        Detects if output is tabular (df, ps, etc.) and formats accordingly.
        Otherwise returns as literal block.
        
        Args:
            command: The shell command that was executed
            output: Raw output from the command
        
        Returns:
            Formatted AsciiDoc string
        """
        # Check if this is a tabular command
        is_tabular = any(cmd in command.lower() for cmd in self.TABULAR_COMMANDS)
        
        if is_tabular and '\n' in output:
            lines = output.strip().split('\n')
            
            # Try to detect header line
            if lines and len(lines) > 1:
                header_line = lines[0]
                header_parts = header_line.split()
                
                # If header has multiple columns, treat as table
                if len(header_parts) >= 2:
                    return self._parse_tabular_output(lines, header_parts)
        
        # Fallback: literal block
        return self.format_literal(output)
    
    def _parse_tabular_output(self, lines: List[str], header: List[str]) -> str:
        """
        Parses tabular shell output into AsciiDoc table.
        
        Args:
            lines: All output lines (including header)
            header: Parsed header columns
        
        Returns:
            AsciiDoc table
        """
        table_lines = ['|===']
        table_lines.append('|' + '|'.join(header))
        
        # Parse data lines
        for line in lines[1:]:
            if not line.strip():
                continue
            
            # Split by whitespace, but limit to header column count
            parts = line.split(None, len(header) - 1)
            
            # Pad with empty strings if needed
            while len(parts) < len(header):
                parts.append('')
            
            # Take only as many columns as header
            parts = parts[:len(header)]
            
            table_lines.append('|' + '|'.join(parts))
        
        table_lines.append('|===')
        
        return '\n'.join(table_lines)
    
    def format_literal(self, text: str) -> str:
        """
        Formats text as a literal block.
        
        Args:
            text: Raw text to format
        
        Returns:
            AsciiDoc literal block
        """
        return f"[source,text]\n----\n{text}\n----\n"
    
    def format_note(self, message: str) -> str:
        """
        Formats a note/info message.
        
        Args:
            message: Note text
        
        Returns:
            AsciiDoc note block
        """
        return f"[NOTE]\n====\n{message}\n====\n"
    
    def format_error(self, error: str) -> str:
        """
        Formats an error message.
        
        Args:
            error: Error text
        
        Returns:
            AsciiDoc error block
        """
        return f"[ERROR]\n====\n{error}\n====\n"
