"""
SSH requirement detection for health checks.

This module analyzes check files to determine if they require SSH access
(e.g., for nodetool, JMX, or other administrative commands).
"""

import ast
import json
import re
from pathlib import Path
from typing import Dict, List, Optional


def check_requires_ssh(check_module_path: str, query_file_path: Optional[str] = None) -> Dict:
    """
    Analyzes check files to determine if SSH is required.
    
    Args:
        check_module_path: Path to the check module .py file
        query_file_path: Optional path to the query library file
    
    Returns:
        dict: {
            'requires_ssh': bool,
            'reason': str,
            'commands': list,  # SSH commands detected (e.g., ['nodetool status'])
            'detection_method': str  # How it was detected
        }
    """
    result = {
        'requires_ssh': False,
        'reason': '',
        'commands': [],
        'detection_method': ''
    }
    
    # Check 1: Analyze imports in check module
    import_detection = _check_imports_for_ssh(check_module_path)
    if import_detection['requires_ssh']:
        result.update(import_detection)
        return result
    
    # Check 2: Analyze query file if provided
    if query_file_path:
        query_detection = _check_query_file_for_ssh(query_file_path)
        if query_detection['requires_ssh']:
            result.update(query_detection)
            return result
    
    return result


def _check_imports_for_ssh(check_module_path: str) -> Dict:
    """
    Check if module imports SSH-requiring modules.
    
    Looks for:
    - from ...nodetool_queries import ...
    - from ...jmx_queries import ...
    - Any imports with 'nodetool', 'jmx', 'ssh' in the name
    """
    result = {
        'requires_ssh': False,
        'reason': '',
        'commands': [],
        'detection_method': ''
    }
    
    try:
        with open(check_module_path, 'r', encoding='utf-8') as f:
            content = f.read()
            tree = ast.parse(content)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ''
                
                # Check for nodetool imports
                if 'nodetool' in module.lower():
                    result['requires_ssh'] = True
                    result['reason'] = 'Imports from nodetool query module'
                    result['detection_method'] = f'import from {module}'
                    
                    # Try to determine which commands
                    for alias in node.names:
                        if 'status' in alias.name.lower():
                            result['commands'].append('nodetool status')
                        elif 'compaction' in alias.name.lower():
                            result['commands'].append('nodetool compactionstats')
                        elif 'tpstats' in alias.name.lower():
                            result['commands'].append('nodetool tpstats')
                    
                    return result
                
                # Check for JMX imports
                if 'jmx' in module.lower():
                    result['requires_ssh'] = True
                    result['reason'] = 'Imports from JMX query module'
                    result['detection_method'] = f'import from {module}'
                    result['commands'].append('JMX access')
                    return result
            
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if 'nodetool' in alias.name.lower() or 'jmx' in alias.name.lower():
                        result['requires_ssh'] = True
                        result['reason'] = f'Imports {alias.name} module'
                        result['detection_method'] = f'import {alias.name}'
                        return result
    
    except Exception as e:
        # If we can't parse, be conservative and assume no SSH required
        pass
    
    return result


def _check_query_file_for_ssh(query_file_path: str) -> Dict:
    """
    Check if query file contains SSH-requiring operations.
    
    Looks for:
    - json.dumps({"operation": "nodetool", ...})
    - json.dumps({"operation": "jmx", ...})
    - Shell commands or system calls
    """
    result = {
        'requires_ssh': False,
        'reason': '',
        'commands': [],
        'detection_method': ''
    }
    
    try:
        with open(query_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for JSON-based nodetool operations
        if 'json.dumps' in content and '"nodetool"' in content:
            result['requires_ssh'] = True
            result['reason'] = 'Query file contains nodetool operations'
            result['detection_method'] = 'json.dumps with "nodetool"'
            
            # Try to extract specific commands
            commands = _extract_nodetool_commands(content)
            result['commands'] = commands
            
            return result
        
        # Check for JMX operations
        if 'json.dumps' in content and '"jmx"' in content:
            result['requires_ssh'] = True
            result['reason'] = 'Query file contains JMX operations'
            result['detection_method'] = 'json.dumps with "jmx"'
            result['commands'].append('JMX access')
            return result
        
        # Check for shell/system commands
        if any(keyword in content for keyword in ['subprocess', 'os.system', 'shell=True']):
            result['requires_ssh'] = True
            result['reason'] = 'Query file contains shell commands'
            result['detection_method'] = 'subprocess/os.system detected'
            result['commands'].append('shell commands')
            return result
    
    except Exception as e:
        pass
    
    return result


def _extract_nodetool_commands(content: str) -> List[str]:
    """
    Extract specific nodetool commands from query file content.
    
    Looks for patterns like:
    - "command": "status"
    - "command": "compactionstats"
    """
    commands = []
    
    # Pattern: "command": "status"
    pattern = r'"command"\s*:\s*"(\w+)"'
    matches = re.findall(pattern, content)
    
    for match in matches:
        commands.append(f'nodetool {match}')
    
    # Also look for common patterns
    if 'nodetool' in content.lower():
        if 'status' in content.lower() and 'nodetool status' not in commands:
            commands.append('nodetool status')
        if 'compaction' in content.lower() and 'nodetool compactionstats' not in commands:
            commands.append('nodetool compactionstats')
        if 'tpstats' in content.lower() and 'nodetool tpstats' not in commands:
            commands.append('nodetool tpstats')
    
    return commands if commands else ['nodetool commands']


def get_ssh_config_help_message(plugin_name: str, ssh_info: Dict) -> str:
    """
    Generate helpful message for configuring SSH access.
    
    Args:
        plugin_name: Name of plugin (e.g., 'cassandra', 'mongodb')
        ssh_info: SSH detection result from check_requires_ssh()
    
    Returns:
        str: Formatted help message
    """
    plugin_upper = plugin_name.upper()
    commands_str = ', '.join(ssh_info.get('commands', ['SSH commands']))
    
    message = f"""
  - ⚠️  Check requires SSH access for: {commands_str}
  
  💡 This check cannot be tested with containers.
  
  To test this check, configure an external {plugin_name.title()} server with SSH:
  
  Option 1 - Environment Variables (temporary):
    export {plugin_upper}_TEST_HOST=your-{plugin_name}-server.com
    export {plugin_upper}_TEST_PORT=9042
    export {plugin_upper}_TEST_USER={plugin_name}
    export {plugin_upper}_TEST_PASSWORD=secret
    export {plugin_upper}_SSH_HOST=your-{plugin_name}-server.com
    export {plugin_upper}_SSH_USER=ubuntu
    export {plugin_upper}_SSH_KEY=/home/user/.ssh/{plugin_name}_key
  
  Option 2 - Config File (persistent):
    Edit tools/config/settings.yaml:
    
    integration_tests:
      external_servers:
        {plugin_name}:
          enabled: true
          host: your-{plugin_name}-server.com
          port: 9042
          user: {plugin_name}
          password: secret
          ssh:
            host: your-{plugin_name}-server.com
            user: ubuntu
            key_file: /home/user/.ssh/{plugin_name}_key
  
  Then run: ./aidev.py "regenerate this check" to test with SSH access
"""
    
    return message


def connector_has_ssh_capability(connector) -> bool:
    """
    Check if a connector instance has SSH capability.
    
    Args:
        connector: Database connector instance
    
    Returns:
        bool: True if connector can execute SSH commands
    """
    # Check if connector has SSH client
    if hasattr(connector, 'ssh_client') and connector.ssh_client is not None:
        return True
    
    # Check if connector settings include SSH config
    if hasattr(connector, 'settings'):
        settings = connector.settings
        if isinstance(settings, dict):
            ssh_host = settings.get('ssh_host')
            ssh_user = settings.get('ssh_user')
            ssh_auth = settings.get('ssh_key_file') or settings.get('ssh_password')
            
            if ssh_host and ssh_user and ssh_auth:
                return True
    
    return False


def external_server_has_ssh_config(plugin_name: str, settings: dict) -> bool:
    """
    Check if external server is configured with SSH in settings.
    
    Args:
        plugin_name: Name of plugin
        settings: Full settings dictionary
    
    Returns:
        bool: True if external server with SSH is configured
    """
    import os
    
    # Check environment variables first
    plugin_upper = plugin_name.upper()
    env_ssh_host = os.environ.get(f'{plugin_upper}_SSH_HOST')
    env_ssh_user = os.environ.get(f'{plugin_upper}_SSH_USER')
    env_ssh_auth = (
        os.environ.get(f'{plugin_upper}_SSH_KEY') or 
        os.environ.get(f'{plugin_upper}_SSH_PASSWORD')
    )
    
    if env_ssh_host and env_ssh_user and env_ssh_auth:
        return True
    
    # Check settings file
    external_servers = settings.get('integration_tests', {}).get('external_servers', {})
    plugin_config = external_servers.get(plugin_name, {})
    
    if not plugin_config.get('enabled', False):
        return False
    
    ssh_config = plugin_config.get('ssh', {})
    ssh_host = ssh_config.get('host')
    ssh_user = ssh_config.get('user')
    ssh_auth = ssh_config.get('key_file') or ssh_config.get('password')
    
    return bool(ssh_host and ssh_user and ssh_auth)
