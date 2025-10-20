# lib/validators.py
"""Code validation and self-correction."""
from pathlib import Path
from typing import Optional

# Pyflakes import
try:
    from pyflakes.api import check as pyflakes_check
    from pyflakes.reporter import Reporter
    PYFLAKES_AVAILABLE = True
except ImportError:
    PYFLAKES_AVAILABLE = False
    pyflakes_check = None

class PyflakesReporter(Reporter):
    """Custom reporter to capture pyflakes errors."""
    def __init__(self):
        self.errors = []
    
    def unexpectedError(self, filename, msg):
        self.errors.append(f"Unexpected Error: {msg}")
    
    def syntaxError(self, filename, msg, lineno, offset, text):
        self.errors.append(f"Syntax Error at line {lineno}: {msg}")
    
    def flake(self, message):
        self.errors.append(str(message))

def clean_ai_response(response_text, response_type="json"):
    """
    Cleans markdown fences from AI responses.
    
    Args:
        response_text: Raw AI response
        response_type: Expected type (json, python)
        
    Returns:
        str: Cleaned response
    """
    import re
    import json
    
    if not response_text:
        return ""
    
    cleaned = response_text.strip()
    
    # Handle legacy format
    if "*AI JSON Response:*" in cleaned:
        cleaned = cleaned.split("*AI JSON Response:*")[1].strip()
    
    if response_type == "json":
        # Strategy 1: Try direct JSON parsing (no fences)
        try:
            json.loads(cleaned)
            return cleaned
        except json.JSONDecodeError:
            pass
        
        # Strategy 2: Extract from markdown code fence using regex
        # Patterns to try:
        # - ```json\n{...}\n```
        # - ```\n{...}\n```
        # - ```json{...}```  (no newlines)
        fence_patterns = [
            r'```json\s*\n(.*?)\n```',      # Standard JSON fence with newlines
            r'```json\s*(.*?)```',           # JSON fence without newlines
            r'```\s*\n(\{.*?\})\s*\n```',   # Generic fence with JSON object
            r'```\s*(\{.*?\})```',           # Generic fence, no newlines
        ]
        
        for pattern in fence_patterns:
            matches = re.findall(pattern, cleaned, re.DOTALL)
            if matches:
                extracted = matches[0].strip()
                try:
                    # Validate it's valid JSON
                    json.loads(extracted)
                    return extracted
                except json.JSONDecodeError:
                    # Try next pattern
                    continue
        
        # Strategy 3: Manual fence removal (fallback for malformed fences)
        if '```' in cleaned:
            lines = cleaned.split('\n')
            
            # Find fence start
            start_idx = -1
            for i, line in enumerate(lines):
                if line.strip().startswith('```'):
                    start_idx = i
                    break
            
            # Find fence end
            end_idx = -1
            if start_idx != -1:
                for i in range(start_idx + 1, len(lines)):
                    if lines[i].strip() == '```':
                        end_idx = i
                        break
            
            # Extract content between fences
            if start_idx != -1 and end_idx != -1:
                extracted = '\n'.join(lines[start_idx + 1:end_idx]).strip()
                try:
                    json.loads(extracted)
                    return extracted
                except json.JSONDecodeError:
                    pass
        
        # Strategy 4: Find outermost JSON object
        # Look for { ... } and extract it
        if '{' in cleaned and '}' in cleaned:
            start = cleaned.find('{')
            # Find matching closing brace
            brace_count = 0
            end = -1
            in_string = False
            escape_next = False
            
            for i in range(start, len(cleaned)):
                char = cleaned[i]
                
                # Handle string escaping
                if escape_next:
                    escape_next = False
                    continue
                
                if char == '\\':
                    escape_next = True
                    continue
                
                # Track if we're in a string
                if char == '"':
                    in_string = not in_string
                    continue
                
                # Only count braces outside of strings
                if not in_string:
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            end = i + 1
                            break
            
            if end > start:
                extracted = cleaned[start:end]
                try:
                    json.loads(extracted)
                    return extracted
                except json.JSONDecodeError:
                    pass
        
        # If we get here, return as-is and let the caller handle errors
        return cleaned
    
    elif response_type == "python":
        # Handle Python code fences
        fence_patterns = [
            r'```python\s*\n(.*?)\n```',
            r'```\s*\n(.*?)\n```',
        ]
        
        for pattern in fence_patterns:
            matches = re.findall(pattern, cleaned, re.DOTALL)
            if matches:
                return matches[0].strip()
        
        # Fallback: manual fence removal
        if '```' in cleaned:
            lines = cleaned.split('\n')
            
            # Remove first line if it's a fence
            if lines[0].strip().startswith('```'):
                lines = lines[1:]
            
            # Remove last line if it's a fence
            if lines and lines[-1].strip() == '```':
                lines = lines[:-1]
            
            return '\n'.join(lines).strip()
        
        return cleaned
    
    # Default: just strip whitespace
    return cleaned

def obsolete_clean_ai_response(response_text, response_type="json"):
    """
    Cleans markdown fences from AI responses.
    
    Args:
        response_text: Raw AI response
        response_type: Expected type (json, python)
        
    Returns:
        str: Cleaned response
    """
    fence_map = {"json": "```json", "python": "```python"}
    fence = fence_map.get(response_type, "```")
    
    if fence in response_text:
        return response_text.split(fence)[1].split("```")[0].strip()
    
    if "*AI JSON Response:*" in response_text:
        return response_text.split("*AI JSON Response:*")[1].strip()
    
    return response_text.strip()

def validate_and_correct_code(file_path, settings, max_attempts=3):
    """
    Validates and corrects Python code using pyflakes.
    
    Args:
        file_path: Path to Python file
        settings: Config dict (for AI correction)
        max_attempts: Maximum correction attempts
        
    Returns:
        bool: True if validation succeeded
    """
    if not PYFLAKES_AVAILABLE:
        print("  - ⚠️  Pyflakes not available, skipping validation")
        return True
    
    original_code = Path(file_path).read_text(encoding='utf-8')
    current_code = original_code

    for attempt in range(max_attempts):
        reporter = PyflakesReporter()
        pyflakes_check(current_code, str(file_path), reporter)

        if not reporter.errors:
            # Success!
            if attempt > 0:
                Path(file_path).write_text(current_code, encoding='utf-8')
                print(f"  - ✅ Code validated after {attempt} correction(s)")
            else:
                print("  - ✅ Code is valid")
            return True

        # Still has errors
        if attempt == max_attempts - 1:
            # Final attempt failed - rollback
            print(f"  - ❌ Self-correction failed after {max_attempts} attempts. Rolling back.")
            Path(file_path).write_text(original_code, encoding='utf-8')
            return False

        # Try to correct
        print(f"  - Attempt {attempt + 1}/{max_attempts}: Found {len(reporter.errors)} issues. Correcting...")
        error_string = "\n".join(reporter.errors)

        # Import here to avoid circular dependency
        from lib.ai_client import execute_ai_prompt
        from lib.templates import render_prompt
        
        corrector_prompt = render_prompt("code_corrector_prompt.adoc", {
            "original_code": current_code,
            "linter_errors": error_string
        })

        # Use high-reasoning model for correction
        correction_model = settings.get('high_reasoning_model', settings.get('ai_model'))
        corrected_code_raw = execute_ai_prompt(corrector_prompt, settings, model_override=correction_model)
        
        if not corrected_code_raw:
            print("  - ⚠️  AI failed to provide correction. Rolling back.")
            Path(file_path).write_text(original_code, encoding='utf-8')
            return False

        current_code = clean_ai_response(corrected_code_raw, "python")

    return False

def old_validate_and_correct_with_integration_tests(
    check_files: dict, 
    connector_factory,
    settings: dict,
    max_attempts: int = 3
) -> bool:
    """
    Validates check using integration tests and corrects via AI if needed.
    
    Args:
        check_files: Dict of file paths and content
            {
                'check_module': 'plugins/postgres/checks/check_vacuum.py',
                'query_file': 'plugins/postgres/utils/qrylib/vacuum_queries.py',
                'rule_file': 'plugins/postgres/rules/vacuum.json'
            }
        connector_factory: Function that returns a test database connector
        settings: AI settings for correction
        max_attempts: Maximum correction attempts
    
    Returns:
        bool: True if validation succeeded
    """
    from pathlib import Path
    import sys
    import importlib
    
    for attempt in range(max_attempts):
        print(f"  - Integration test attempt {attempt + 1}/{max_attempts}...")
        
        try:
            # 1. Execute integration test
            test_result = run_integration_test(check_files, connector_factory)
            
            if test_result['success']:
                if attempt > 0:
                    print(f"  - ✅ Integration test passed after {attempt} correction(s)")
                else:
                    print("  - ✅ Integration test passed")
                return True
            
            # 2. Test failed - check if we've exhausted attempts
            if attempt == max_attempts - 1:
                print(f"  - ❌ Integration test failed after {max_attempts} attempts")
                print(f"     Error: {test_result['error']}")
                return False
            
            # 3. Ask AI to fix the issue
            print(f"  - Integration test failed: {test_result['error']}")
            print(f"  - Asking AI to correct the issue...")
            
            corrected_content = request_ai_correction(
                original_files=check_files,
                test_error=test_result['error'],
                test_output=test_result.get('output', ''),
                database_type=test_result.get('database_type', 'unknown'),
                settings=settings
            )
            
            if not corrected_content:
                print("  - ⚠️  AI failed to provide correction")
                return False
            
            # 4. Write corrected files
            for file_key, corrected_code in corrected_content.items():
                file_path = Path(check_files[file_key])
                file_path.write_text(corrected_code, encoding='utf-8')
                print(f"  - Updated: {file_path}")
        
        except Exception as e:
            print(f"  - ❌ Integration test execution error: {e}")
            if attempt == max_attempts - 1:
                return False
    
    return False

def run_integration_test(check_files: dict, connector_factory) -> dict:
    """
    Executes integration test for the check.
    
    Args:
        check_files: Dict with check module and query file paths
        connector_factory: Function that returns test database connector
    
    Returns:
        dict: Test result with success status and error details
    """
    import importlib.util
    import inspect
    from pathlib import Path
    
    connector = None  # Initialize connector variable
    
    try:
        # Get connector
        connector = connector_factory()
        
        # Import query file dynamically
        query_file_path = Path(check_files['query_file'])
        spec = importlib.util.spec_from_file_location("query_module", query_file_path)
        query_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(query_module)
        
        # Get query functions from __all__ or fall back to naming convention
        if hasattr(query_module, '__all__'):
            query_function_names = query_module.__all__
        else:
            # Fallback: get_*_query pattern, excluding private functions (starting with _)
            query_function_names = [
                name for name in dir(query_module) 
                if name.startswith('get_') 
                and name.endswith('_query')
                and not name.startswith('_')  # Exclude private helpers like _get_query_base
            ]
        
        if not query_function_names:
            return {
                'success': False,
                'error': 'No query function found (expected __all__ list or get_*_query pattern)',
                'database_type': connector.__class__.__name__
            }
        
        # Test each query function
        for func_name in query_function_names:
            query_func = getattr(query_module, func_name)
            
            # Inspect function signature to handle parameters
            sig = inspect.signature(query_func)
            params = list(sig.parameters.keys())
            
            # Try to call the function
            try:
                if len(params) == 1:
                    # Simple case: only needs connector
                    query = query_func(connector)
                else:
                    # Function needs additional parameters
                    # Check if all extra parameters have defaults
                    required_params = [
                        p for p in sig.parameters.values() 
                        if p.default == inspect.Parameter.empty and p.name != 'connector'
                    ]
                    
                    if required_params:
                        # Has required parameters beyond connector - skip with message
                        print(f"  - ⚠️  Skipping {func_name} (requires parameters: {[p.name for p in required_params]})")
                        print(f"     Note: Add default values to parameters for integration testing.")
                        continue
                    
                    # All extra params have defaults - call with just connector
                    query = query_func(connector)
                    
            except TypeError as e:
                # Function signature issue - skip this query
                print(f"  - ⚠️  Skipping {func_name} (signature error: {str(e)})")
                continue
            
            # Execute query
            formatted, raw = connector.execute_query(query, return_raw=True)

            # Print the actual output
            print("\n" + "="*60)
            print(f"📊 Integration Test Output for {func_name}:")
            print("="*60)
            print("\n--- AsciiDoc Output ---")
            print(formatted)
            print("\n--- Structured Data (JSON) ---")
            import json
            print(json.dumps(raw, indent=2, default=str))
            print("="*60 + "\n")
         
            
            # Check for errors
            if "[ERROR]" in formatted:
                # Extract actual error message
                error_lines = [line for line in formatted.split('\n') 
                              if 'ERROR' in line or 'error' in line]
                error_msg = '\n'.join(error_lines) if error_lines else formatted
                
                return {
                    'success': False,
                    'error': error_msg,
                    'query': query,
                    'function': func_name,
                    'database_type': connector.__class__.__name__,
                    'output': formatted
                }
        
        # All queries passed (or were appropriately skipped)
        return {
            'success': True,
            'database_type': connector.__class__.__name__
        }
    
    except Exception as e:
        database_type = connector.__class__.__name__ if connector else 'Unknown'
        return {
            'success': False,
            'error': str(e),
            'database_type': database_type,
            'exception_type': type(e).__name__
        }

def run_integration_test_with_ssh_detection(
    check_files: dict, 
    connector_factory,
    plugin_name: str,
    settings: dict
) -> dict:
    """
    Enhanced integration test runner with SSH detection.
    
    Args:
        check_files: Dict with check module and query file paths
        connector_factory: Function that returns test database connector
        plugin_name: Name of the plugin (e.g., 'cassandra', 'postgres')
        settings: Full settings dictionary
    
    Returns:
        dict: Test result with status, skipped flag, and details
    """
    from lib.ssh_detection import (
        check_requires_ssh,
        connector_has_ssh_capability,
        external_server_has_ssh_config,
        get_ssh_config_help_message
    )
    
    # First, check if this check requires SSH
    ssh_info = check_requires_ssh(
        check_files.get('check_module'),
        check_files.get('query_file')
    )
    
    if ssh_info['requires_ssh']:
        # This check needs SSH - verify we have it configured
        
        # Check if external server has SSH configured
        if external_server_has_ssh_config(plugin_name, settings):
            print(f"  - ✅ SSH configured via external server")
            # Continue with normal test - SSH is available
        else:
            # No SSH configured - skip gracefully
            help_msg = get_ssh_config_help_message(plugin_name, ssh_info)
            print(help_msg)
            
            return {
                'success': True,  # Not a failure - just skipped
                'skipped': True,
                'reason': f'Check requires SSH: {ssh_info["reason"]}',
                'ssh_info': ssh_info,
                'database_type': plugin_name
            }
    
    # Either doesn't need SSH, or SSH is configured - run normal test
    connector = None
    
    try:
        # Get connector
        connector = connector_factory()
        
        # If check requires SSH, verify connector actually has it
        if ssh_info['requires_ssh']:
            if not connector_has_ssh_capability(connector):
                return {
                    'success': False,
                    'skipped': False,
                    'error': 'Connector does not have SSH capability despite configuration',
                    'database_type': plugin_name
                }
        
        # Run the actual integration test
        return run_integration_test(check_files, lambda: connector)
    
    except Exception as e:
        database_type = connector.__class__.__name__ if connector else plugin_name
        return {
            'success': False,
            'skipped': False,
            'error': str(e),
            'database_type': database_type,
            'exception_type': type(e).__name__
        }
    
    finally:
        # Note: Don't close connector here if it came from a factory
        # The factory/container is responsible for cleanup
        pass


# Update the validate_and_correct_with_integration_tests to use new function
def validate_and_correct_with_integration_tests(
    check_files: dict, 
    connector_factory,
    plugin_name: str,
    settings: dict,
    max_attempts: int = 3
) -> bool:
    """
    Enhanced validation with SSH detection and AI correction.
    
    Args:
        check_files: Dict of file paths
        connector_factory: Function that returns test database connector
        plugin_name: Name of the plugin
        settings: AI settings for correction
        max_attempts: Maximum correction attempts
    
    Returns:
        bool: True if validation succeeded or was appropriately skipped
    """
    from pathlib import Path
    
    for attempt in range(max_attempts):
        print(f"  - Integration test attempt {attempt + 1}/{max_attempts}...")
        
        try:
            # Run integration test with SSH detection
            test_result = run_integration_test_with_ssh_detection(
                check_files=check_files,
                connector_factory=connector_factory,
                plugin_name=plugin_name,
                settings=settings
            )
            
            # Check if test was skipped (SSH required but not available)
            if test_result.get('skipped', False):
                print(f"  - ⚠️  Integration test skipped: {test_result['reason']}")
                return True  # Skipped is not a failure
            
            # Check if test passed
            if test_result['success']:
                if attempt > 0:
                    print(f"  - ✅ Integration test passed after {attempt} correction(s)")
                else:
                    print("  - ✅ Integration test passed")
                return True
            
            # Test failed - check if we've exhausted attempts
            if attempt == max_attempts - 1:
                print(f"  - ❌ Integration test failed after {max_attempts} attempts")
                print(f"     Error: {test_result.get('error', 'Unknown error')}")
                return False
            
            # Ask AI to fix the issue
            print(f"  - Integration test failed: {test_result.get('error', 'Unknown')}")
            print(f"  - Asking AI to correct the issue...")
            
            corrected_content = request_ai_correction(
                original_files=check_files,
                test_error=test_result.get('error', ''),
                test_output=test_result.get('output', ''),
                database_type=test_result.get('database_type', 'unknown'),
                settings=settings
            )
            
            if not corrected_content:
                print("  - ⚠️  AI failed to provide correction")
                return False
            
            # Write corrected files
            for file_key, corrected_code in corrected_content.items():
                if file_key in check_files:
                    file_path = Path(check_files[file_key])
                    file_path.write_text(corrected_code, encoding='utf-8')
                    print(f"  - Updated: {file_path}")
        
        except Exception as e:
            print(f"  - ❌ Integration test execution error: {e}")
            if attempt == max_attempts - 1:
                return False
    
    return False


def request_ai_correction(
    original_files: dict,
    test_error: str,
    test_output: str,
    database_type: str,
    settings: dict
) -> dict:
    """
    Asks AI to correct integration test failure.
    
    Args:
        original_files: Original file paths and content
        test_error: Error message from integration test
        test_output: Full test output
        database_type: Database type (e.g., 'PostgreSQLConnector')
        settings: AI settings
    
    Returns:
        dict: Corrected file content keyed by file type
    """
    from lib.ai_client import execute_ai_prompt
    from lib.templates import render_prompt
    from pathlib import Path
    
    # Read current file contents
    file_contents = {}
    for file_key, file_path in original_files.items():
        try:
            file_contents[file_key] = Path(file_path).read_text(encoding='utf-8')
        except:
            file_contents[file_key] = ""
    
    # Build correction prompt
    prompt = render_prompt("integration_test_corrector_prompt.adoc", {
        "database_type": database_type,
        "test_error": test_error,
        "test_output": test_output,
        "query_file_content": file_contents.get('query_file', ''),
        "check_module_content": file_contents.get('check_module', ''),
        "query_file_path": original_files['query_file']
    })
    
    # Use high-reasoning model for complex debugging
    correction_model = settings.get('high_reasoning_model', settings.get('ai_model'))
    
    corrected_response = execute_ai_prompt(prompt, settings, model_override=correction_model)
    
    if not corrected_response:
        return None
    
    # Parse AI response - expecting JSON with corrected files
    import json
    from lib.validators import clean_ai_response
    
    try:
        corrected_json = json.loads(clean_ai_response(corrected_response, "json"))
        
        # Expected format:
        # {
        #   "query_file": "corrected query file content",
        #   "check_module": "corrected check module content (if needed)"
        # }
        
        return corrected_json
    
    except Exception as e:
        print(f"  - Failed to parse AI correction: {e}")
        return None
