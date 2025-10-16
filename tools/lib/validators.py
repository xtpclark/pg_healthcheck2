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

def validate_and_correct_with_integration_tests(
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
        
        # Get the query function (assumes pattern: get_*_query)
        query_functions = [name for name in dir(query_module) 
                          if name.startswith('get_') and name.endswith('_query')]
        
        if not query_functions:
            return {
                'success': False,
                'error': 'No query function found (expected get_*_query pattern)',
                'database_type': connector.__class__.__name__
            }
        
        # Test each query function
        for func_name in query_functions:
            query_func = getattr(query_module, func_name)
            
            # Get query string
            query = query_func(connector)
            
            # Execute query
            formatted, raw = connector.execute_query(query, return_raw=True)
            
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
        
        # All queries passed
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
