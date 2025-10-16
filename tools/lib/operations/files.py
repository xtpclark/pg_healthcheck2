# lib/operations/files.py
"""File operations with rollback support."""
import os
import sys
from pathlib import Path
from typing import List, Dict

def execute_operations(operations, settings, run_integration_tests=True):
    """
    Executes file operations with optional integration testing.
    
    Args:
        operations: List of operation dicts
        settings: Config dict
        run_integration_tests: Whether to run integration tests after creation
    
    Returns:
        bool: True if all operations succeeded
    """
    if not operations:
        print("⚠️  AI did not provide any file operations to execute.")
        return False

    print("\n--- Executing File Creation Plan ---")
    created_paths = []
    check_files = {}  # Track check-related files for testing

    original_dir = Path.cwd()
    # files.py is at tools/lib/operations/files.py, go up 4 levels to project root
    project_root = Path(__file__).parent.parent.parent.parent
    
    # Verify we're in the right place by checking for key directories
    if not (project_root / 'plugins').exists() or not (project_root / 'tests').exists():
        print(f"❌ ERROR: Project root detection failed!")
        print(f"   Expected: pg_healthcheck2/")
        print(f"   Detected: {project_root}")
        print(f"   Missing: plugins/ or tests/ directory")
        return False
    
    print(f"  [DEBUG] Project root: {project_root}")
    os.chdir(project_root)
    
    # Add project root to Python path so we can import from tests/
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
        print(f"  [DEBUG] Added to sys.path: {project_root}")
    
    print(f"  [DEBUG] Changed to: {Path.cwd()}")
    print(f"  [DEBUG] plugins/ exists: {Path('plugins').exists()}")
    print(f"  [DEBUG] tests/ exists: {Path('tests').exists()}")

    try:
        for op in operations:
            action = op.get("action")
            path_str = op.get("path") or op.get("target_file")
            
            if not all([action, path_str]):
                print(f"⚠️  Skipping malformed operation: {op}")
                continue
            
            path = Path(path_str)
            
            if action == "create_file":
                print(f"  - Writing file: {path}")
                print(f"    [DEBUG] Absolute path: {path.resolve()}")
                content = op.get("content", "")
                
                if isinstance(content, list):
                    content = "\n".join(str(line) for line in content)
                
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding='utf-8')
                created_paths.append(path)
                
                # Track check-related files
                path_str = str(path)
                if path_str.startswith('plugins/') and '/checks/' in path_str and path.suffix == '.py':
                    check_files['check_module'] = path_str
                    print(f"    [DEBUG] Tracked as check_module")
                elif path_str.startswith('plugins/') and '/qrylib/' in path_str and path.suffix == '.py':
                    check_files['query_file'] = path_str
                    print(f"    [DEBUG] Tracked as query_file")
                elif path_str.startswith('plugins/') and '/rules/' in path_str and path.suffix == '.json':
                    check_files['rule_file'] = path_str
                    print(f"    [DEBUG] Tracked as rule_file")
                
                # Validate Python files with pyflakes
                if path.suffix == '.py':
                    from lib.validators import validate_and_correct_code
                    if not validate_and_correct_code(path, settings):
                        raise ValueError(f"Failed to validate: {path}")
            
            elif action == "create_directory":
                print(f"  - Creating directory: {path}")
                path.mkdir(parents=True, exist_ok=True)
                created_paths.append(path)
            
            else:
                print(f"⚠️  Unknown action '{action}'. Skipping.")
        
        # Run integration tests if enabled and we have check files
        if run_integration_tests and check_files.get('check_module') and check_files.get('query_file'):
            print("\n--- Running Integration Tests ---")
            print(f"  [DEBUG] check_module: {check_files.get('check_module')}")
            print(f"  [DEBUG] query_file: {check_files.get('query_file')}")
            
            # Determine database type from path
            plugin_name = extract_plugin_name(check_files['check_module'])
            print(f"  [DEBUG] Extracted plugin_name: {plugin_name}")
            
            if plugin_name:
                connector_factory = get_connector_factory(plugin_name, settings)
                
                if connector_factory:
                    from lib.validators import validate_and_correct_with_integration_tests
                    
                    integration_success = validate_and_correct_with_integration_tests(
                        check_files=check_files,
                        connector_factory=connector_factory,
                        settings=settings,
                        max_attempts=3
                    )
                    
                    if not integration_success:
                        print("  - ⚠️  Integration tests failed, but files were created")
                        print("  - You may need to fix the issues manually or re-generate")
                else:
                    print(f"  - ⚠️  No integration test connector available for {plugin_name}")
            else:
                print("  - ⚠️  Could not determine plugin type for integration testing")
        
        print("✅ File creation plan executed successfully.")
        return True

    except Exception as e:
        print(f"\n❌ Operation failed: {e}")
        print("🔄 Rolling back all changes...")
        
        for path in reversed(created_paths):
            try:
                if path.is_file():
                    path.unlink()
                    print(f"  - Removed file: {path}")
                elif path.is_dir() and not any(path.iterdir()):
                    path.rmdir()
                    print(f"  - Removed empty directory: {path}")
            except Exception as cleanup_error:
                print(f"  - ⚠️  Could not remove {path}: {cleanup_error}")
        
        print("❌ All changes rolled back.")
        return False

    finally:
        os.chdir(original_dir)


def extract_plugin_name(file_path: str) -> str:
    """Extract plugin name from file path."""
    import re
    # Normalize path separators and handle both absolute and relative paths
    normalized_path = file_path.replace('\\', '/')
    match = re.search(r'plugins/([^/]+)/', normalized_path)
    if match:
        return match.group(1)
    else:
        # Debug: print what we're trying to match
        print(f"  [DEBUG] Failed to extract plugin from: {file_path}")
        print(f"  [DEBUG] Normalized to: {normalized_path}")
        return None


def get_connector_factory(plugin_name: str, settings: dict):
    """
    Get a connector factory function for integration testing.
    
    Args:
        plugin_name: Name of plugin (e.g., 'postgres', 'mongodb')
        settings: Settings dict (may contain test DB config)
    
    Returns:
        Callable that returns a test database connector
    """
    # Check if integration testing is enabled
    if not settings.get('integration_tests', {}).get('enabled', False):
        print(f"  [DEBUG] Integration tests disabled in config")
        return None
    
    print(f"  [DEBUG] Attempting to import container for: {plugin_name}")
    
    try:
        if plugin_name == 'postgres':
            print(f"  [DEBUG] Importing: from tests.integration.framework import PostgreSQLContainer")
            from tests.integration.framework import PostgreSQLContainer
            print(f"  [DEBUG] Import successful!")
            
            def factory():
                version = settings.get('integration_tests', {}).get('postgres_version', '16')
                print(f"  [DEBUG] Creating PostgreSQL {version} container...")
                container = PostgreSQLContainer(version=version)
                container.start()
                return container.get_connector()
            
            return factory
        
        elif plugin_name == 'mongodb':
            from tests.integration.framework import MongoDBContainer
            
            def factory():
                version = settings.get('integration_tests', {}).get('mongodb_version', '7.0')
                container = MongoDBContainer(version=version)
                container.start()
                return container.get_connector()
            
            return factory
        
        elif plugin_name in ['valkey', 'redis']:
            from tests.integration.framework import ValkeyContainer, RedisContainer
            
            def factory():
                version = settings.get('integration_tests', {}).get('valkey_version', '7.2')
                if plugin_name == 'redis':
                    container = RedisContainer(version=version)
                else:
                    container = ValkeyContainer(version=version)
                container.start()
                return container.get_connector()
            
            return factory
        
        elif plugin_name == 'cassandra':
            print(f"  [DEBUG] Importing: from tests.integration.framework import CassandraContainer")
            from tests.integration.framework import CassandraContainer
            print(f"  [DEBUG] Import successful!")
            
            def factory():
                version = settings.get('integration_tests', {}).get('cassandra_version', '4.1')
                print(f"  [DEBUG] Creating Cassandra {version} container...")
                container = CassandraContainer(version=version)
                container.start()
                return container.get_connector()
            
            return factory

        elif plugin_name == 'kafka':
            print(f"  [DEBUG] Importing: from tests.integration.framework import KafkaContainer")
            from tests.integration.framework import KafkaContainer
            print(f"  [DEBUG] Import successful!")
            
            def factory():
                version = settings.get('integration_tests', {}).get('kafka_version', 'latest')
                print(f"  [DEBUG] Creating Kafka container (Redpanda)...")
                container = KafkaContainer(version=version)
                container.start()
                return container.get_connector()
            
            return factory
        
        else:
            print(f"  [DEBUG] No container implementation for: {plugin_name} in operations/files.py")
            return None
    
    except ImportError as e:
        print(f"  [DEBUG] Import failed: {e}")
        print(f"  [DEBUG] sys.path: {sys.path[:3]}...")  # Show first 3 entries
        import traceback
        traceback.print_exc()
        return None
