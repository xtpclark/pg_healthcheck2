#!/usr/bin/env python3
"""
Module Template Generator for PostgreSQL Health Check System

This utility helps create new modules following the established patterns in the health check system.
It generates a complete module template with proper structure, error handling, and documentation.

Usage:
    python module_template_generator.py --module-name "my_new_module" --description "Analyzes something important"
"""

import argparse
import os
import re
from datetime import datetime

class ModuleTemplateGenerator:
    def __init__(self):
        self.template_dir = "module_templates"
        self.modules_dir = "modules"
        
    def sanitize_name(self, name):
        """Convert module name to valid Python filename"""
        # Replace spaces and special chars with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name.lower())
        # Remove multiple underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        return sanitized
    
    def generate_function_name(self, module_name):
        """Generate function name from module name"""
        return f"run_{self.sanitize_name(module_name)}"
    
    def create_module_template(self, module_name, description, author="", category="analysis"):
        """Generate a complete module template"""
        
        sanitized_name = self.sanitize_name(module_name)
        function_name = self.generate_function_name(module_name)
        
        template = f'''def {function_name}(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    {description}
    
    This module analyzes {module_name.lower()} to identify potential issues and provide recommendations.
    
    Args:
        cursor: Database cursor for direct queries
        settings: Configuration settings dictionary
        execute_query: Function to execute queries with error handling
        execute_pgbouncer: Function to execute queries against PgBouncer
        all_structured_findings: Dictionary to store all structured findings
    
    Returns:
        tuple: (formatted_asciidoc_content, structured_data)
    """
    adoc_content = ["=== {module_name}", "{description}\n"]
    structured_data = {{}} # Dictionary to hold structured findings for this module
    
    # Show queries if requested
    if settings['show_qry'] == 'true':
        adoc_content.append("{module_name} queries:")
        adoc_content.append("[,sql]\\n----")
        adoc_content.append("-- Example query 1")
        adoc_content.append("SELECT * FROM example_table WHERE condition = 'value';")
        adoc_content.append("-- Example query 2") 
        adoc_content.append("SELECT count(*) FROM another_table;")
        adoc_content.append("----")

    # Define your queries here
    queries = [
        (
            "Primary Analysis Query", 
            "SELECT * FROM example_table WHERE condition = 'value' LIMIT %(limit)s;", 
            True,  # Condition to run this query
            "primary_analysis" # Data key for structured findings
        ),
        (
            "Secondary Analysis Query", 
            "SELECT count(*) as total_count FROM another_table;", 
            True,  # Condition to run this query
            "secondary_analysis" # Data key for structured findings
        )
        # Add more queries as needed
    ]

    # Process each query
    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{{title}}\\n[NOTE]\\n====\\nQuery not applicable.\\n====\\n")
            structured_data[data_key] = {{"status": "not_applicable", "reason": "Query not applicable due to condition."}}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = {{'limit': settings['row_limit']}} if '%(limit)s' in query else None
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{{title}}\\n{{formatted_result}}")
            structured_data[data_key] = {{"status": "error", "details": raw_result}}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {{"status": "success", "data": raw_result}} # Store raw data
    
    # Add analysis and recommendations
    adoc_content.append("[TIP]\\n====\\n")
    adoc_content.append("Key recommendations for {module_name.lower()}:\\n")
    adoc_content.append("* Monitor regularly for optimal performance\\n")
    adoc_content.append("* Consider implementing automated alerts\\n")
    adoc_content.append("* Review and optimize as needed\\n")
    adoc_content.append("====\\n")
    
    # Add Aurora-specific notes if applicable
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\\n====\\n")
        adoc_content.append("AWS RDS Aurora considerations for {module_name.lower()}:\\n")
        adoc_content.append("* Monitor via CloudWatch metrics\\n")
        adoc_content.append("* Consider Aurora-specific optimizations\\n")
        adoc_content.append("====\\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\\n".join(adoc_content), structured_data
'''
        
        return template
    
    def create_test_template(self, module_name, function_name):
        """Generate a test template for the module"""
        
        test_template = f'''#!/usr/bin/env python3
"""
Test module for {module_name}
"""

import unittest
from unittest.mock import Mock, MagicMock
import sys
import os

# Add the modules directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'modules'))

# Import the module to test
from {self.sanitize_name(module_name)} import {function_name}

class Test{module_name.replace(' ', '').replace('_', '')}(unittest.TestCase):
    """Test cases for {module_name} module"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_cursor = Mock()
        self.mock_settings = {{
            'show_qry': 'false',
            'row_limit': 10,
            'database': 'test_db',
            'is_aurora': 'false'
        }}
        self.mock_execute_query = Mock()
        self.mock_execute_pgbouncer = Mock()
        self.all_structured_findings = {{}}
    
    def test_module_import(self):
        """Test that the module can be imported"""
        self.assertIsNotNone({function_name})
    
    def test_function_signature(self):
        """Test that the function has the correct signature"""
        # This will raise an error if the function doesn't exist or has wrong signature
        try:
            {function_name}(self.mock_cursor, self.mock_settings, 
                          self.mock_execute_query, self.mock_execute_pgbouncer, 
                          self.all_structured_findings)
        except TypeError as e:
            self.fail(f"Function signature error: {{e}}")
    
    def test_successful_execution(self):
        """Test successful execution with mock data"""
        # Mock successful query execution
        self.mock_execute_query.return_value = ("Mock formatted result", [{{"test": "data"}}])
        
        adoc_content, structured_data = {function_name}(
            self.mock_cursor, self.mock_settings, 
            self.mock_execute_query, self.mock_execute_pgbouncer, 
            self.all_structured_findings
        )
        
        # Verify return types
        self.assertIsInstance(adoc_content, str)
        self.assertIsInstance(structured_data, dict)
        
        # Verify content contains expected sections
        self.assertIn("===", adoc_content)
    
    def test_error_handling(self):
        """Test error handling in query execution"""
        # Mock query execution error
        self.mock_execute_query.return_value = ("[ERROR] Test error", None)
        
        adoc_content, structured_data = {function_name}(
            self.mock_cursor, self.mock_settings, 
            self.mock_execute_query, self.mock_execute_pgbouncer, 
            self.all_structured_findings
        )
        
        # Verify error is handled gracefully
        self.assertIn("[ERROR]", adoc_content)
        self.assertIn("error", str(structured_data))

if __name__ == '__main__':
    unittest.main()
'''
        
        return test_template
    
    def create_config_template(self, module_name):
        """Generate configuration template for the module"""
        
        config_template = f'''# Configuration for {module_name} module
# Add this section to your config.yaml file

{self.sanitize_name(module_name)}:
  enabled: true
  # Add module-specific settings here
  # Example:
  # threshold: 100
  # alert_level: warning
'''
        
        return config_template
    
    def create_readme_template(self, module_name, description, author=""):
        """Generate README template for the module"""
        
        readme_template = f'''# {module_name} Module

## Overview
{description}

## Purpose
This module analyzes {module_name.lower()} to identify potential issues and provide actionable recommendations.

## Features
- Comprehensive analysis of {module_name.lower()}
- Structured data output for integration
- Aurora-specific recommendations
- Configurable thresholds and limits

## Configuration
Add the following section to your `config.yaml`:

```yaml
{self.sanitize_name(module_name)}:
  enabled: true
  # Add module-specific settings here
```

## Usage
The module is automatically included in the health check when enabled in configuration.

## Output
The module generates:
- AsciiDoc formatted report section
- Structured data for trend analysis
- Recommendations and best practices

## Dependencies
- PostgreSQL system catalogs
- Standard health check framework

## Author
{author or "Health Check System"}

## Version
1.0.0

## Last Updated
{datetime.now().strftime('%Y-%m-%d')}
'''
        
        return readme_template
    
    def generate_module(self, module_name, description, author="", category="analysis", 
                       create_test=True, create_config=True, create_readme=True):
        """Generate a complete module with all supporting files"""
        
        sanitized_name = self.sanitize_name(module_name)
        function_name = self.generate_function_name(module_name)
        
        # Create modules directory if it doesn't exist
        os.makedirs(self.modules_dir, exist_ok=True)
        
        # Generate main module file
        module_content = self.create_module_template(module_name, description, author, category)
        module_file = os.path.join(self.modules_dir, f"{sanitized_name}.py")
        
        with open(module_file, 'w') as f:
            f.write(module_content)
        
        print(f"✓ Created module: {module_file}")
        
        # Generate test file
        if create_test:
            test_content = self.create_test_template(module_name, function_name)
            test_file = f"test_{sanitized_name}.py"
            
            with open(test_file, 'w') as f:
                f.write(test_content)
            
            print(f"✓ Created test: {test_file}")
        
        # Generate config template
        if create_config:
            config_content = self.create_config_template(module_name)
            config_file = f"{sanitized_name}_config.yaml"
            
            with open(config_file, 'w') as f:
                f.write(config_content)
            
            print(f"✓ Created config template: {config_file}")
        
        # Generate README
        if create_readme:
            readme_content = self.create_readme_template(module_name, description, author)
            readme_file = f"{sanitized_name}_README.md"
            
            with open(readme_file, 'w') as f:
                f.write(readme_content)
            
            print(f"✓ Created README: {readme_file}")
        
        # Print integration instructions
        print(f"\\n📋 Integration Instructions:")
        print(f"1. Add the following import to pg_healthcheck.py:")
        print(f"   from modules.{sanitized_name} import {function_name}")
        print(f"")
        print(f"2. Add the function call in the main execution loop:")
        print(f"   adoc_content, structured_data = {function_name}(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings)")
        print(f"   all_structured_findings.update(structured_data)")
        print(f"")
        print(f"3. Update your config.yaml with the module settings")
        print(f"")
        print(f"4. Run tests: python test_{sanitized_name}.py")
        
        return {
            'module_file': module_file,
            'test_file': f"test_{sanitized_name}.py" if create_test else None,
            'config_file': f"{sanitized_name}_config.yaml" if create_config else None,
            'readme_file': f"{sanitized_name}_README.md" if create_readme else None
        }

def main():
    """Main function to handle command line arguments"""
    parser = argparse.ArgumentParser(description='Generate PostgreSQL Health Check Module Templates')
    parser.add_argument('--module-name', required=True, help='Name of the module (e.g., "Connection Pool Analysis")')
    parser.add_argument('--description', required=True, help='Description of what the module does')
    parser.add_argument('--author', default='', help='Author of the module')
    parser.add_argument('--category', default='analysis', help='Category of the module (analysis, security, performance, etc.)')
    parser.add_argument('--no-test', action='store_true', help='Skip creating test file')
    parser.add_argument('--no-config', action='store_true', help='Skip creating config template')
    parser.add_argument('--no-readme', action='store_true', help='Skip creating README')
    
    args = parser.parse_args()
    
    generator = ModuleTemplateGenerator()
    
    try:
        result = generator.generate_module(
            module_name=args.module_name,
            description=args.description,
            author=args.author,
            category=args.category,
            create_test=not args.no_test,
            create_config=not args.no_config,
            create_readme=not args.no_readme
        )
        
        print(f"\\n🎉 Module '{args.module_name}' generated successfully!")
        print(f"Files created: {list(result.values())}")
        
    except Exception as e:
        print(f"❌ Error generating module: {e}")
        return 1
    
    return 0

if __name__ == '__main__':
    exit(main()) 