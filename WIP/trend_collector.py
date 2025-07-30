#!/usr/bin/env python3
"""
Trend Data Collector

A lightweight utility for automated collection of PostgreSQL health check data
for trend analysis without generating full AsciiDoc reports.

Usage:
    python3 trend_collector.py [config_file]

This script runs all health check modules, collects structured data,
and stores it in the trend database for analysis.
"""

import yaml
import sys
import psycopg2
import importlib
import inspect
import json
import logging
from pathlib import Path
from datetime import datetime
from decimal import Decimal
import time

# Import the main health check class
from pg_healthcheck import HealthCheck, CustomJsonEncoder

class TrendCollector(HealthCheck):
    """
    Lightweight health check collector for trend analysis.
    Inherits from HealthCheck but skips AsciiDoc generation.
    """
    
    def __init__(self, config_file, trend_config_file=None):
        # Load trend configuration if provided
        if trend_config_file:
            self.trend_config = self.load_trend_config(trend_config_file)
            # Merge trend config with main config for compatibility
            self.merge_configs(config_file, trend_config_file)
        else:
            self.trend_config = {}
        
        # Initialize parent class with merged config
        super().__init__(config_file)
        
        # Override paths for trend collection
        self.paths = self.get_trend_paths()
        
        # Track collection start time
        self.collection_start_time = time.time()
        
        # Setup logging based on trend config
        self.setup_logging()
    
    def load_trend_config(self, trend_config_file):
        """Load trend-specific configuration."""
        try:
            with open(trend_config_file, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Warning: Trend config file '{trend_config_file}' not found")
            return {}
        except yaml.YAMLError as e:
            print(f"Error parsing trend config: {e}")
            return {}
    
    def merge_configs(self, main_config_file, trend_config_file):
        """Merge trend config with main config for compatibility."""
        try:
            # Load main config
            with open(main_config_file, 'r') as f:
                main_config = yaml.safe_load(f)
            
            # Load trend config
            with open(trend_config_file, 'r') as f:
                trend_config = yaml.safe_load(f)
            
            # Merge configurations
            merged_config = main_config.copy()
            
            # Override with trend config settings
            if 'target_database' in trend_config:
                merged_config.update(trend_config['target_database'])
            
            # Add trend-specific settings
            if 'trend_storage_enabled' in trend_config:
                merged_config['trend_storage_enabled'] = trend_config['trend_storage_enabled']
            
            if 'trend_database' in trend_config:
                merged_config['trend_database'] = trend_config['trend_database']
            
            if 'trend_analysis' in trend_config:
                merged_config['trend_analysis'] = trend_config['trend_analysis']
            
            # Write merged config to temporary file
            import tempfile
            temp_config = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml')
            yaml.dump(merged_config, temp_config)
            temp_config.close()
            
            # Update config file path to use merged config
            self.merged_config_file = temp_config.name
            
        except Exception as e:
            print(f"Error merging configs: {e}")
            # Fall back to original config
            self.merged_config_file = main_config_file
    
    def setup_logging(self):
        """Setup logging based on trend configuration."""
        log_config = self.trend_config.get('output', {})
        log_file = log_config.get('log_file', 'logs/trend_collection.log')
        
        # Create log directory
        log_dir = Path(log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        verbose = self.trend_config.get('collection', {}).get('verbose_logging', False)
        log_level = logging.DEBUG if verbose else logging.INFO
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler() if verbose else logging.NullHandler()
            ]
        )
        
    def get_trend_paths(self):
        """Set up minimal paths for trend collection."""
        workdir = Path.cwd()
        sanitized_company_name = self._sanitize_company_name(self.settings['company_name'])
        
        return {
            'modules': workdir / 'modules',
            'comments': workdir / 'comments',
            'trend_out': workdir / 'trend_data' / sanitized_company_name,
            'structured_out': workdir / 'trend_data' / sanitized_company_name / 'structured'
        }
    
    def _sanitize_company_name(self, company_name):
        """Sanitize company name for file paths."""
        import re
        sanitized = re.sub(r'\W+', '_', company_name.lower()).strip('_')
        return sanitized[:50]  # Limit length
    
    def run_trend_collection(self):
        """
        Run health check modules and collect structured data for trend analysis.
        Skips AsciiDoc generation for performance.
        """
        print(f"Starting trend data collection for {self.settings['company_name']}...")
        
        # Connect to target database
        self.connect_db()
        
        # Track module execution
        total_modules = 0
        successful_modules = 0
        failed_modules = 0
        
        # Import and run modules from report configuration
        from report_config import REPORT_SECTIONS
        
        for section in REPORT_SECTIONS:
            # Skip header sections
            if section.get('actions') and section['actions'][0]['type'] == 'header':
                continue
                
            # Check section conditions
            if section.get('condition') and not self.settings.get(section['condition']['var'].lower()) == section['condition']['value']:
                continue
            
            for action in section['actions']:
                if action['type'] == 'module':
                    # Check action conditions
                    if action.get('condition') and not self.settings.get(action['condition']['var'].lower()) == action['condition']['value']:
                        continue
                    
                    module_name = action['module']
                    function_name = action['function']
                    
                    # Skip trend analysis modules to avoid recursion
                    if module_name in ['trend_analysis_storage', 'trend_analysis_viewer']:
                        continue
                    
                    total_modules += 1
                    
                    try:
                        print(f"  Running {module_name}.{function_name}...")
                        
                        # Run the module
                        module_output = self.run_module(module_name, function_name)
                        
                        # Track success/failure
                        if module_name in self.all_structured_findings:
                            module_status = self.all_structured_findings[module_name].get('status', 'unknown')
                            if module_status == 'success':
                                successful_modules += 1
                            elif module_status == 'error':
                                failed_modules += 1
                        else:
                            successful_modules += 1
                            
                    except Exception as e:
                        print(f"    Error in {module_name}: {e}")
                        failed_modules += 1
                        self.all_structured_findings[module_name] = {
                            "status": "error",
                            "error": str(e),
                            "details": "Module execution failed"
                        }
        
        # Calculate collection duration
        collection_duration = time.time() - self.collection_start_time
        
        # Add collection metadata
        self.all_structured_findings['collection_metadata'] = {
            "status": "success",
            "data": {
                "collection_timestamp": datetime.now().isoformat(),
                "total_modules": total_modules,
                "successful_modules": successful_modules,
                "failed_modules": failed_modules,
                "collection_duration_seconds": round(collection_duration, 2),
                "company_name": self.settings['company_name'],
                "database_name": self.settings['database'],
                "host_name": self.settings['host'],
                "pg_version": self._get_pg_version()
            }
        }
        
        # Store trend data if enabled
        if self.settings.get('trend_storage_enabled', False):
            self._store_trend_data(collection_duration)
        
        # Save structured data to file
        self._save_structured_data()
        
        # Close database connection
        self.conn.close()
        
        print(f"\nTrend collection completed:")
        print(f"  Total modules: {total_modules}")
        print(f"  Successful: {successful_modules}")
        print(f"  Failed: {failed_modules}")
        print(f"  Duration: {collection_duration:.2f} seconds")
        
        return {
            'total_modules': total_modules,
            'successful_modules': successful_modules,
            'failed_modules': failed_modules,
            'duration_seconds': collection_duration
        }
    
    def _get_pg_version(self):
        """Get PostgreSQL version."""
        try:
            result = self.execute_query("SELECT version();", is_check=True)
            if result and 'PostgreSQL' in result:
                # Extract version number
                import re
                match = re.search(r'PostgreSQL (\d+\.\d+)', result)
                if match:
                    return match.group(1)
            return None
        except:
            return None
    
    def _store_trend_data(self, collection_duration):
        """Store trend data in database."""
        try:
            from modules.trend_analysis_storage import TrendAnalysisStorage
            
            trend_db_settings = self.settings.get('trend_database', {})
            if not trend_db_settings:
                print("Warning: Trend database settings not found")
                return
            
            # Initialize trend storage
            trend_storage = TrendAnalysisStorage(
                trend_db_settings=trend_db_settings,
                company_name=self.settings['company_name'],
                database_name=self.settings['database'],
                host_name=self.settings['host']
            )
            
            # Connect and store data
            trend_storage.connect_trend_db()
            trend_storage.create_trend_schema()
            
            run_metadata = {
                'duration_seconds': collection_duration
            }
            
            run_id = trend_storage.store_health_check_run(
                self.all_structured_findings, 
                run_metadata
            )
            
            trend_storage.close()
            
            print(f"  Trend data stored with run_id: {run_id}")
            
        except Exception as e:
            print(f"  Error storing trend data: {e}")
    
    def _save_structured_data(self):
        """Save structured data to JSON file."""
        try:
            # Ensure output directory exists
            output_dir = self.paths['structured_out']
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Create filename with timestamp
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            filename = f"trend_data_{timestamp}.json"
            output_path = output_dir / filename
            
            # Save structured data
            with open(output_path, 'w') as f:
                json.dump(self.all_structured_findings, f, indent=2, cls=CustomJsonEncoder)
            
            print(f"  Structured data saved to: {output_path}")
            
        except Exception as e:
            print(f"  Error saving structured data: {e}")

def main():
    """Main entry point for trend collection."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Collect PostgreSQL health check data for trend analysis"
    )
    parser.add_argument(
        'config_file', 
        nargs='?', 
        default='config/config.yaml',
        help='Path to main configuration file (default: config/config.yaml)'
    )
    parser.add_argument(
        '--trend-config',
        default='config/trend_config.yaml',
        help='Path to trend configuration file (default: config/trend_config.yaml)'
    )
    parser.add_argument(
        '--environment', '-e',
        choices=['development', 'staging', 'production'],
        help='Environment to use from trend config'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be collected without running modules'
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize trend collector with trend config
        collector = TrendCollector(args.config_file, args.trend_config)
        
        # Apply environment-specific settings if specified
        if args.environment and args.environment in collector.trend_config:
            env_config = collector.trend_config[args.environment]
            print(f"Using {args.environment} environment configuration")
            
            # Override settings with environment-specific values
            if 'target_database' in env_config:
                collector.settings.update(env_config['target_database'])
            
            if 'trend_database' in env_config:
                collector.settings['trend_database'] = env_config['trend_database']
            
            if 'trend_storage_enabled' in env_config:
                collector.settings['trend_storage_enabled'] = env_config['trend_storage_enabled']
        
        # Check for dry run
        if args.dry_run:
            print("DRY RUN - Would collect data for:")
            print(f"  Company: {collector.settings.get('company_name', 'N/A')}")
            print(f"  Database: {collector.settings.get('database', 'N/A')}")
            print(f"  Host: {collector.settings.get('host', 'N/A')}")
            print(f"  Trend Storage: {collector.settings.get('trend_storage_enabled', False)}")
            return
        
        # Run collection
        results = collector.run_trend_collection()
        
        # Exit with appropriate code
        if results['failed_modules'] > 0:
            print(f"\nWarning: {results['failed_modules']} modules failed")
            sys.exit(1)
        else:
            print("\nTrend collection completed successfully")
            sys.exit(0)
            
    except FileNotFoundError as e:
        print(f"Error: Configuration file not found: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 