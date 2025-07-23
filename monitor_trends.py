#!/usr/bin/env python3
"""
Trend Collection Monitor

A utility to monitor trend collection status and provide insights.
"""

import yaml
import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
import argparse

def load_config(config_file):
    """Load configuration file."""
    try:
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: Config file {config_file} not found.")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing config file: {e}")
        sys.exit(1)

def check_recent_collections(trend_data_dir, days_back=7):
    """Check for recent trend data collections."""
    trend_dir = Path(trend_data_dir)
    if not trend_dir.exists():
        print(f"Trend data directory not found: {trend_data_dir}")
        return []
    
    cutoff_date = datetime.now() - timedelta(days=days_back)
    recent_files = []
    
    for file_path in trend_dir.rglob("*.json"):
        try:
            file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
            if file_time >= cutoff_date:
                recent_files.append({
                    'file': file_path,
                    'modified': file_time,
                    'size': file_path.stat().st_size
                })
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
    
    return sorted(recent_files, key=lambda x: x['modified'], reverse=True)

def analyze_collection_data(file_path):
    """Analyze a single collection data file."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Extract metadata
        metadata = data.get('collection_metadata', {}).get('data', {})
        
        return {
            'timestamp': metadata.get('collection_timestamp'),
            'total_modules': metadata.get('total_modules', 0),
            'successful_modules': metadata.get('successful_modules', 0),
            'failed_modules': metadata.get('failed_modules', 0),
            'duration_seconds': metadata.get('collection_duration_seconds', 0),
            'company_name': metadata.get('company_name'),
            'database_name': metadata.get('database_name'),
            'host_name': metadata.get('host_name'),
            'pg_version': metadata.get('pg_version')
        }
    except Exception as e:
        print(f"Error analyzing {file_path}: {e}")
        return None

def check_log_file(log_file):
    """Check the trend collection log file."""
    log_path = Path(log_file)
    if not log_path.exists():
        return None
    
    try:
        # Get last few lines of log
        with open(log_path, 'r') as f:
            lines = f.readlines()
            last_lines = lines[-10:] if len(lines) > 10 else lines
        
        # Check for errors in last 24 hours
        cutoff_time = datetime.now() - timedelta(hours=24)
        recent_errors = []
        
        for line in lines:
            if 'ERROR' in line or 'Error' in line:
                try:
                    # Try to parse timestamp from log line
                    timestamp_str = line.split(' - ')[0]
                    log_time = datetime.fromisoformat(timestamp_str.replace(' ', 'T'))
                    if log_time >= cutoff_time:
                        recent_errors.append(line.strip())
                except:
                    # If timestamp parsing fails, include line anyway
                    recent_errors.append(line.strip())
        
        return {
            'last_lines': last_lines,
            'recent_errors': recent_errors,
            'file_size': log_path.stat().st_size,
            'last_modified': datetime.fromtimestamp(log_path.stat().st_mtime)
        }
    except Exception as e:
        print(f"Error reading log file: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Monitor trend collection status")
    parser.add_argument('--config', default='config/config.yaml', help='Main config file')
    parser.add_argument('--trend-config', default='config/trend_config.yaml', help='Trend config file')
    parser.add_argument('--days', type=int, default=7, help='Days to look back')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    print("=== PostgreSQL Health Check Trend Collection Monitor ===\n")
    
    # Load configurations
    try:
        main_config = load_config(args.config)
        trend_config = load_config(args.trend_config)
    except Exception as e:
        print(f"Error loading configs: {e}")
        return
    
    # Get output settings
    output_config = trend_config.get('output', {})
    log_file = output_config.get('log_file', 'logs/trend_collection.log')
    structured_data_dir = output_config.get('structured_data_dir', 'trend_data')
    
    print(f"Configuration:")
    print(f"  Log file: {log_file}")
    print(f"  Data directory: {structured_data_dir}")
    print(f"  Monitoring period: {args.days} days")
    print()
    
    # Check recent collections
    print("=== Recent Collections ===")
    recent_files = check_recent_collections(structured_data_dir, args.days)
    
    if not recent_files:
        print("No recent collection files found.")
    else:
        print(f"Found {len(recent_files)} collection(s) in the last {args.days} days:")
        print()
        
        for file_info in recent_files:
            analysis = analyze_collection_data(file_info['file'])
            if analysis:
                print(f"📊 Collection: {analysis['timestamp']}")
                print(f"   Company: {analysis['company_name']}")
                print(f"   Database: {analysis['database_name']} on {analysis['host_name']}")
                print(f"   PostgreSQL: {analysis['pg_version']}")
                print(f"   Modules: {analysis['successful_modules']}/{analysis['total_modules']} successful")
                print(f"   Duration: {analysis['duration_seconds']:.2f} seconds")
                print(f"   File: {file_info['file'].name}")
                print()
    
    # Check log file
    print("=== Log Analysis ===")
    log_info = check_log_file(log_file)
    
    if not log_info:
        print("Log file not found or not accessible.")
    else:
        print(f"Log file: {log_file}")
        print(f"Size: {log_info['file_size']} bytes")
        print(f"Last modified: {log_info['last_modified']}")
        print()
        
        if log_info['recent_errors']:
            print(f"⚠️  Found {len(log_info['recent_errors'])} error(s) in last 24 hours:")
            for error in log_info['recent_errors'][:5]:  # Show last 5 errors
                print(f"   {error}")
            if len(log_info['recent_errors']) > 5:
                print(f"   ... and {len(log_info['recent_errors']) - 5} more")
        else:
            print("✅ No recent errors found")
        
        if args.verbose and log_info['last_lines']:
            print("\nLast log entries:")
            for line in log_info['last_lines']:
                print(f"   {line.rstrip()}")
    
    # Summary
    print("\n=== Summary ===")
    if recent_files:
        latest_file = recent_files[0]
        latest_analysis = analyze_collection_data(latest_file['file'])
        if latest_analysis:
            hours_ago = (datetime.now() - datetime.fromisoformat(latest_analysis['timestamp'])).total_seconds() / 3600
            print(f"Latest collection: {hours_ago:.1f} hours ago")
            print(f"Success rate: {(latest_analysis['successful_modules'] / latest_analysis['total_modules'] * 100):.1f}%")
            
            if hours_ago > 24:
                print("⚠️  Warning: No recent collections (more than 24 hours ago)")
            else:
                print("✅ Recent collections detected")
    else:
        print("⚠️  Warning: No recent collections found")
    
    if log_info and log_info['recent_errors']:
        print("⚠️  Warning: Recent errors detected in log file")
    elif log_info:
        print("✅ Log file shows no recent errors")

if __name__ == '__main__':
    main() 