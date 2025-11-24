#!/usr/bin/env python3
"""
Entry point for running the Trends App.

This script creates and runs the Flask application.

Usage:
    python run.py
    python run.py --host 0.0.0.0 --port 5000 --debug
"""

if __name__ == '__main__':
    import argparse
    import sys
    import os

    # Get the directory containing this script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)

    # Add parent directory to Python path FIRST
    # This allows Python to import 'trends_app' as a package
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    # Now we can import trends_app as a package
    try:
        from trends_app import create_app
    except ImportError as e:
        print(f"Error importing trends_app: {e}")
        print(f"\nCurrent directory: {current_dir}")
        print(f"Parent directory: {parent_dir}")
        print(f"Python path: {sys.path[:3]}")
        print("\nMake sure you have:")
        print("1. All required dependencies installed:")
        print("   pip install -r requirements-minimal.txt")
        print("2. Config file at ../config/trends.yaml")
        sys.exit(1)

    # Create the app
    app = create_app()

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run the Trends App')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to (default: 5000)')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')

    args = parser.parse_args()

    print("=" * 70)
    print("Trends App Starting")
    print("=" * 70)
    print(f"Host: {args.host}")
    print(f"Port: {args.port}")
    print(f"Debug: {args.debug}")
    print(f"Dashboard: http://{'localhost' if args.host == '0.0.0.0' else args.host}:{args.port}/dashboard")
    print(f"Health: http://{'localhost' if args.host == '0.0.0.0' else args.host}:{args.port}/api/health")
    print("=" * 70)
    print()

    app.run(
        host=args.host,
        port=args.port,
        debug=args.debug
    )
