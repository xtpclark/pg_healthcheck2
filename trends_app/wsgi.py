#!/usr/bin/env python3
"""
WSGI entry point for production deployments.

This module is used by WSGI servers like Gunicorn to run the application.

Usage with Gunicorn:
    gunicorn -w 4 -b 0.0.0.0:5000 wsgi:app

Usage with systemd (in service file):
    ExecStart=/path/to/venv/bin/gunicorn --bind 0.0.0.0:5000 wsgi:app
"""

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
from trends_app import create_app

# Create the application instance
# This is the object that WSGI servers will use
app = create_app()

if __name__ == '__main__':
    # This allows testing the WSGI app directly
    app.run(host='0.0.0.0', port=5000, debug=False)
