# trends_app/utils.py
import yaml
from pathlib import Path
import re
from flask import current_app

def load_trends_config(config_path='config/trends.yaml'):
    """Loads the trend shipper configuration for the web app."""
    try:
        # The project root is two levels up from this file's directory
        project_root = Path(__file__).parent.parent
        with open(project_root / config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        # Use Flask's logger for better integration
        current_app.logger.error(f"Error loading trends.yaml: {e}")
        return None

def format_path(path):
    return re.sub(r"root\['(.*?)'\]", r'\1', path).replace("'", "")
