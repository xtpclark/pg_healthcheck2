# lib/config.py
"""Configuration management for aidev."""
import yaml
from pathlib import Path

def load_config(config_path):
    """
    Loads the main YAML configuration file.
    
    Args:
        config_path: Path to config.yaml
        
    Returns:
        dict: Configuration settings, or None on error
    """
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"⚠️  Warning: Main config file not found at '{config_path}'.")
        return None
    except yaml.YAMLError as e:
        print(f"❌ Error loading settings from {config_path}: {e}")
        return None

def validate_config(settings):
    """
    Validates required configuration fields.
    
    Args:
        settings: Config dict
        
    Returns:
        bool: True if valid
        
    Raises:
        ValueError: With helpful message if invalid
    """
    required = ['ai_provider', 'ai_endpoint', 'ai_model', 'ai_api_key']
    missing = [key for key in required if not settings.get(key)]
    
    if missing:
        raise ValueError(
            f"Missing required config fields: {', '.join(missing)}\n"
            f"Please check config/config.yaml"
        )
    
    return True
