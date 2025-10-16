#!/usr/bin/env python3
"""
AI Developer Agent for the Health Check Framework.

Entry point for the conversational AI development assistant.
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path

from lib.config import load_config, validate_config
from lib.sanitizer import sanitize_user_input
from lib.intent import recognize_intent_and_dispatch

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f'logs/aidev_{datetime.now():%Y%m%d_%H%M%S}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='AI Developer Agent.')
    parser.add_argument('query', nargs='?', default=None, 
                       help='Your development request in natural language.')
    parser.add_argument('--config', default='config/config.yaml', 
                       help='Path to the main configuration file')
    args = parser.parse_args()

    # Load and validate configuration
    settings = load_config(args.config)
    if not settings:
        print("Please ensure your config/config.yaml is set up correctly.")
        return
    
    try:
        validate_config(settings)
    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
        return

    # Single query mode
    if args.query:
        try:
            sanitized = sanitize_user_input(args.query)
            recognize_intent_and_dispatch(sanitized, settings)
        except ValueError as e:
            print(f"❌ Invalid input: {e}")
        return

    # Interactive mode
    print("🤖 AI Developer Agent is ready.")
    print("   (e.g., 'generate a comprehensive set of postgres health checks')")
    print("   Type 'quit' or 'exit' to end the session.")
    
    while True:
        try:
            user_input = input("> ")
            if user_input.lower() in ['quit', 'exit']:
                break
            if user_input:
                try:
                    sanitized = sanitize_user_input(user_input)
                    recognize_intent_and_dispatch(sanitized, settings)
                except ValueError as e:
                    print(f"❌ Invalid input: {e}")
        except KeyboardInterrupt:
            break
    
    print("Goodbye!")

if __name__ == '__main__':
    main()
