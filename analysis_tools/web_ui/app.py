from flask import Flask, render_template, abort
import sys
from pathlib import Path

# Add the root directory to the Python path to allow for imports
# from other project directories.
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from analysis_tools.cli.monitor_trends import (
    load_trends_config,
    get_encryption_key,
    fetch_and_decrypt_runs
)
from cryptography.fernet import Fernet

app = Flask(__name__)

@app.route('/')
def dashboard():
    """
    Main dashboard route. In a real app, this would be protected by a login,
    and the company_id would be determined from the user's session.
    """
    # --- This logic is reused directly from the CLI tool ---
    config = load_trends_config()
    if not config:
        abort(500, description="Could not load trends.yaml configuration.")

    key_string = config.get('encryption_key')
    if not key_string:
        abort(500, description="Encryption key not found in trends.yaml.")
        
    fernet = Fernet(get_encryption_key(key_string))
    db_settings = config.get('database')
    
    # For now, we'll hardcode the company_id and limit
    # In the future, this will be dynamic.
    company_id = 1
    limit = 10
    
    runs = fetch_and_decrypt_runs(db_settings, fernet, company_id, limit)
    # --- End of reused logic ---

    if not runs:
        runs = []

    # Pass the decrypted data to the HTML template
    return render_template('dashboard.html', runs=runs)

if __name__ == '__main__':
    # Runs the Flask development server
    app.run(debug=True, port=5001)
