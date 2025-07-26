from flask import Flask, render_template, abort
import sys
from pathlib import Path
from deepdiff import DeepDiff
import re
import json

# Add the root directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from analysis_tools.cli.monitor_trends import (
    load_trends_config,
    get_encryption_key,
    fetch_and_decrypt_runs
)
from cryptography.fernet import Fernet

app = Flask(__name__)

def format_path(path):
    """Cleans up the deepdiff path for readability."""
    return re.sub(r"root\['(.*?)'\]", r'\1', path).replace("'", "")

@app.template_filter('format_path')
def jinja_format_path(path):
    """Makes the format_path function available in Jinja templates."""
    return format_path(path)

@app.route('/')
def dashboard():
    """Main dashboard route."""
    config = load_trends_config()
    if not config:
        abort(500, description="Could not load trends.yaml configuration.")

    key_string = config.get('encryption_key')
    if not key_string:
        abort(500, description="Encryption key not found in trends.yaml.")

    fernet = Fernet(get_encryption_key(key_string))
    db_settings = config.get('database')
    
    company_id = 1
    runs = fetch_and_decrypt_runs(db_settings, fernet, company_id, limit=2)
    
    changes_by_check = {}
    if len(runs) >= 2:
        diff = DeepDiff(runs[1]['findings'], runs[0]['findings'], ignore_order=True, view='tree')
        
        # --- FIX: Group the changes by check name here in the backend ---
        if diff:
            for change_type, items in diff.items():
                for item in items:
                    top_level_check = item.path(output_format='list')[0]
                    if top_level_check not in changes_by_check:
                        changes_by_check[top_level_check] = []
                    
                    summary = ""
                    if change_type == 'values_changed':
                        summary = f"🔄 Value at `{format_path(item.path())}` changed from **'{item.t1}'** to **'{item.t2}'**"
                    elif change_type == 'iterable_item_added':
                        summary = f"➕ Item added to `{format_path(item.path())}`: `{json.dumps(item.t2)}`"
                    elif change_type == 'iterable_item_removed':
                        summary = f"➖ Item removed from `{format_path(item.path())}`: `{json.dumps(item.t1)}`"
                    
                    if summary:
                        changes_by_check[top_level_check].append(summary)

    return render_template('dashboard.html', runs=runs, changes_by_check=changes_by_check)

if __name__ == '__main__':
    app.run(debug=True, port=5001)
