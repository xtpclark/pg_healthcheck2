"""
API Key Management Blueprint

Provides UI for company administrators to manage API keys:
- List company's API keys with usage statistics
- Generate new API keys
- Revoke existing API keys
- View detailed usage analytics
"""

import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, jsonify, abort, flash, redirect, url_for
from flask_login import login_required, current_user
from .utils import load_trends_config

bp = Blueprint('api_keys', __name__, url_prefix='/profile/api-keys')


# No helper function needed - using unified_generate_api_key() directly


@bp.route('/')
@login_required
def list_keys():
    """List all API keys for the user's accessible companies."""
    if not current_user.has_privilege('ManageAPIKeys'):
        abort(403)

    # Get user's accessible companies
    if not current_user.accessible_companies:
        flash('You must be associated with a company to manage API keys.', 'warning')
        return render_template('api_keys/list.html', keys=[], companies=[], selected_company_id=None, key_types=[])

    # Get selected company from query parameter (None = All Companies)
    selected_company_id = request.args.get('company_id', type=int)

    # Verify user has access to the selected company
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]

    if selected_company_id and selected_company_id not in accessible_company_ids:
        flash('You do not have access to that company.', 'danger')
        selected_company_id = None

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None

    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Get API keys - either for one company or all accessible companies
        if selected_company_id:
            # Single company - use stored procedure
            cursor.execute(
                "SELECT * FROM get_api_keys_for_companies(%s);",
                ([selected_company_id],)
            )
            keys = cursor.fetchall()
        else:
            # All companies - use stored procedure
            cursor.execute(
                "SELECT * FROM get_api_keys_for_companies(%s);",
                (accessible_company_ids,)
            )
            keys = cursor.fetchall()

        # Get active key types for the generate modal
        cursor.execute("SELECT * FROM get_active_api_key_types();")
        key_types = cursor.fetchall()

        return render_template(
            'api_keys/list.html',
            keys=keys,
            companies=current_user.accessible_companies,
            selected_company_id=selected_company_id,
            key_types=key_types
        )

    except psycopg2.Error as e:
        flash(f"Database error: {e}", "danger")
        return render_template(
            'api_keys/list.html',
            keys=[],
            companies=current_user.accessible_companies,
            selected_company_id=selected_company_id,
            key_types=[]
        )
    finally:
        if conn:
            conn.close()


@bp.route('/generate', methods=['POST'])
@login_required
def generate_key():
    """Generate a new API key for the user's company."""
    if not current_user.has_privilege('ManageAPIKeys'):
        abort(403)

    # Get user's accessible companies
    if not current_user.accessible_companies:
        return jsonify({'error': 'You must be associated with a company'}), 400

    # Get company_id from form data (passed from modal)
    company_id = request.form.get('company_id', type=int)

    # Verify user has access to the selected company
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]
    if not company_id or company_id not in accessible_company_ids:
        return jsonify({'error': 'Invalid company selection'}), 400

    # Get form data
    key_name = request.form.get('key_name', '').strip()
    key_type = request.form.get('key_type', 'customer').strip()
    expires_days_str = request.form.get('expires_days', '').strip()
    notes = request.form.get('notes', '').strip()

    if not key_name:
        return jsonify({'error': 'Key name is required'}), 400

    # Validate and convert expiration days if specified (optional override of api_key_types default)
    expires_days = None
    if expires_days_str:
        try:
            expires_days = int(expires_days_str)
            if expires_days <= 0:
                return jsonify({'error': 'Expiration days must be positive'}), 400
        except ValueError:
            return jsonify({'error': 'Invalid expiration days'}), 400

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None

    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Generate API key using unified stored procedure
        # This reads all defaults from api_key_types table and generates the key
        cursor.execute("""
            SELECT * FROM unified_generate_api_key(
                %s,  -- company_id
                %s,  -- key_name
                %s,  -- created_by_user_id
                %s,  -- key_type_code
                %s,  -- expires_days (override if provided)
                %s   -- notes
            );
        """, (company_id, key_name, current_user.id, key_type, expires_days, notes))

        result = cursor.fetchone()
        conn.commit()

        flash(f"API key '{key_name}' generated successfully!", "success")

        # Return the plain API key (only shown once!)
        return jsonify({
            'success': True,
            'api_key': result['api_key'],
            'key_id': result['key_id'],
            'key_name': key_name,
            'key_type': result['key_type_name'],
            'expires_at': result['expires_at'].isoformat() if result['expires_at'] else None,
            'max_submissions': result['max_submissions'],
            'rate_limit': {
                'period': result['rate_limit_period'],
                'count': result['rate_limit_count']
            } if result['rate_limit_period'] else None,
            'warning': 'IMPORTANT: Save this API key now. You will not be able to see it again!'
        })

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        return jsonify({'error': f'Database error: {str(e)}'}), 500
    finally:
        if conn:
            conn.close()


@bp.route('/<int:key_id>/revoke', methods=['POST'])
@login_required
def revoke_key(key_id):
    """Revoke (deactivate) an API key."""
    if not current_user.has_privilege('ManageAPIKeys'):
        abort(403)

    # Get user's accessible companies
    if not current_user.accessible_companies:
        return jsonify({'error': 'You must be associated with a company'}), 400

    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None

    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Revoke the key using stored procedure
        cursor.execute(
            "SELECT * FROM revoke_api_key(%s, %s);",
            (key_id, accessible_company_ids)
        )
        result = cursor.fetchone()

        if not result:
            conn.rollback()
            return jsonify({'error': 'API key not found or access denied'}), 404

        conn.commit()
        flash(f"API key '{result['key_name']}' revoked successfully.", "success")
        return jsonify({'success': True})

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        return jsonify({'error': f'Database error: {str(e)}'}), 500
    finally:
        if conn:
            conn.close()


@bp.route('/<int:key_id>/usage')
@login_required
def key_usage(key_id):
    """View detailed usage statistics for an API key."""
    if not current_user.has_privilege('ManageAPIKeys'):
        abort(403)

    # Get user's accessible companies
    if not current_user.accessible_companies:
        abort(403)

    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None

    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Get key details using stored procedure (includes access control)
        cursor.execute(
            "SELECT * FROM get_api_key_details(%s, %s);",
            (key_id, accessible_company_ids)
        )
        key_info = cursor.fetchone()

        if not key_info:
            abort(404)

        # Get recent submissions
        cursor.execute(
            "SELECT * FROM get_api_key_recent_submissions(%s, 20);",
            (key_id,)
        )
        recent_submissions = cursor.fetchall()

        # Get chart data (last 30 days)
        cursor.execute(
            "SELECT * FROM get_api_key_usage_chart_data(%s, 30);",
            (key_id,)
        )
        chart_data = cursor.fetchall()

        return render_template(
            'api_keys/usage.html',
            key_info=key_info,
            recent_submissions=recent_submissions,
            chart_data=chart_data
        )

    except psycopg2.Error as e:
        flash(f"Database error: {e}", "danger")
        abort(500)
    finally:
        if conn:
            conn.close()


@bp.route('/<int:key_id>/activate', methods=['POST'])
@login_required
def activate_key(key_id):
    """Reactivate a revoked API key."""
    if not current_user.has_privilege('ManageAPIKeys'):
        abort(403)

    # Get user's accessible companies
    if not current_user.accessible_companies:
        return jsonify({'error': 'You must be associated with a company'}), 400

    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None

    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Activate the key using stored procedure
        cursor.execute(
            "SELECT * FROM activate_api_key(%s, %s);",
            (key_id, accessible_company_ids)
        )
        result = cursor.fetchone()

        if not result:
            conn.rollback()
            return jsonify({'error': 'API key not found or access denied'}), 404

        conn.commit()
        flash(f"API key '{result['key_name']}' activated successfully.", "success")
        return jsonify({'success': True})

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        return jsonify({'error': f'Database error: {str(e)}'}), 500
    finally:
        if conn:
            conn.close()
