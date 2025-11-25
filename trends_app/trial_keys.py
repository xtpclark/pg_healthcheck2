"""
Trial API Key Management for Freemium Submission.

This module handles trial key generation for prospects who want to submit
health check findings for AI analysis without having an Instaclustr account.
"""

import re
import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify, render_template
from datetime import datetime, timedelta

bp = Blueprint('trial_keys', __name__, url_prefix='/trial')


def is_valid_email(email):
    """Validate email format."""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def get_db_connection():
    """Get database connection from app config."""
    from flask import current_app
    db_config = current_app.config.get('DATABASE_CONFIG')
    return psycopg2.connect(**db_config)


@bp.route('/')
def trial_page():
    """
    Trial key request page.

    Public page where prospects can request a free trial API key.
    """
    return render_template('trial/request_key.html')


@bp.route('/api/generate-trial-key', methods=['POST'])
def generate_trial_key():
    """
    Generate trial API key for prospects.

    POST /trial/api/generate-trial-key

    Body (JSON or form-encoded):
        email (required): Valid email address
        company_name (optional): Company name for lead tracking
        phone (optional): Phone number for follow-up
        database_type (optional): PostgreSQL, Cassandra, Kafka, etc. (default: PostgreSQL)
        lead_source (optional): website, github, conference (default: website)

    Returns:
        201 Created:
            {
                "success": true,
                "api_key": "trial-abc123...",  # ONLY shown once!
                "key_id": 123,
                "expires_at": "2025-12-04T...",
                "max_submissions": 3,
                "message": "Trial key generated successfully"
            }

        400 Bad Request:
            {
                "error": "Invalid email address"
            }

        409 Conflict:
            {
                "error": "Active trial key already exists for this email",
                "hint": "Check your email for the existing key or wait for it to expire"
            }

    Notes:
        - Trial keys are prefixed with 'trial-'
        - Default limits: 3 submissions, expiration from api_key_types.default_expiry_days (fallback: 14 days)
        - Expiration days can be configured via Admin → API Key Types
        - Email is sent to the prospect with the key (TODO)
        - Sales CRM is notified (webhook) (TODO)
    """
    # Parse request data (support both JSON and form)
    if request.is_json:
        data = request.get_json()
    else:
        data = request.form.to_dict()

    email = data.get('email', '').strip().lower()
    company_name = data.get('company_name', '').strip()
    phone = data.get('phone', '').strip()
    database_type = data.get('database_type', 'PostgreSQL').strip()
    lead_source = data.get('lead_source', 'website').strip()

    # Validation
    if not email:
        return jsonify({'error': 'Email is required'}), 400

    if not is_valid_email(email):
        return jsonify({'error': 'Invalid email address'}), 400

    valid_db_types = ['PostgreSQL', 'Cassandra', 'Kafka', 'Redis', 'Valkey',
                      'OpenSearch', 'ClickHouse', 'MongoDB', 'MySQL']
    if database_type not in valid_db_types:
        return jsonify({
            'error': f'Invalid database_type. Must be one of: {", ".join(valid_db_types)}'
        }), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Generate trial key using unified stored procedure
        # This reads all defaults from api_key_types table (no hardcoded values!)
        cursor.execute("""
            SELECT * FROM unified_generate_api_key(
                NULL,  -- company_id (not required for trial keys)
                %s,    -- key_name (Trial Key - email/company)
                NULL,  -- created_by_user_id (no user for public trial signups)
                'trial',  -- key_type_code
                NULL,  -- expires_days (use api_key_types default)
                %s,    -- notes
                %s,    -- email
                %s,    -- phone
                %s,    -- database_type
                %s     -- lead_source
            );
        """, (
            f'Trial Key - {company_name or email}',  -- key_name
            f'Trial signup from {lead_source}',      -- notes
            email,
            phone or None,
            database_type,
            lead_source
        ))

        result = cursor.fetchone()
        conn.commit()
        conn.close()

        api_key = result['api_key']
        key_id = result['key_id']
        expires_at = result['expires_at']
        max_submissions = result['max_submissions']

        # TODO: Send email with trial key (implement in separate email module)
        # send_trial_key_email(email, api_key, company_name, database_type)

        # TODO: Notify sales CRM (implement webhook)
        # notify_crm_new_trial(email, company_name, database_type, lead_source)

        return jsonify({
            'success': True,
            'api_key': api_key,  # ONLY shown once - must save it!
            'key_id': key_id,
            'key_type': result['key_type_name'],
            'expires_at': expires_at.isoformat() if expires_at else None,
            'max_submissions': max_submissions,
            'rate_limit': {
                'period': result['rate_limit_period'],
                'count': result['rate_limit_count']
            } if result['rate_limit_period'] else None,
            'message': 'Trial key generated successfully! Save this key - it will not be shown again.'
        }), 201

    except psycopg2.Error as e:
        if 'Active trial key already exists' in str(e):
            return jsonify({
                'error': 'Active trial key already exists for this email',
                'hint': 'Check your email for the existing key or wait for it to expire'
            }), 409
        else:
            from flask import current_app
            current_app.logger.error(f"Trial key generation failed: {e}", exc_info=True)
            return jsonify({
                'error': 'Failed to generate trial key',
                'message': 'Please try again or contact support'
            }), 500


@bp.route('/api/check-trial-status', methods=['POST'])
def check_trial_status():
    """
    Check trial key status (submissions remaining, expiration).

    POST /trial/api/check-trial-status

    Headers:
        X-API-Key: trial-abc123...

    Returns:
        200 OK:
            {
                "key_type": "trial",
                "email": "user@example.com",
                "is_active": true,
                "submissions_used": 2,
                "submissions_remaining": 1,
                "expires_at": "2025-12-04T...",
                "days_remaining": 5
            }

        401 Unauthorized:
            {
                "error": "Invalid or expired trial key"
            }
    """
    api_key = request.headers.get('X-API-Key')

    if not api_key:
        return jsonify({'error': 'X-API-Key header required'}), 401

    if not api_key.startswith('trial-'):
        return jsonify({'error': 'Not a trial key'}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Validate key and get status
        cursor.execute("""
            SELECT * FROM validate_api_key(%s);
        """, (api_key,))

        result = cursor.fetchone()
        conn.close()

        if not result or not result['is_valid']:
            return jsonify({'error': 'Invalid or expired trial key'}), 401

        # Get detailed key info
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT
                key_type,
                email,
                is_active,
                usage_count,
                max_submissions,
                expires_at,
                database_type
            FROM api_keys
            WHERE id = %s;
        """, (result['key_id'],))

        key_info = cursor.fetchone()
        conn.close()

        days_remaining = None
        if key_info['expires_at']:
            days_remaining = (key_info['expires_at'] - datetime.now()).days

        return jsonify({
            'key_type': key_info['key_type'],
            'email': key_info['email'],
            'database_type': key_info['database_type'],
            'is_active': key_info['is_active'],
            'submissions_used': key_info['usage_count'],
            'submissions_remaining': result['submissions_remaining'],
            'expires_at': key_info['expires_at'].isoformat() + 'Z' if key_info['expires_at'] else None,
            'days_remaining': days_remaining
        }), 200

    except Exception as e:
        from flask import current_app
        current_app.logger.error(f"Trial status check failed: {e}", exc_info=True)
        return jsonify({'error': 'Failed to check trial status'}), 500


@bp.route('/success')
def success_page():
    """
    Trial key success page.

    Shown after trial key generation with instructions on how to use it.
    """
    return render_template('trial/success.html')


# Admin routes (require login)

@bp.route('/admin/trials')
def admin_list_trials():
    """
    Admin: List all trial keys with lead scoring.

    Requires: AdministerUsers privilege
    """
    from flask_login import login_required, current_user
    from flask import abort

    @login_required
    def inner():
        if not current_user.has_privilege('AdministerUsers'):
            abort(403)

        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            cursor.execute("""
                SELECT
                    api_key_id,
                    email,
                    database_type,
                    lead_source,
                    lead_status,
                    created_at,
                    usage_count,
                    max_submissions,
                    submission_count,
                    email_open_count,
                    link_click_count,
                    lead_score,
                    lead_temperature
                FROM v_lead_scores
                ORDER BY lead_score DESC, created_at DESC
                LIMIT 100;
            """)

            trials = cursor.fetchall()
            conn.close()

            return render_template('trial/admin_list.html', trials=trials)

        except Exception as e:
            from flask import current_app
            current_app.logger.error(f"Failed to load trials: {e}", exc_info=True)
            abort(500)

    return inner()


@bp.route('/admin/trials/<int:key_id>/convert', methods=['POST'])
def admin_convert_trial(key_id):
    """
    Admin: Convert trial key to customer key.

    POST /trial/admin/trials/<key_id>/convert

    Body:
        company_id: Target company ID

    Requires: AdministerUsers privilege
    """
    from flask_login import login_required, current_user
    from flask import abort, redirect, url_for, flash

    @login_required
    def inner():
        if not current_user.has_privilege('AdministerUsers'):
            abort(403)

        company_id = request.form.get('company_id', type=int)

        if not company_id:
            flash('Company ID required', 'error')
            return redirect(url_for('trial_keys.admin_list_trials'))

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT convert_trial_to_customer(%s, %s);
            """, (key_id, company_id))

            success = cursor.fetchone()[0]
            conn.commit()
            conn.close()

            if success:
                flash(f'Trial key {key_id} converted to customer successfully', 'success')
            else:
                flash(f'Failed to convert trial key {key_id}', 'error')

            return redirect(url_for('trial_keys.admin_list_trials'))

        except Exception as e:
            from flask import current_app
            current_app.logger.error(f"Trial conversion failed: {e}", exc_info=True)
            flash('Conversion failed', 'error')
            return redirect(url_for('trial_keys.admin_list_trials'))

    return inner()
