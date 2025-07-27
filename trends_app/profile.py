from flask import Blueprint, render_template, request, jsonify, current_app, redirect, url_for, flash
from flask_login import login_required, current_user
import psycopg2
from .utils import load_trends_config

bp = Blueprint('profile', __name__, url_prefix='/profile')

@bp.route('/ai-settings', methods=['GET'])
@login_required
def ai_settings():
    """Displays the user's AI profiles and the form to create new ones."""
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    profiles = []
    providers = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        # Fetch user's existing profiles
        cursor.execute(
            """
            SELECT up.id, up.profile_name, p.provider_name, up.temperature, up.max_output_tokens
            FROM user_ai_profiles up
            JOIN ai_providers p ON up.provider_id = p.id
            WHERE up.user_id = %s ORDER BY up.profile_name;
            """,
            (current_user.id,)
        )
        for row in cursor.fetchall():
            profiles.append({
                "id": row[0], "name": row[1], "provider": row[2],
                "temp": row[3], "tokens": row[4]
            })

        # Fetch available AI providers for the dropdown
        cursor.execute("SELECT id, provider_name FROM ai_providers WHERE is_active = true ORDER BY provider_name;")
        for row in cursor.fetchall():
            providers.append({"id": row[0], "name": row[1]})

    except psycopg2.Error as e:
        flash("Database error while loading AI settings.", "danger")
        current_app.logger.error(f"DB error on AI settings page: {e}")
    finally:
        if conn: conn.close()
    
    return render_template('profile/ai_settings.html', profiles=profiles, providers=providers)

@bp.route('/ai-settings/create', methods=['POST'])
@login_required
def create_ai_profile():
    """Handles the creation of a new AI profile."""
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        profile_name = request.form.get('profile_name')
        provider_id = request.form.get('provider_id', type=int)
        temperature = request.form.get('temperature', 0.7, type=float)
        max_tokens = request.form.get('max_tokens', 2048, type=int)
        user_api_key = request.form.get('user_api_key', '')
        proxy_username = request.form.get('proxy_username', '')

        if not profile_name or not provider_id:
            flash("Profile Name and AI Provider are required.", "danger")
            return redirect(url_for('profile.ai_settings'))

        encrypted_key = None
        if user_api_key:
            cursor.execute("SELECT pgp_sym_encrypt(%s, get_encryption_key());", (user_api_key,))
            encrypted_key = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO user_ai_profiles (
                user_id, profile_name, provider_id, temperature, max_output_tokens,
                encrypted_user_api_key, proxy_username
            ) VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            (current_user.id, profile_name, provider_id, temperature, max_tokens, encrypted_key, proxy_username)
        )
        conn.commit()
        flash(f"AI Profile '{profile_name}' created successfully.", "success")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error: {e}", "danger")
        current_app.logger.error(f"DB error creating AI profile: {e}")
    finally:
        if conn: conn.close()

    return redirect(url_for('profile.ai_settings'))

@bp.route('/ai-settings/get/<int:profile_id>', methods=['GET'])
@login_required
def get_ai_profile(profile_id):
    """Fetches data for a single AI profile to populate the edit modal."""
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, profile_name, provider_id, temperature, max_output_tokens, proxy_username
            FROM user_ai_profiles
            WHERE id = %s AND user_id = %s;
            """,
            (profile_id, current_user.id)
        )
        profile = cursor.fetchone()
        if profile:
            return jsonify({
                "id": profile[0], "profile_name": profile[1], "provider_id": profile[2],
                "temperature": float(profile[3]), "max_output_tokens": profile[4],
                "proxy_username": profile[5] or ""
            })
        return jsonify({"error": "Profile not found"}), 404
    except psycopg2.Error as e:
        return jsonify({"error": "Database error"}), 500
    finally:
        if conn: conn.close()

@bp.route('/ai-settings/edit/<int:profile_id>', methods=['POST'])
@login_required
def edit_ai_profile(profile_id):
    """Handles updates to an existing AI profile."""
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        profile_name = request.form.get('profile_name')
        provider_id = request.form.get('provider_id', type=int)
        temperature = request.form.get('temperature', type=float)
        max_tokens = request.form.get('max_tokens', type=int)
        user_api_key = request.form.get('user_api_key', '')
        proxy_username = request.form.get('proxy_username', '')

        if not profile_name or not provider_id:
            flash("Profile Name and AI Provider are required.", "danger")
            return redirect(url_for('profile.ai_settings'))

        # Build the update query dynamically
        query_parts = ["UPDATE user_ai_profiles SET profile_name = %s, provider_id = %s, temperature = %s, max_output_tokens = %s, proxy_username = %s"]
        params = [profile_name, provider_id, temperature, max_tokens, proxy_username]

        if user_api_key:
            cursor.execute("SELECT pgp_sym_encrypt(%s, get_encryption_key());", (user_api_key,))
            encrypted_key = cursor.fetchone()[0]
            query_parts.append(", encrypted_user_api_key = %s")
            params.append(encrypted_key)

        query_parts.append("WHERE id = %s AND user_id = %s;")
        params.extend([profile_id, current_user.id])

        cursor.execute(" ".join(query_parts), tuple(params))
        conn.commit()
        flash(f"AI Profile '{profile_name}' updated successfully.", "success")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn: conn.close()
        
    return redirect(url_for('profile.ai_settings'))

@bp.route('/ai-settings/delete/<int:profile_id>', methods=['POST'])
@login_required
def delete_ai_profile(profile_id):
    """Deletes an AI profile owned by the current user."""
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM user_ai_profiles WHERE id = %s AND user_id = %s;",
            (profile_id, current_user.id)
        )
        conn.commit()
        if cursor.rowcount > 0:
            flash("AI Profile deleted successfully.", "success")
        else:
            flash("Profile not found or you do not have permission to delete it.", "danger")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash("Database error while deleting profile.", "danger")
    finally:
        if conn: conn.close()
    
    return redirect(url_for('profile.ai_settings'))

# Placeholder for Report History Page
@bp.route('/report-history')
@login_required
def report_history():
    flash("Report History page is under construction.", "info")
    return "Report History Page (to be implemented)"
