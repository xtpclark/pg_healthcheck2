from flask import Flask
from flask_login import LoginManager
from .utils import load_trends_config
from .database import load_user as db_load_user

login_manager = LoginManager()
login_manager.login_view = 'auth.login'

def create_app():
    """Create and configure an instance of the Flask application."""
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'a-super-secret-key-that-should-be-changed'

    login_manager.init_app(app)

    # Register blueprints
    from . import main, auth, admin, profile # Import the new profile blueprint
    app.register_blueprint(main.bp)
    app.register_blueprint(auth.bp)
    app.register_blueprint(admin.bp)
    app.register_blueprint(profile.bp) # Register the new blueprint

    @login_manager.user_loader
    def load_user(user_id):
        config = load_trends_config()
        db_config = config.get('database')
        return db_load_user(db_config, user_id)

    return app
