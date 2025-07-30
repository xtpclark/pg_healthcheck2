# run_web_app.py
from trends_app import create_app

app = create_app()

if __name__ == '__main__':
    # This makes the app accessible on your network,
    # useful for development and testing.
    app.run(debug=True, host='0.0.0.0', port=5001)
