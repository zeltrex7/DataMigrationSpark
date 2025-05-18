"""Default configuration for the Airflow webserver"""

from flask_appbuilder.security.manager import AUTH_DB

# Web UI authentication
AUTH_TYPE = AUTH_DB
AUTH_USER_REGISTRATION = False
AUTH_USER_REGISTRATION_ROLE = "Public"

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = False

# CORS Settings
ENABLE_CORS = True
CORS_OPTIONS = {
    'origins': ['*'],
    'methods': ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    'allow_headers': ['Content-Type', 'Authorization'],
    'expose_headers': ['Content-Type', 'Authorization'],
    'supports_credentials': True,
    'max_age': 600,
}