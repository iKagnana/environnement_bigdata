import os

# Secret key for signing cookies and other security-related needs
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', '3YLpq3ON/qk1iPLKRlU88FQSy1SDXCRGGS9FYYiR9cr0jcvaphgPQxcQ')

# Database configuration for metadata
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# Disable CSRF protection for API calls (for development only)
WTF_CSRF_ENABLED = False

# Cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'simple',
}

# SQL Lab settings
SQLLAB_ASYNC_TIME_LIMIT_SEC = 300
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300

# Enable proxy fix for running behind reverse proxy
ENABLE_PROXY_FIX = True

# Allow additional database engines
ADDITIONAL_MODULE_DS_MAP = {}

# Feature flags
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'SQLLAB_BACKEND_PERSISTENCE': True,
}

# Allowed database engines
ALLOWED_EXTRA_SCHEMAS = ['hive', 'system']