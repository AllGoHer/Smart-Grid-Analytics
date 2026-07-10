# superset/superset_config.py
SECRET_KEY = 'your-secret-key-change-this'

# Configuración de la base de datos de Superset
SQLALCHEMY_DATABASE_URI = 'postgresql://admin:admin@postgres/superset'

# Configuración de visualización
SUPERSET_WEBSERVER_PORT = 8088

# Habilitar características avanzadas
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'ALERT_REPORTS': False
}

# Configuración de caché
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300
}