FROM apache/superset:latest

# Cambiar a usuario root para instalar paquetes
USER root

# Instalar psycopg2-binary para conexión a PostgreSQL
RUN pip install psycopg2-binary --user superset

# Asegurar que el usuario superset tenga acceso a los paquetes instalados
RUN chown -R superset:superset /app/.venv

# Volver al usuario superset
USER superset

# Copiar configuración personalizada
COPY superset_config.py /app/pythonpath/superset_config.py