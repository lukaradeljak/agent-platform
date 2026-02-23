FROM python:3.12-slim

WORKDIR /app

# Copiar e instalar deps del scheduler primero (aprovecha cache de Docker)
COPY scheduler/requirements.txt scheduler/requirements.txt
RUN pip install --no-cache-dir -r scheduler/requirements.txt

# Copiar código del proyecto
COPY scheduler/ scheduler/
COPY agents/ agents/

# Instalar deps de todos los agentes (se detectan automáticamente por requirements.txt)
RUN find /app/agents -maxdepth 2 -name "requirements.txt" \
      -exec pip install --no-cache-dir -r {} \;

# Agregar el directorio raíz al PYTHONPATH para que los imports funcionen
ENV PYTHONPATH=/app

# El comando se sobreescribe en docker-compose.yml por servicio:
# celery_beat:   celery -A scheduler.celery_app beat
# celery_worker: celery -A scheduler.celery_app worker
# flower:        celery -A scheduler.celery_app flower
CMD ["celery", "-A", "scheduler.celery_app", "worker", "--loglevel=info"]
