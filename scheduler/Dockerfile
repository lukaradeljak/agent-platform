FROM python:3.12-slim

WORKDIR /app

# Copiar e instalar deps del scheduler primero (aprovecha cache de Docker)
COPY scheduler/requirements.txt scheduler/requirements.txt
RUN pip install --no-cache-dir -r scheduler/requirements.txt

# Copiar código del scheduler
COPY scheduler/ scheduler/

# Solo copiar base files de agents (no el código de agentes individuales).
# El código de cada agente viene del volume mount /opt/agents:/app/agents:ro
COPY agents/__init__.py agents/__init__.py
COPY agents/base_agent.py agents/base_agent.py
COPY agents/registry.py agents/registry.py

# Deps de agentes se instalan en runtime (via deploy script de cada agente)
# para que no haga falta rebuild de imagen cuando se agrega un agente nuevo.

# Agregar el directorio raíz al PYTHONPATH para que los imports funcionen
ENV PYTHONPATH=/app

# El comando se sobreescribe en docker-compose.yml por servicio:
# celery_beat:   celery -A scheduler.celery_app beat
# celery_worker: celery -A scheduler.celery_app worker
# flower:        celery -A scheduler.celery_app flower
CMD ["celery", "-A", "scheduler.celery_app", "worker", "--loglevel=info"]
