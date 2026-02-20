FROM python:3.12-slim

WORKDIR /app

# Instalar dependencias del scheduler y Celery
COPY scheduler/requirements.txt scheduler/requirements.txt
RUN pip install --no-cache-dir -r scheduler/requirements.txt

# Instalar dependencias de cada agente
COPY agents/example_agent/requirements.txt agents/example_agent/requirements.txt
RUN pip install --no-cache-dir -r agents/example_agent/requirements.txt || true

COPY agents/lead_generation/requirements.txt agents/lead_generation/requirements.txt
RUN pip install --no-cache-dir -r agents/lead_generation/requirements.txt

COPY agents/onboarding_clients/requirements.txt agents/onboarding_clients/requirements.txt
RUN pip install --no-cache-dir -r agents/onboarding_clients/requirements.txt

# Copiar el código del proyecto
# El contexto del build es la raíz del proyecto (ver docker-compose.yml)
COPY scheduler/ scheduler/
COPY agents/ agents/

# Agregar el directorio raíz al PYTHONPATH para que los imports funcionen
ENV PYTHONPATH=/app

# El comando se sobreescribe en docker-compose.yml por servicio:
# celery_beat:   celery -A celery_app beat
# celery_worker: celery -A celery_app worker
# flower:        celery -A celery_app flower
CMD ["celery", "-A", "celery_app", "worker", "--loglevel=info"]
