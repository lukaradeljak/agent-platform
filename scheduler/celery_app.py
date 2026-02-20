"""
Aplicación Celery — define el broker, el backend y el beat_schedule.

Para agregar un agente nuevo al schedule, agrega una entrada en beat_schedule.
La task siempre apunta a "tasks.runner.run_agent" — solo cambia el nombre del agente.
"""

import os

from celery import Celery
from celery.schedules import crontab

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

app = Celery(
    "agent_platform",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["tasks.runner"],
)

app.conf.timezone = "UTC"
app.conf.task_serializer = "json"
app.conf.result_serializer = "json"
app.conf.accept_content = ["json"]

# ─── Schedule de agentes ────────────────────────────────────────────────────
# Agregar una entrada por cada agente que quieras agendar.
# La clave es un nombre descriptivo (solo para logs). "args" es el agent_name del registry.
#
# Ejemplos de schedule:
#   crontab(minute=0)           → cada hora en punto
#   crontab(minute="*/15")      → cada 15 minutos
#   crontab(hour=6, minute=0)   → todos los días a las 06:00 UTC
#   60.0                        → cada 60 segundos
#
app.conf.beat_schedule = {
    "run-example-agent-every-hour": {
        "task": "tasks.runner.run_agent",
        "schedule": crontab(minute=0),
        "args": ("example_agent",),
    },
    # Agregar más agentes aquí:
    # "run-mi-agente-cada-15min": {
    #     "task": "tasks.runner.run_agent",
    #     "schedule": crontab(minute="*/15"),
    #     "args": ("mi_agente",),
    # },
}
