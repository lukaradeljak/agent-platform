"""
Aplicación Celery — define el broker, el backend y el beat_schedule.

El beat_schedule se construye automáticamente leyendo el atributo `schedule`
de cada clase en AGENT_REGISTRY. No necesitas editar este archivo para agendar
un agente nuevo: basta con definir `schedule` en la clase del agente.
"""

import os

from celery import Celery
from celery.schedules import crontab

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

app = Celery(
    "agent_platform",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["scheduler.tasks.runner", "scheduler.tasks.sync"],
)

app.conf.timezone = "UTC"
app.conf.task_serializer = "json"
app.conf.result_serializer = "json"
app.conf.accept_content = ["json"]
app.conf.task_default_queue = "default"


def _build_beat_schedule() -> dict:
    """
    Construye el beat_schedule leyendo el atributo `schedule` de cada agente registrado.
    Para agregar un agente al schedule, define `schedule` en su clase (agents/<nombre>/agent.py).

    Ejemplos de schedule:
        crontab()               → cada minuto
        crontab(minute=0)       → cada hora en punto
        crontab(minute="*/15")  → cada 15 minutos
        crontab(hour=6)         → todos los días a las 06:00 UTC
        60.0                    → cada 60 segundos
    """
    from agents.registry import AGENT_REGISTRY

    schedule: dict = {}

    for agent_name, agent_class in AGENT_REGISTRY.items():
        agent_schedule = getattr(agent_class, "schedule", None)
        if agent_schedule is None:
            continue
        schedule[f"run-{agent_name}"] = {
            "task": "scheduler.tasks.runner.run_agent",
            "schedule": agent_schedule,
            "args": (agent_name,),
        }

    # Sync de métricas → Supabase (para el dashboard del cliente)
    schedule["sync-platform-to-supabase"] = {
        "task": "scheduler.tasks.sync.sync_platform_to_supabase",
        "schedule": crontab(minute="*/5"),  # cada 5 minutos
    }

    return schedule


app.conf.beat_schedule = _build_beat_schedule()
