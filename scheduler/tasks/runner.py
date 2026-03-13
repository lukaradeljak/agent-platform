"""
Task genérica de Celery — ejecuta cualquier agente registrado por nombre.

Lanza cada agente como subprocess para evitar caché de módulos Python.
Cambios en disco se aplican automáticamente en la próxima ejecución
sin necesidad de reiniciar containers.
"""

import logging
import os
import subprocess

import httpx
from celery import shared_task

log = logging.getLogger(__name__)

AGENT_TIMEOUT = int(os.environ.get("AGENT_TIMEOUT", "3600"))  # 1 hora default


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    name="scheduler.tasks.runner.run_agent",
)
def run_agent(self, agent_name: str) -> dict:
    """
    Lanza un agente como subprocess.

    Args:
        agent_name: Nombre del agente (e.g. "lead_enrichment")

    Returns:
        dict con resultado de la ejecución.
    """
    # ── Verificar si el agente está pausado ──────────────────────────────
    collector_url = os.environ.get("COLLECTOR_URL", "http://collector:8000")
    try:
        r = httpx.get(f"{collector_url}/agents/{agent_name}/control", timeout=5)
        if r.status_code == 200 and r.json().get("paused"):
            log.info("Agente '%s' pausado — ejecución omitida.", agent_name)
            return {"status": "skipped", "reason": "paused", "agent": agent_name}
    except Exception as exc:
        log.warning("No se pudo verificar estado de pausa para '%s': %s", agent_name, exc)

    # ── Ejecutar via subprocess ──────────────────────────────────────────
    # Cada invocación es un proceso Python nuevo → sin caché de sys.modules.
    # Cambios en disco (git pull, scp) se aplican inmediatamente.
    try:
        result = subprocess.run(
            ["python", "/app/scheduler/run_agent.py", agent_name],
            capture_output=True,
            text=True,
            timeout=AGENT_TIMEOUT,
            env={**os.environ},
        )

        if result.stdout:
            log.info("[%s] stdout:\n%s", agent_name, result.stdout.strip())
        if result.stderr:
            log.warning("[%s] stderr:\n%s", agent_name, result.stderr.strip())

        if result.returncode != 0:
            error_msg = (
                f"Agent '{agent_name}' exited with code {result.returncode}:\n"
                f"{result.stderr[-500:] if result.stderr else '(no stderr)'}"
            )
            raise RuntimeError(error_msg)

        return {"status": "success", "agent": agent_name}

    except subprocess.TimeoutExpired:
        log.error("Agent '%s' timed out after %ss", agent_name, AGENT_TIMEOUT)
        raise self.retry(exc=RuntimeError(f"Agent '{agent_name}' timed out after {AGENT_TIMEOUT}s"))
    except RuntimeError as exc:
        log.error("Agent '%s' failed: %s", agent_name, exc)
        raise self.retry(exc=exc)
