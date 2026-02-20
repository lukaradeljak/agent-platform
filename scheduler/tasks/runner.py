"""
Task genérica de Celery — ejecuta cualquier agente registrado por nombre.

Esta es la única task que necesita existir. Todos los agentes usan esta misma task;
la diferencia está en el argumento `agent_name`.
"""

from celery import shared_task


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=60,  # 60 segundos entre reintentos
    name="scheduler.tasks.runner.run_agent",
)
def run_agent(self, agent_name: str) -> dict:
    """
    Instancia y ejecuta un agente por nombre.

    Args:
        agent_name: Clave en AGENT_REGISTRY (e.g. "example_agent")

    Returns:
        dict con resultado de la ejecución.
    """
    # Importación tardía — evita circular imports y permite que el Dockerfile
    # del scheduler tenga acceso al directorio agents/
    from agents.registry import AGENT_REGISTRY

    agent_class = AGENT_REGISTRY.get(agent_name)
    if not agent_class:
        registered = list(AGENT_REGISTRY.keys())
        raise ValueError(f"Agente desconocido: '{agent_name}'. Registrados: {registered}")

    agent = agent_class()
    try:
        agent.execute()
        return {"status": "success", "agent": agent_name}
    except Exception as exc:
        raise self.retry(exc=exc)
