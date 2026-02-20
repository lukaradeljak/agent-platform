"""
BaseAgent — Clase base que todos los agentes deben heredar.

Uso:
    class MiAgente(BaseAgent):
        name = "mi_agente"

        def run(self) -> dict[str, Any]:
            # Lógica del agente
            return {"items": 42, "errores": 0}

El método execute() es llamado por la tarea Celery. Ejecuta run() y
automáticamente hace POST de las métricas al servicio collector.
Los agentes nunca necesitan llamar al collector directamente.
"""

import os
import httpx
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

COLLECTOR_URL = os.getenv("COLLECTOR_URL", "http://collector:8000")


class BaseAgent(ABC):
    name: str  # Debe ser definido en cada subclase. Debe ser único globalmente.

    @abstractmethod
    def run(self) -> dict[str, Any]:
        """
        Ejecutar la lógica del agente.

        Returns:
            dict plano con métricas. Keys → metric_name, values → metric_value (numérico o str).

        Example:
            return {
                "pages_scraped": 100,
                "errors": 2,
                "status": "ok",
            }
        """
        ...

    def execute(self) -> None:
        """
        Llamado por la tarea Celery run_agent().
        Ejecuta run() y hace POST de las métricas al collector automáticamente.
        Relanza excepciones para que Celery pueda registrar el fallo y hacer retry.
        """
        if not hasattr(self, "name") or not self.name:
            raise ValueError(f"{self.__class__.__name__} debe definir el atributo 'name'")

        started_at = datetime.now(timezone.utc)
        error: str | None = None
        metrics: dict[str, Any] = {}

        try:
            metrics = self.run()
        except Exception as exc:
            error = str(exc)
            raise
        finally:
            self._push_metrics(
                metrics=metrics,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                error=error,
            )

    def _push_metrics(
        self,
        metrics: dict[str, Any],
        started_at: datetime,
        finished_at: datetime,
        error: str | None,
    ) -> None:
        payload = {
            "agent_name": self.name,
            "metrics": metrics,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "error": error,
        }
        try:
            with httpx.Client(timeout=10) as client:
                resp = client.post(f"{COLLECTOR_URL}/metrics", json=payload)
                resp.raise_for_status()
        except Exception as exc:
            # Log pero no relanzar — no queremos que un fallo del collector
            # oculte el error original del agente ni bloquee la ejecución.
            print(f"[{self.name}] WARNING: No se pudieron enviar métricas al collector: {exc}")
