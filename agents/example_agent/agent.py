"""
Agente de ejemplo — úsalo como plantilla para crear nuevos agentes.

Copia esta carpeta completa, renómbrala, y modifica:
1. El atributo `name` (debe ser único)
2. El método `run()` con la lógica de tu agente
3. requirements.txt con las dependencias necesarias
"""

import random
import time
from typing import Any

from agents.base_agent import BaseAgent


class ExampleAgent(BaseAgent):
    name = "example_agent"

    def run(self) -> dict[str, Any]:
        """
        Ejemplo de agente que simula procesamiento y retorna métricas.
        Reemplaza este método con la lógica real de tu agente.
        """
        start = time.time()

        # ── Tu lógica aquí ──────────────────────────────────────────
        # Ejemplo: llamar a una API, procesar datos, ejecutar un LangGraph graph, etc.
        items_processed = random.randint(10, 100)
        errors = random.randint(0, 3)
        time.sleep(0.1)  # Simula trabajo
        # ────────────────────────────────────────────────────────────

        duration = round(time.time() - start, 3)

        return {
            "items_processed": items_processed,
            "errors": errors,
            "duration_seconds": duration,
            "status": "ok" if errors == 0 else "partial",
        }
