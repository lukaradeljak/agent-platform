"""
Entrypoint para ejecución de agentes via subprocess.

Llamado por runner.py:
    subprocess.run(["python", "/app/scheduler/run_agent.py", "lead_enrichment"])

Cada invocación es un proceso Python nuevo — no hay caché de módulos.
Cambios en disco se aplican automáticamente en la próxima ejecución.
"""

import importlib
import sys


def main():
    if len(sys.argv) < 2:
        print("Usage: python run_agent.py <agent_name>")
        sys.exit(1)

    agent_name = sys.argv[1]

    # Asegurar que /app está en PYTHONPATH
    if "/app" not in sys.path:
        sys.path.insert(0, "/app")

    # Importar el módulo del agente dinámicamente
    module = importlib.import_module(f"agents.{agent_name}.agent")

    # Buscar la subclase de BaseAgent
    from agents.base_agent import BaseAgent

    agent_class = None
    for obj in vars(module).values():
        if isinstance(obj, type) and issubclass(obj, BaseAgent) and obj is not BaseAgent:
            agent_class = obj
            break

    if not agent_class:
        print(f"ERROR: No BaseAgent subclass found in agents.{agent_name}.agent")
        sys.exit(1)

    agent = agent_class()
    agent.execute()


if __name__ == "__main__":
    main()
