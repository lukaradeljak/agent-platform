"""
Registro de agentes — auto-discovery.

NO necesitas editar este archivo para agregar un agente nuevo.
Solo crea agents/<nombre>/agent.py con una clase que herede BaseAgent y defina `name` y `schedule`.
Este módulo la descubrirá automáticamente al iniciar.
"""

import importlib
from pathlib import Path

from agents.base_agent import BaseAgent


def _discover_agents() -> dict[str, type]:
    registry: dict[str, type] = {}
    agents_dir = Path(__file__).parent

    for agent_dir in sorted(agents_dir.iterdir()):
        if not agent_dir.is_dir() or agent_dir.name.startswith("_"):
            continue
        if not (agent_dir / "agent.py").exists():
            continue
        try:
            module = importlib.import_module(f"agents.{agent_dir.name}.agent")
            for cls in vars(module).values():
                if (
                    isinstance(cls, type)
                    and issubclass(cls, BaseAgent)
                    and cls is not BaseAgent
                    and getattr(cls, "name", None)
                ):
                    registry[cls.name] = cls
        except Exception as e:
            print(f"[registry] WARNING: no se pudo cargar agente '{agent_dir.name}': {e}")

    return registry


AGENT_REGISTRY: dict[str, type] = _discover_agents()
