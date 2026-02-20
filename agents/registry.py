"""
Registro de agentes.

Agregar cada nuevo agente aquí para que el scheduler pueda encontrarlo.
La clave del dict debe coincidir exactamente con el atributo `name` de la clase.
"""

from agents.example_agent.agent import ExampleAgent

AGENT_REGISTRY: dict[str, type] = {
    "example_agent": ExampleAgent,
    # Agregar nuevos agentes aquí:
    # "mi_agente": MiAgente,
}
