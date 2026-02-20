"""
Registro de agentes.

Agregar cada nuevo agente aquí para que el scheduler pueda encontrarlo.
La clave del dict debe coincidir exactamente con el atributo `name` de la clase.
"""

from agents.example_agent.agent import ExampleAgent
from agents.lead_generation.agent import LeadGenerationAgent

AGENT_REGISTRY: dict[str, type] = {
    "example_agent": ExampleAgent,
    "lead_generation": LeadGenerationAgent,
}
