"""
Lead Generation & Enrichment Agent.

Ejecuta el pipeline completo:
  1. Descubre agencias de marketing via Apollo.io
  2. Enriquece con emails y datos de contacto
  3. Analiza con IA (Gemini/OpenAI)
  4. Envía reporte diario por email (Gmail)
  5. Outreach personalizado via GMass (si está configurado)

Métricas reportadas:
  - discovered:       leads nuevos encontrados
  - enriched:         leads enriquecidos via web scraping
  - with_email:       leads con email encontrado via Apollo
  - ai_analyzed:      leads analizados con IA
  - sent:             leads incluidos en el reporte diario
  - outreach_sent:    emails de outreach enviados
  - duration_seconds: duración total del pipeline
  - errors_count:     errores durante la ejecución
"""

import sys
import os
from typing import Any

# Agregar el directorio del agente al path para que 'tools' sea importable
sys.path.insert(0, os.path.dirname(__file__))

from agents.base_agent import BaseAgent


class LeadGenerationAgent(BaseAgent):
    name = "lead_generation"

    def run(self) -> dict[str, Any]:
        from tools.run_pipeline import main
        stats = main() or {}
        return {
            "discovered":       stats.get("discovered", 0),
            "enriched":         stats.get("enriched", 0),
            "with_email":       stats.get("with_email", 0),
            "ai_analyzed":      stats.get("ai_analyzed", 0),
            "items_processed":  stats.get("sent", 0),        # métrica principal del dashboard
            "outreach_sent":    stats.get("outreach_sent", 0),
            "duration_seconds": stats.get("duration_seconds", 0),
            "errors_count":     len(stats.get("errors", [])),
        }
