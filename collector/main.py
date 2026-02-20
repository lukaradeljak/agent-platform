"""FastAPI — Servicio collector de métricas de agentes."""

from fastapi import FastAPI

from collector.routers import health, metrics

app = FastAPI(
    title="Agent Metrics Collector",
    description="Recibe y almacena métricas de ejecución de agentes Python.",
    version="1.0.0",
)

app.include_router(health.router)
app.include_router(metrics.router)
