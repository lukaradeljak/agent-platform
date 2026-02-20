"""Schemas Pydantic para request/response del API de métricas."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, field_validator


class MetricsPushRequest(BaseModel):
    """
    Payload que el agente (via BaseAgent._push_metrics) envía al collector.

    El campo `metrics` es un dict libre — keys son nombres de métricas,
    values pueden ser int, float, o str.
    """

    agent_name: str
    metrics: dict[str, Any]
    started_at: datetime
    finished_at: datetime
    error: str | None = None

    @field_validator("agent_name")
    @classmethod
    def agent_name_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("agent_name no puede estar vacío")
        return v.strip()

    @field_validator("metrics")
    @classmethod
    def metrics_not_none(cls, v: dict) -> dict:
        return v or {}


class MetricsPushResponse(BaseModel):
    run_id: int
    agent_name: str
    status: str


class AgentRunSummary(BaseModel):
    """Resumen de una ejecución — para el endpoint GET /metrics."""

    run_id: int
    agent_name: str
    started_at: datetime
    finished_at: datetime | None
    status: str
    error_message: str | None
    metrics: dict[str, Any]

    model_config = {"from_attributes": True}
