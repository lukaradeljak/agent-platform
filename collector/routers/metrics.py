"""
Endpoints del API de métricas.

POST /metrics  — Recibe métricas de un agente y las guarda en la DB.
GET  /metrics  — Consulta runs recientes (útil para el web app y debugging).
"""

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from collector.database import get_db
from collector.models.db import AgentMetric, AgentRun
from collector.models.metric import AgentRunSummary, MetricsPushRequest, MetricsPushResponse

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.post("", response_model=MetricsPushResponse, status_code=201)
async def push_metrics(
    payload: MetricsPushRequest,
    db: AsyncSession = Depends(get_db),
) -> MetricsPushResponse:
    """
    Recibe las métricas de una ejecución de agente.
    Llamado automáticamente por BaseAgent._push_metrics().
    """
    status = "failed" if payload.error else "success"

    run = AgentRun(
        agent_name=payload.agent_name,
        started_at=payload.started_at,
        finished_at=payload.finished_at,
        status=status,
        error_message=payload.error,
    )
    db.add(run)
    await db.flush()  # Obtener run.id antes de insertar métricas

    for key, value in payload.metrics.items():
        metric = AgentMetric(
            run_id=run.id,
            agent_name=payload.agent_name,
            metric_name=key,
            metric_value=_to_numeric(value),
            metric_text=str(value) if not _is_numeric(value) else None,
        )
        db.add(metric)

    await db.commit()

    return MetricsPushResponse(
        run_id=run.id,
        agent_name=payload.agent_name,
        status=status,
    )


@router.get("", response_model=list[AgentRunSummary])
async def get_metrics(
    agent_name: str | None = Query(None, description="Filtrar por nombre de agente"),
    started_after: datetime | None = Query(None, description="Solo runs con started_at posterior a esta fecha (ISO 8601)"),
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
) -> list[AgentRunSummary]:
    """
    Retorna runs recientes con sus métricas.
    Útil para el web app, sync con Supabase y debugging manual.
    """
    stmt = (
        select(AgentRun)
        .options(selectinload(AgentRun.metrics))
        .order_by(AgentRun.started_at.asc())
        .limit(limit)
    )
    if agent_name:
        stmt = stmt.where(AgentRun.agent_name == agent_name)
    if started_after:
        stmt = stmt.where(AgentRun.started_at > started_after)

    result = await db.execute(stmt)
    runs = result.scalars().all()

    return [_run_to_summary(run) for run in runs]


def _run_to_summary(run: AgentRun) -> AgentRunSummary:
    metrics: dict[str, Any] = {}
    for m in run.metrics:
        if m.metric_value is not None:
            metrics[m.metric_name] = float(m.metric_value)
        else:
            metrics[m.metric_name] = m.metric_text

    return AgentRunSummary(
        run_id=run.id,
        agent_name=run.agent_name,
        started_at=run.started_at,
        finished_at=run.finished_at,
        status=run.status,
        error_message=run.error_message,
        metrics=metrics,
    )


def _is_numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _to_numeric(value: Any):
    if _is_numeric(value):
        return value
    return None
