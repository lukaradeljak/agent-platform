"""
Endpoints de control de agentes.

GET  /agents/{name}/control  → estado actual (sin auth — solo interno desde el worker)
POST /agents/{name}/pause    → pausar agente (requiere Bearer CRON_SECRET)
POST /agents/{name}/resume   → reanudar agente (requiere Bearer CRON_SECRET)
POST /agents/{name}/execute  → forzar ejecución inmediata (requiere Bearer CRON_SECRET)
"""

import os

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from collector.database import get_db
from collector.models.db import AgentControl

router = APIRouter(prefix="/agents", tags=["agents"])


def _verify_secret(authorization: str = Header(default="")) -> None:
    secret = os.environ.get("CRON_SECRET", "").strip()
    if not secret:
        return  # sin secret configurado → acceso libre (dev)
    expected = f"Bearer {secret}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


@router.get("/{agent_name}/control")
async def get_control(agent_name: str, db: AsyncSession = Depends(get_db)) -> dict:
    """Devuelve el estado de pausa del agente. Sin auth — solo uso interno."""
    result = await db.execute(
        select(AgentControl).where(AgentControl.agent_name == agent_name)
    )
    control = result.scalar_one_or_none()
    return {"agent_name": agent_name, "paused": control.paused if control else False}


@router.post("/{agent_name}/pause")
async def pause_agent(
    agent_name: str,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(_verify_secret),
) -> dict:
    """Pausa la ejecución programada del agente."""
    await _set_paused(db, agent_name, paused=True)
    return {"agent_name": agent_name, "paused": True}


@router.post("/{agent_name}/resume")
async def resume_agent(
    agent_name: str,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(_verify_secret),
) -> dict:
    """Reanuda la ejecución programada del agente."""
    await _set_paused(db, agent_name, paused=False)
    return {"agent_name": agent_name, "paused": False}


@router.post("/{agent_name}/execute")
async def execute_agent(
    agent_name: str,
    _: None = Depends(_verify_secret),
) -> dict:
    """Dispara una ejecución inmediata del agente fuera del schedule."""
    redis_url = os.environ.get("REDIS_URL", "").strip()
    if not redis_url:
        raise HTTPException(status_code=503, detail="REDIS_URL no configurada")

    try:
        from celery import Celery
        celery_app = Celery(broker=redis_url)
        celery_app.send_task(
            "scheduler.tasks.runner.run_agent",
            args=[agent_name],
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error al encolar tarea: {exc}")

    return {"agent_name": agent_name, "queued": True}


async def _set_paused(db: AsyncSession, agent_name: str, paused: bool) -> None:
    result = await db.execute(
        select(AgentControl).where(AgentControl.agent_name == agent_name)
    )
    control = result.scalar_one_or_none()
    if control:
        control.paused = paused
    else:
        db.add(AgentControl(agent_name=agent_name, paused=paused))
    await db.commit()
