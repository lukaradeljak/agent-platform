from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from collector.database import get_db

router = APIRouter()


@router.get("/health")
async def health(db: AsyncSession = Depends(get_db)):
    """Health check — verifica conectividad con la DB."""
    await db.execute(text("SELECT 1"))
    return {"status": "ok"}
