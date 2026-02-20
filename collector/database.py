"""Configuración de la conexión async a PostgreSQL."""

import os
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

DATABASE_URL = os.environ["DATABASE_URL"]  # Falla explícitamente si no está configurada

engine = create_async_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    echo=False,  # Cambiar a True para debug SQL
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency de FastAPI — inyecta una sesión DB en cada request."""
    async with AsyncSessionLocal() as session:
        yield session
