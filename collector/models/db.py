"""Modelos ORM SQLAlchemy — fuente de verdad del schema de la DB."""

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Date,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from sqlalchemy.types import TIMESTAMP


def now_utc():
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class AgentRun(Base):
    """Un registro por ejecución de agente."""

    __tablename__ = "agent_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    agent_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="running"
    )  # running | success | failed
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    metrics: Mapped[list["AgentMetric"]] = relationship(
        "AgentMetric", back_populates="run", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_agent_runs_started_at", "started_at"),
        Index("idx_agent_runs_status", "status"),
    )


class AgentMetric(Base):
    """
    Métricas individuales (key/value) por ejecución.
    Patrón EAV — permite agregar nuevas métricas sin cambiar el schema.
    metric_value para valores numéricos, metric_text para strings.
    """

    __tablename__ = "agent_metrics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("agent_runs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    agent_name: Mapped[str] = mapped_column(
        String(100), nullable=False, index=True
    )  # denormalizado para queries rápidas sin JOIN
    metric_name: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    metric_value: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    metric_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    run: Mapped["AgentRun"] = relationship("AgentRun", back_populates="metrics")

    __table_args__ = (Index("idx_agent_metrics_recorded_at", "recorded_at"),)


class AgentDailySummary(Base):
    """Agregados diarios por agente — para performance del dashboard."""

    __tablename__ = "agent_daily_summaries"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    agent_name: Mapped[str] = mapped_column(String(100), nullable=False)
    summary_date: Mapped[datetime] = mapped_column(Date, nullable=False)
    total_runs: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    successful_runs: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_runs: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    avg_duration_s: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), nullable=True)

    __table_args__ = (UniqueConstraint("agent_name", "summary_date", name="uq_agent_date"),)
