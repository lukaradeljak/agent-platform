"""Initial schema — agent_runs, agent_metrics, agent_daily_summaries

Revision ID: 0001
Revises:
Create Date: 2026-02-19
"""

from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_runs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("agent_name", sa.String(length=100), nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="running"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_agent_runs_agent_name", "agent_runs", ["agent_name"])
    op.create_index("idx_agent_runs_started_at", "agent_runs", ["started_at"])
    op.create_index("idx_agent_runs_status", "agent_runs", ["status"])

    op.create_table(
        "agent_metrics",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.Column("agent_name", sa.String(length=100), nullable=False),
        sa.Column("metric_name", sa.String(length=200), nullable=False),
        sa.Column("metric_value", sa.Numeric(), nullable=True),
        sa.Column("metric_text", sa.Text(), nullable=True),
        sa.Column("recorded_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["agent_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_agent_metrics_run_id", "agent_metrics", ["run_id"])
    op.create_index("idx_agent_metrics_agent_name", "agent_metrics", ["agent_name"])
    op.create_index("idx_agent_metrics_metric_name", "agent_metrics", ["metric_name"])
    op.create_index("idx_agent_metrics_recorded_at", "agent_metrics", ["recorded_at"])

    op.create_table(
        "agent_daily_summaries",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("agent_name", sa.String(length=100), nullable=False),
        sa.Column("summary_date", sa.Date(), nullable=False),
        sa.Column("total_runs", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("successful_runs", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_runs", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("avg_duration_s", sa.Numeric(10, 3), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("agent_name", "summary_date", name="uq_agent_date"),
    )


def downgrade() -> None:
    op.drop_table("agent_daily_summaries")
    op.drop_table("agent_metrics")
    op.drop_table("agent_runs")
