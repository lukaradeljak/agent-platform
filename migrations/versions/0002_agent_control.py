"""Add agent_control table for pause/resume control.

Revision ID: 0002
Revises: 0001
Create Date: 2026-02-24
"""

from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_control",
        sa.Column("agent_name", sa.String(100), primary_key=True),
        sa.Column("paused", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("agent_control")
