"""Renomear SIH_SI_SP para SIH_Serviços_Profissionais

Revision ID: 42ee8e13a777
Revises: xxxxxxxxxxxx
Create Date: 2024-11-27 08:25:48.937954

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '42ee8e13a777'
down_revision: Union[str, None] = 'xxxxxxxxxxxx'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
