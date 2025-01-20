"""Renomear tabelas SIH para nomes padronizados

Revision ID: 7f86ed43a086
Revises: 42ee8e13a777
Create Date: 2024-11-27 21:18:52.957396

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7f86ed43a086'
down_revision: Union[str, None] = '42ee8e13a777'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Renomear as tabelas
    op.rename_table('SIH_ER', 'sih_aih_rejeitada_erro')
    op.rename_table('SIH_RD', 'sih_aih_reduzida')
    op.rename_table('SIH_SI_RJ', 'sih_aih_rejeitada')
    op.rename_table('SIH_SI_SP', 'sih_servicos_profissionais')

def downgrade():
    # Reverter os nomes das tabelas
    op.rename_table('sih_aih_rejeitada_erro', 'SIH_ER')
    op.rename_table('sih_aih_reduzida', 'SIH_RD')
    op.rename_table('sih_aih_rejeitada', 'SIH_SI_RJ')
    op.rename_table('sih_servicos_profissionais', 'SIH_SI_SP')
