# alembic/versions/xxxxxxxxxxxx_renomear_sih_si_sp_para_sih_servicos_profissionais.py

from alembic import op
import sqlalchemy as sa

# Revisão identifiers
revision = 'xxxxxxxxxxxx'
down_revision = '7f840b9e0a85' 
branch_labels = None
depends_on = None

def upgrade():
    # Renomear a tabela
    op.execute("""
        ALTER TABLE "SIH_SI_SP" RENAME TO "SIH_Serviços_Profissionais";
    """)
    
    # Renomear partições, se existirem
    estados = ['SP']  # Adicione outros estados se necessário
    for estado in estados:
        old_partition = f"SIH_SI_SP_{estado}"
        new_partition = f"SIH_Serviços_Profissionais_{estado}"
        op.execute(f"""
            ALTER TABLE "{old_partition}" RENAME TO "{new_partition}";
        """)

def downgrade():
    # Renomear a tabela de volta
    op.execute("""
        ALTER TABLE "SIH_Serviços_Profissionais" RENAME TO "SIH_SI_SP";
    """)
    
    # Renomear partições de volta, se existirem
    estados = ['SP']  # Adicione outros estados se necessário
    for estado in estados:
        old_partition = f"SIH_Serviços_Profissionais_{estado}"
        new_partition = f"SIH_SI_SP_{estado}"
        op.execute(f"""
            ALTER TABLE "{old_partition}" RENAME TO "{new_partition}";
        """)
