# models/__init__.py

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Importar todos os modelos para registrar no MetaData
from .sih_sp import SIH_SP
from .sih_rj import SIH_RJ
from .sih_er import SIH_ER
from .sih_rd import SIH_RD
