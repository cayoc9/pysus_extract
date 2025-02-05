from sqlalchemy import Column, Integer, String, Date, Float
from ..base import Base

class SIHBase(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)
    uf = Column(String(2))
    ano = Column(Integer)
    mes = Column(Integer)
    cnes = Column(String(7))

class SIH_RD(SIHBase):
    __tablename__ = 'sih_aih_reduzida'
    # Campos específicos...

class SIH_SP(SIHBase):
    __tablename__ = 'sih_servicos_profissionais'
    # Campos específicos... 