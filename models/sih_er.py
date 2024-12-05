# models/sih_er.py

from sqlalchemy import Column, Integer, String, Date, Boolean
from . import Base

class SIH_ER(Base):
    __tablename__ = 'sih_aih_rejeitada_erro'

    id = Column(Integer, primary_key=True, autoincrement=True)
    SEQUENCIA = Column(Integer, nullable=True)
    REMESSA = Column(String(50), nullable=True)
    CNES = Column(String(10), nullable=True)
    AIH = Column(String(50), nullable=True)
    ANO = Column(Integer, nullable=True)
    MES = Column(Integer, nullable=True)
    DT_INTER = Column(Date, nullable=True)
    DT_SAIDA = Column(Date, nullable=True)
    MUN_MOV = Column(String(50), nullable=True)
    UF_ZI = Column(String(2), nullable=True)
    MUN_RES = Column(String(50), nullable=True)
    UF_RES = Column(String(2), nullable=True)
    CO_ERRO = Column(String(50), nullable=True)
