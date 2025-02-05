# models/sih_er.py

from sqlalchemy import Column, Integer, String, Float, Date
from . import Base

class SIH_ER(Base):
    __tablename__ = 'sih_aih_rejeitada_erro'

    id = Column(Integer, primary_key=True)
    UF_ZI = Column(String(2), index=True)
    ANO_CMPT = Column(Integer, index=True)
    MES_CMPT = Column(Integer, index=True)
    CNES = Column(String(7))
    N_AIH = Column(String(13))
    MOTIVO_REJ = Column(String(100))
    ERRO_SMS = Column(String(100))
    ERRO_SES = Column(String(100))
    ERRO_CRIT = Column(String(100))
    NUM_CRIT = Column(Integer)
    VAL_APRES = Column(Float)
    VAL_GLOSA = Column(Float)
