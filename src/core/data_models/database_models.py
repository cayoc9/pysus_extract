from sqlalchemy import Column, Integer, String, Float, Date, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class SIH_RD(Base):
    __tablename__ = 'sih_aih_reduzida'
    # Referência ao modelo existente
    # startLine: 1
    # endLine: 120
    # file: models/sih_rd.py

class SIH_RJ(Base):
    __tablename__ = 'sih_aih_rejeitada'
    # Referência ao modelo existente
    # startLine: 1
    # endLine: 45
    # file: models/sih_rj.py

class SIH_ER(Base):
    __tablename__ = 'sih_aih_rejeitada_erro'
    # Referência ao modelo existente
    # startLine: 38
    # endLine: 57
    # file: docs/task/implementacao.md

class SIH_SP(Base):
    __tablename__ = 'sih_servicos_profissionais'
    # Referência ao modelo existente
    # startLine: 61
    # endLine: 78
    # file: docs/task/implementacao.md 