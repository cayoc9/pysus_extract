from pydantic import BaseModel, validator
from typing import List, Optional
from datetime import datetime

class QueryParams(BaseModel):
    base: str
    grupo: str
    estados: List[str]
    colunas: List[str]
    competencia_inicio: str
    competencia_fim: str

    @validator('base')
    def validate_base(cls, v):
        if v not in ['SIH', 'SIA']:
            raise ValueError('Base deve ser SIH ou SIA')
        return v

    @validator('grupo')
    def validate_grupo(cls, v):
        if v not in ['RD', 'RJ', 'ER', 'SP', 'PA']:
            raise ValueError('Grupo inválido')
        return v

    @validator('estados')
    def validate_estados(cls, v):
        valid_states = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
                       'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
                       'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
        if not all(estado in valid_states for estado in v):
            raise ValueError('Estado inválido')
        return v

    @validator('competencia_inicio', 'competencia_fim')
    def validate_competencia(cls, v):
        try:
            datetime.strptime(v, '%m/%Y')
        except ValueError:
            raise ValueError('Formato de competência deve ser MM/YYYY')
        return v 