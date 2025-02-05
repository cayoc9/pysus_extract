from typing import List, Dict, Any
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy.orm import Session
from datetime import datetime
import os

class UnifiedDataService:
    VALID_GROUPS = {
        'SIH': ['RD', 'RJ', 'ER', 'SP'],
        'SIA': ['PA']
    }
    
    COLUMN_MAPPINGS = {
        'RD': ['UF_ZI', 'ANO_CMPT', 'MES_CMPT', 'N_AIH', 'CNES'],
        'SP': ['SP_UF', 'SP_AA', 'SP_MM', 'SP_CNES', 'SP_NAIH', 'SP_PROCREA'],
        'PA': ['PA_CODUNI', 'PA_DOCORIG', 'PA_PROC_ID']
    }

    def __init__(self, db: Session):
        self.db = db
        self.base_path = "parquet_files"
        
    def validate_group_columns(self, grupo: str, colunas: List[str]) -> bool:
        if grupo not in self.COLUMN_MAPPINGS:
            return False
        return all(col in self.COLUMN_MAPPINGS[grupo] for col in colunas)

    async def process_data(self, params: Dict[str, Any]) -> pd.DataFrame:
        if params['base'] not in self.VALID_GROUPS or params['grupo'] not in self.VALID_GROUPS[params['base']]:
            raise ValueError("Base ou grupo inválido")
            
        files = self._get_parquet_files(
            base=params['base'],
            grupo=params['grupo'],
            estados=params['estados'],
            comp_inicio=params['competencia_inicio'],
            comp_fim=params['competencia_fim']
        )
        
        dfs = []
        for file in files:
            df = pq.read_table(file).to_pandas()
            if params['colunas']:
                df = df[params['colunas']]
            dfs.append(df)
            
        return pd.concat(dfs) if dfs else pd.DataFrame()