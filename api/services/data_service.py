import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy.orm import Session
from typing import List
import os
from datetime import datetime

class DataService:
    def __init__(self, db: Session):
        self.db = db
        self.base_path = "parquet_files"

    def _parse_competencia(self, comp_str: str) -> tuple:
        dt = datetime.strptime(comp_str, '%m/%Y')
        return dt.year, dt.month

    def _get_parquet_files(self, base: str, grupo: str, estados: List[str], 
                          comp_inicio: str, comp_fim: str) -> List[str]:
        files = []
        ano_ini, mes_ini = self._parse_competencia(comp_inicio)
        ano_fim, mes_fim = self._parse_competencia(comp_fim)

        for estado in estados:
            path = os.path.join(self.base_path, base, grupo)
            if not os.path.exists(path):
                continue
                
            pattern = f"{grupo}{estado}"
            for file in os.listdir(path):
                if file.startswith(pattern) and file.endswith('.parquet'):
                    files.append(os.path.join(path, file))
        
        return files

    async def get_data(self, base: str, grupo: str, estados: List[str],
                      colunas: List[str], competencia_inicio: str,
                      competencia_fim: str) -> pd.DataFrame:
        files = self._get_parquet_files(base, grupo, estados, 
                                      competencia_inicio, competencia_fim)
        
        if not files:
            return pd.DataFrame()

        dfs = []
        for file in files:
            try:
                df = pq.read_table(file).to_pandas()
                if not colunas:
                    colunas = df.columns.tolist()
                df = df[colunas]
                dfs.append(df)
            except Exception as e:
                print(f"Erro ao ler arquivo {file}: {e}")
                continue

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True) 