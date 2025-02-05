from fastapi import APIRouter, Depends, HTTPException
from ..models.request_models import QueryParams
from ..services.data_service import DataService
from ..auth.jwt_handler import verify_token
from ..utils.db import get_db
from sqlalchemy.orm import Session
from typing import Dict, Any
import pandas as pd

router = APIRouter()

@router.get("/query")
async def query_data(
    params: QueryParams,
    token: Dict = Depends(verify_token),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    try:
        service = DataService(db)
        df = await service.get_data(
            base=params.base,
            grupo=params.grupo,
            estados=params.estados,
            colunas=params.colunas,
            competencia_inicio=params.competencia_inicio,
            competencia_fim=params.competencia_fim
        )
        if df.empty:
            raise HTTPException(status_code=404, detail="Dados não encontrados")
            
        return {
            "data": df.to_dict(orient='records'),
            "total_registros": len(df),
            "colunas": df.columns.tolist()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 