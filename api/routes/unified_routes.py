from fastapi import APIRouter, Depends, HTTPException
from ..models.request_models import QueryParams
from ..services.unified_data_service import UnifiedDataService
from ..auth.jwt_handler import verify_token
from ..utils.db import get_db
from sqlalchemy.orm import Session
from typing import Dict, Any

router = APIRouter(prefix="/api/v1")

@router.get("/query")
async def query_data(
    params: QueryParams,
    token: Dict = Depends(verify_token),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    try:
        service = UnifiedDataService(db)
        df = await service.process_data(params.dict())
        
        return {
            "data": df.to_dict(orient='records'),
            "total_registros": len(df),
            "colunas": df.columns.tolist()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 