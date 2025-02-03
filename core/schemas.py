from pydantic import BaseModel, Field

class QueryParams(BaseModel):
    base: str = Field(..., example="SIH")
    grupo: str = Field(..., example="SP")
    cnes_list: list[str] = Field(..., min_items=1)
    # Adicionar outros campos conforme necessário 