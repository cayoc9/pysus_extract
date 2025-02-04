import pytest
from main import app  # Importar app diretamente

@pytest.fixture
async def test_client():
    from fastapi.testclient import TestClient
    return TestClient(app) 

@pytest.fixture
def default_params():
    return {
        "base": "SIH",
        "grupo": "SP",
        "cnes_list": ["TESTE123"],
        "campos_agrupamento": ["CNES"],
        "competencia_inicio": "01/2024",
        "competencia_fim": "01/2024",
        "table_name": "test_table"
    } 