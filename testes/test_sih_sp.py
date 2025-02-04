import pytest
from main import process_file
from resource_manager import ManagedThreadPool

TEST_PARAMS = {
    "base": "SIH",
    "grupo": "SP",
    "cnes_list": [
        "2292386", "2269899", "2273357", "7065515", "0012521",
        "0012769", "0679550", "2269678", "2270234", "2270617",
        "2270803", "2273209", "2273411", "2291304", "2295067",
        "2298031", "2298724", "2696932", "3784916", "5478898",
        "6518893", "6586767", "7011857", "7185081", "7267975",
        "7516800", "7529384", "9074457", "3343715"
    ],
    "campos_agrupamento": [
        "SP_GESTOR", "SP_UF", "SP_AA", "SP_MM", "SP_NAIH",
        "SP_PROCREA", "SP_DTINTER", "SP_DTSAIDA", "SP_NUM_PR",
        "SP_TIPO", "SP_ATOPROF", "SP_TP_ATO", "SP_QTD_ATO",
        "SP_PTSP", "SP_VALATO", "SP_M_HOSP", "SP_M_PAC",
        "SP_DES_HOS", "SP_DES_PAC", "SP_COMPLEX", "SP_FINANC",
        "IN_TP_VAL", "SERV_CLA", "SP_CIDPRI", "SP_CIDSEC",
        "SP_QT_PROC", "SP_U_AIH"
    ],
    "competencia_inicio": "01/2024",
    "competencia_fim": "01/2024",
    "table_name": "sih_servicos_profissionais"
}

def test_sp_processing():
    files = [
        "/root/pysus_sih/parquet_files/SIH/SP/SPDF2401.parquet/000db8fca09740d79ea006dd03005b59-0.parquet",
        # ... outros arquivos ...
    ]
    
    with ManagedThreadPool() as pool:
        futures = [pool.submit(process_file, f, TEST_PARAMS) for f in files]
        results = [f.result() for f in futures]
    
    assert len(results) == len(files)
    assert all('error' not in r for r in results) 