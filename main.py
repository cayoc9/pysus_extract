import logging
import os
import datetime
from typing import List, Dict, Any, Optional, Tuple
from dotenv import load_dotenv
import duckdb
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError
from datetime import datetime
import logging.handlers
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4
import psutil
import time
import json
from io import StringIO
import traceback
import threading
from queue import PriorityQueue
from asyncio import Semaphore
import gc
from fastapi.responses import JSONResponse
import csv
import numpy as np
from collections import defaultdict
import re
import logging.config
import psycopg2
import tempfile
import shutil
from sqlalchemy.pool import NullPool

# Importações das definições
from api.definitions import (
    grupos_dict, CAMPOS_CNES, GRUPOS_INFO,
    QueryParams, Settings
)

# Importando funções de utilidade do módulo utils
from api.utils import (
    log_execution, get_parquet_files, get_schema_info, create_error_columns,
    convert_datatypes, validate_data_for_postgres, apply_filters, split_csv,
    save_results, adaptive_processing, build_conversion_query, get_cnes_column,
    process_data, process_parquet_files, export_schema, validate_csv_sample,
    process_with_logging, verify_db_connection
)

# -----------------------------------------------------------------------------
# Configurações iniciais e carregamento do ambiente
# -----------------------------------------------------------------------------
load_dotenv(override=True)
os.environ.update(os.environ)

# Configurar logging (arquivo + console)
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "app.log")),
        logging.StreamHandler()
    ]
)

# -----------------------------------------------------------------------------
# Endpoints FastAPI
# -----------------------------------------------------------------------------
app = FastAPI(title="DataSUS API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Variáveis globais para gerenciamento de tarefas
# -----------------------------------------------------------------------------
task_lock = threading.Lock()
task_queue = PriorityQueue()
async_jobs = {}

# -----------------------------------------------------------------------------
# Endpoint principal para consulta
# -----------------------------------------------------------------------------
@app.post("/query", tags=["Main"])
async def query_data(params: QueryParams, background_tasks: BackgroundTasks):
    logging.info(f"Nova requisição: {params.model_dump()}")
    files = get_parquet_files(params.base, params.grupo, params.competencia_inicio, params.competencia_fim)
    if not files:
        logging.error("Nenhum arquivo encontrado")
        raise HTTPException(status_code=404, detail="Nenhum arquivo encontrado")
    start_time = datetime.now().isoformat()
    logging.info(f"Iniciando processamento de {len(files)} arquivos às {start_time}")
    background_tasks.add_task(lambda: process_with_logging(files, params, start_time))
    return {
        "status": "processing",
        "start_time": start_time,
        "total_files": len(files),
        "message": "Processamento iniciado. Verifique os logs para detalhes."
    }

@app.post("/query/async", tags=["Async Operations"])
async def async_query(params: QueryParams, background_tasks: BackgroundTasks) -> Dict[str, str]:
    async with Semaphore(1):
        job_id = str(uuid4())
        async_jobs[job_id] = {
            "status": "processing",
            "start_time": datetime.now().isoformat(),
            "progress": 0
        }
        def task_processor():
            try:
                files = get_parquet_files(params.base, params.grupo, params.competencia_inicio, params.competencia_fim)
                temp_table = process_parquet_files(files, params)
                table_name = params.table_name if params.table_name else GRUPOS_INFO[params.grupo]['tabela']
                save_results(temp_table, table_name, params, engine)
                async_jobs[job_id].update({
                    "status": "completed", 
                    "end_time": datetime.now().isoformat(),
                    "table_name": table_name
                })
            except Exception as e:
                async_jobs[job_id].update({
                    "status": "error",
                    "error": str(e),
                    "end_time": datetime.now().isoformat()
                })
        background_tasks.add_task(task_processor)
        return {"job_id": job_id, "status_url": f"/query/jobs/{job_id}"}

@app.post("/analise_procedimento/async", tags=["Async Operations"])
async def async_analysis(params: QueryParams, background_tasks: BackgroundTasks) -> Dict[str, str]:
    async with Semaphore(1):
        job_id = str(uuid4())
        async_jobs[job_id] = {
            "status": "processing",
            "start_time": datetime.now().isoformat(),
            "progress": 0
        }
        def task_processor():
            try:
                # Importando aqui para evitar importação circular
                from analise_procedimentos import gerar_relatorio_procedimentos
                
                # Extração do ano a partir da competência
                anos = []
                try:
                    inicio = datetime.strptime(params.competencia_inicio, '%m/%Y')
                    fim = datetime.strptime(params.competencia_fim, '%m/%Y')
                    # Adiciona todos os anos entre início e fim
                    for ano in range(inicio.year, fim.year + 1):
                        anos.append(ano)
                except Exception as e:
                    logging.error(f"Erro ao processar datas: {e}")
                    anos = [datetime.now().year]  # Usa ano atual como fallback
                
                # Definir diretório de saída
                output_dir = params.output_dir if hasattr(params, 'output_dir') and params.output_dir else "relatorios"
                
                # Atualizar progresso
                async_jobs[job_id]["progress"] = 10
                
                # Gerar relatório
                gerar_relatorio_procedimentos(
                    base=params.base,
                    grupo=params.grupo,
                    cnes_list=params.cnes_list,
                    competencia_inicio=params.competencia_inicio,
                    competencia_fim=params.competencia_fim,
                    anos=anos,
                    output_dir=output_dir
                )
                
                async_jobs[job_id].update({
                    "status": "completed", 
                    "end_time": datetime.now().isoformat(),
                    "output_dir": output_dir,
                    "anos_processados": anos
                })
            except Exception as e:
                logging.error(f"Erro na análise de procedimentos: {str(e)}", exc_info=True)
                async_jobs[job_id].update({
                    "status": "error",
                    "error": str(e),
                    "end_time": datetime.now().isoformat()
                })
        
        background_tasks.add_task(task_processor)
        return {"job_id": job_id, "status_url": f"/query/jobs/{job_id}"}

@app.get("/query/jobs/{job_id}", tags=["Async Operations"])

async def get_job_status(job_id: str):
    if job_id not in async_jobs:
        raise HTTPException(status_code=404, detail=f"Job {job_id} não encontrado")
    return async_jobs[job_id]

# -----------------------------------------------------------------------------
# Middleware de Monitoramento de Performance
# -----------------------------------------------------------------------------
@app.middleware("http")
async def resource_guard(request: Request, call_next):
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent()
    if mem.percent > 75 or cpu > 85:
        return JSONResponse(
            status_code=503,
            content={
                "error": "high_load",
                "message": "Servidor sob alta carga",
                "metrics": {
                    "memory_used": f"{mem.used/1024**3:.1f}GB",
                    "cpu_usage": f"{cpu}%"
                }
            }
        )
    try:
        response = await call_next(request)
        return response
    except MemoryError:
        return JSONResponse(
            status_code=500,
            content={"error": "out_of_memory", "message": "Memória insuficiente"}
        )

# -----------------------------------------------------------------------------
# Execução local com Uvicorn
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    try:
        settings = Settings()
        print("Configurações carregadas com sucesso!")
        print(settings.model_dump())
    except ValidationError as e:
        print("Erro nas configurações:")
        print(e.json(indent=2))
        exit(1)
    uvicorn.run(app, host="0.0.0.0", port=8000)

settings = Settings()

# Atualizar a conexão do SQLAlchemy
engine = create_engine(
    f"postgresql+psycopg2://{settings.db_user}:{quote_plus(settings.db_pass)}"
    f"@{settings.db_host}:{settings.db_port}/{settings.db_name}",
    poolclass=NullPool,
    connect_args={'options': '-c statement_timeout=15000'}
)
