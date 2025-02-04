# Conteúdo completo do resource_manager.py conforme sua solução 

import duckdb
import threading
import pandas as pd
import gc
import time
import os
import psutil
import logging
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, Future
import asyncio

def get_system_load():
    """Coleta métricas do sistema em tempo real"""
    mem = psutil.virtual_memory()
    return {
        'memory_total_gb': mem.total / (1024**3),
        'memory_used_gb': mem.used / (1024**3),
        'memory_percent': mem.percent,
        'cpu_cores': psutil.cpu_count(),
        'cpu_usage': psutil.cpu_percent(interval=1)
    }

class ManagedThreadPool:
    """Pool de threads inteligente que se adapta aos recursos do sistema"""
    def __init__(self, max_workers=None):
        self.max_workers = max_workers or self.calculate_max_workers()
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self._futures = []
        self._lock = threading.Lock()
        logging.info(f"Pool de threads inicializado com {self.max_workers} workers")

    def calculate_max_workers(self):
        """Calcula o número ideal de workers baseado nos recursos disponíveis"""
        mem_free = psutil.virtual_memory().available / (1024**3)
        cpu_free = 100 - psutil.cpu_percent(interval=0.1)
        return min(int(mem_free // 0.3), int((cpu_free * psutil.cpu_count()) // 20), 16)

    def submit(self, fn, *args, **kwargs) -> Future:
        """Submete uma tarefa ao pool com controle de throttling"""
        with self._lock:
            if self._should_throttle():
                time.sleep(0.5)
            future = self.executor.submit(fn, *args, **kwargs)
            self._futures.append(future)
            future.timeout = 30  # Segundos
            return future

    def _should_throttle(self):
        """Decide se deve reduzir o ritmo de processamento"""
        load = get_system_load()
        return (load['memory_percent'] > 85 or 
                load['cpu_usage'] > 75 or 
                len(self._futures) > self.max_workers * 2)

    def shutdown(self, timeout=30):
        """Desligamento seguro do pool de threads"""
        with self._lock:
            self.executor.shutdown(wait=False)
            for future in self._futures:
                future.cancel()
            self._force_resource_release(timeout)

    def _force_resource_release(self, timeout):
        """Liberação com verificação de progresso"""
        start = time.time()
        while time.time() - start < timeout:
            if all(f.done() for f in self._futures):
                return
            time.sleep(0.5)
        logging.error("Timeout crítico - reinicializando recursos")
        os._exit(1)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.shutdown()

    def submit_async(self, fn, *args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_in_executor(self, fn, *args, **kwargs)

class DuckDBConnection:
    """Pool de conexões thread-safe para DuckDB"""
    _lock = threading.Lock()
    _conn_pool = []
    _max_pool_size = 5

    def __init__(self):
        self.conn = None
        self.conn_id = None

    def __enter__(self):
        with self._lock:
            if self._conn_pool:
                self.conn = self._conn_pool.pop()
                self.conn_id = id(self.conn)
                logging.debug(f"Reutilizando conexão {self.conn_id}")
            else:
                self.conn = duckdb.connect()
                self.conn_id = id(self.conn)
                logging.debug(f"Nova conexão {self.conn_id} criada")
            return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._lock:
            try:
                if self.conn.transaction:
                    self.conn.execute("ROLLBACK")
                self.conn.execute("RESET ALL")
                self._conn_pool.append(self.conn)
                logging.debug(f"Conexão {self.conn_id} devolvida ao pool")
            except Exception as e:
                logging.error(f"Erro ao resetar conexão {self.conn_id}: {str(e)}")
                self.conn.close()
            finally:
                self.conn = None

    @classmethod
    def initialize_pool(cls):
        """Pré-aloca conexões no pool"""
        with cls._lock:
            while len(cls._conn_pool) < cls._max_pool_size:
                cls._conn_pool.append(duckdb.connect())
            logging.info(f"Pool DuckDB inicializado com {cls._max_pool_size} conexões")

class MemoryGuardian(threading.Thread):
    """Monitor de memória que prevê e evita OOM (Out Of Memory)"""
    def __init__(self, threshold=90):
        super().__init__(daemon=True)
        self.threshold = threshold
        self.running = True

    def run(self):
        while self.running:
            mem = psutil.virtual_memory()
            if mem.percent > self.threshold:
                self._emergency_cleanup()
            time.sleep(5)

    def _emergency_cleanup(self):
        """Ações de emergência para liberar memória"""
        mem = psutil.virtual_memory()
        logging.critical(f"Memória em uso: {mem.percent}%")
        gc.collect()
        if mem.percent > 95:
            os.kill(os.getpid(), 15)

@contextmanager
def managed_processing():
    """Contexto para processamento intensivo com limpeza garantida"""
    try:
        pd.reset_option("all")
        gc.collect()
        yield
    finally:
        with ThreadPoolExecutor(1) as cleaner:
            cleaner.submit(duckdb.reset)
            cleaner.submit(gc.collect) 