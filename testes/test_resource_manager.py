import time
from unittest.mock import patch
import pytest
import duckdb
from resource_manager import ManagedThreadPool, DuckDBConnection, MemoryGuardian

def test_thread_pool_scaling():
    pool = ManagedThreadPool()
    initial_workers = pool.max_workers
    
    # Simular alta carga
    with patch('psutil.virtual_memory') as mock_mem:
        mock_mem.return_value.available = 0.1 * 1024**3  # 100MB livres
        new_workers = pool.calculate_max_workers()
    
    assert new_workers < initial_workers, "Pool deve reduzir workers sob alta carga"

def test_throttling_mechanism():
    pool = ManagedThreadPool(max_workers=2)
    with patch.object(pool, '_should_throttle', return_value=True):
        start = time.time()
        pool.submit(time.sleep, 0.1)
        elapsed = time.time() - start
        assert elapsed >= 0.5, "Throttling deve adicionar atraso"

def test_connection_pool_reuse():
    with DuckDBConnection() as conn1:
        pass
    with DuckDBConnection() as conn2:
        assert conn1 == conn2, "Deve reutilizar conexão do pool"

def test_connection_reset():
    with DuckDBConnection() as conn:
        conn.execute("CREATE TEMP TABLE test (id INT)")
    with DuckDBConnection() as conn:
        with pytest.raises(duckdb.CatalogException):
            conn.execute("SELECT * FROM test"), "Conexão deve ser resetada" 