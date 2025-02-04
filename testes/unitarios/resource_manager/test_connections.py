import pytest
import duckdb
from resource_manager import DuckDBConnection

def test_connection_pool_reuse():
    with DuckDBConnection() as conn1:
        pass
    
    with DuckDBConnection() as conn2:
        assert conn1 == conn2, "Deveria reutilizar a mesma conexão"

def test_connection_reset():
    with DuckDBConnection() as conn:
        conn.execute("CREATE TEMP TABLE test (id INT)")
    
    with DuckDBConnection() as conn:
        with pytest.raises(duckdb.CatalogException):
            conn.execute("SELECT * FROM test") 