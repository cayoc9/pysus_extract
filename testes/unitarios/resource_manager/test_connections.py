import pytest
import duckdb
from resource_manager import DuckDBConnection

def test_connection_pool_reuse():
    with DuckDBConnection() as conn1:
        conn1_id = id(conn1)
        conn1.execute("CREATE TEMP TABLE test (id INT)")
    
    with DuckDBConnection() as conn2:
        conn2_id = id(conn2)
        assert conn2_id == conn1_id, "IDs de conexão devem ser iguais"
        conn2.execute("SELECT * FROM test")  # Teste funcional

def test_connection_reset():
    with DuckDBConnection() as conn:
        conn.execute("CREATE TEMP TABLE test (id INT)")
    
    with DuckDBConnection() as conn:
        with pytest.raises(duckdb.CatalogException):
            conn.execute("SELECT * FROM test") 