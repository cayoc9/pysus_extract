from resource_manager import ManagedThreadPool
from main import process_data_chunk

def test_high_load_processing():
    test_data = [...]  # Definir dados de teste
    with ManagedThreadPool() as pool:
        futures = [
            pool.submit(process_data_chunk, test_data)
            for _ in range(100)
        ]
        results = [f.result() for f in futures]
    
    assert len(results) == 100
    assert all(r['status'] == 'success' for r in results) 