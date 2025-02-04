from resource_manager import ManagedThreadPool
from main import process_file

def test_concurrent_processing():
    with ManagedThreadPool() as pool:
        tasks = [pool.submit(process_file, f) for f in get_test_files()]
        results = [t.result() for t in tasks]
    
    assert len(results) == len(tasks)
    assert sum(len(r) for r in results) > 1_000_000 

def get_test_files():
    return [...]  # Lista de arquivos de teste 