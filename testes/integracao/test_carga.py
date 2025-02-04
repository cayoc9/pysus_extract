import pytest
from resource_manager import ManagedThreadPool
from main import process_file

def test_high_load_processing():
    files = [...]  # Lista de 100 arquivos de teste
    
    with ManagedThreadPool() as pool:
        futures = [pool.submit(process_file, f) for f in files]
        results = [f.result() for f in futures]
    
    assert len(results) == len(files)
    assert all('error' not in r for r in results) 