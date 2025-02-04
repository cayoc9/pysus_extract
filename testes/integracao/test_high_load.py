from resource_manager import ManagedThreadPool
from main import process_file
import asyncio
import pytest

@pytest.mark.asyncio
async def test_concurrent_processing(default_params):
    files = get_test_files()
    
    async with ManagedThreadPool() as pool:
        tasks = [pool.submit_async(process_file, f, default_params) for f in files]
        results = await asyncio.gather(*tasks)
    
    assert sum(len(r) for r in results) > 1_000_000

def get_test_files():
    return [...]  # Lista de arquivos de teste 