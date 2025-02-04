import pytest
import asyncio
from resource_manager import ManagedThreadPool
from main import process_file

@pytest.mark.asyncio
async def test_high_load_processing(default_params):
    files = [...]  # Lista de arquivos mockados
    
    async with ManagedThreadPool() as pool:
        tasks = [pool.submit(process_file, f, default_params) for f in files]
        results = await asyncio.gather(*tasks)
    
    assert len(results) == len(files)
    assert all('error' not in r for r in results) 