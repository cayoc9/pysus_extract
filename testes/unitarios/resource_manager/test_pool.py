import pytest
from unittest.mock import patch
import time
from resource_manager import ManagedThreadPool

def test_dynamic_worker_adjustment():
    with patch('psutil.cpu_percent', return_value=10), \
         patch('psutil.virtual_memory') as mock_mem:
        mock_mem.return_value.available = 8 * 1024**3  # 8GB livres
        
        pool = ManagedThreadPool()
        assert pool.max_workers == 16  # 8GB / 0.3GB = 26.6 → min(26, (90*Núcleos)/20, 16)

def test_throttling_mechanism():
    pool = ManagedThreadPool(max_workers=2)
    
    with patch.object(pool, '_should_throttle', return_value=True):
        start = time.time()
        pool.submit(lambda: time.sleep(0.1))
        assert time.time() - start >= 0.5, "Deveria aplicar throttling"

    with patch.object(pool, '_should_throttle', return_value=False):
        start = time.time()
        pool.submit(lambda: time.sleep(0.1))
        assert time.time() - start < 0.5, "Não deveria aplicar throttling" 