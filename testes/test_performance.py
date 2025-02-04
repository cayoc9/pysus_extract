from resource_manager import MemoryGuardian
from unittest.mock import patch
import time

def test_memory_guardian_activation():
    guardian = MemoryGuardian(threshold=5)
    with patch('psutil.virtual_memory') as mock_mem:
        mock_mem.return_value.percent = 10
        guardian.start()
        time.sleep(1)
        assert guardian.is_alive()
        
        mock_mem.return_value.percent = 95
        time.sleep(6)
        assert not guardian.is_alive(), "Deve encerrar processo acima do limite" 