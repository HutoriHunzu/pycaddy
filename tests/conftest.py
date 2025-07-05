import pytest
from pykit.ledger import Ledger



@pytest.fixture
def ledger(tmp_path) -> Ledger:
    """Fixture to create a Ledger instance for testing."""
    return Ledger(path=tmp_path / 'metadata.json')
