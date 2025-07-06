import pytest
from pykit.ledger import Ledger
from pykit.project import Project




@pytest.fixture
def ledger(tmp_path) -> Ledger:
    """Fixture to create a Ledger instance for testing."""
    return Ledger(path=tmp_path / 'metadata.json')


@pytest.fixture
def project(tmp_path) -> Project:
    """
    A fresh project rooted in a temporary folder, defaulting to SUBFOLDER
    storage.  Each test gets its own isolated ledger file.
    """
    root = tmp_path / "results"
    proj = Project(root=root)
    proj.ensure_folder()
    return proj
