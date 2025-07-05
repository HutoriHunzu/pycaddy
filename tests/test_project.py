# tests/test_project.py
from __future__ import annotations

from pathlib import Path
import pytest

from pykit.project import Project
from pykit.project.structs import StorageMode, ExistingRun
from pykit.ledger import Status


# ----------------------------------------------------------------------
# fixtures
# ----------------------------------------------------------------------
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


# ----------------------------------------------------------------------
# basic navigation
# ----------------------------------------------------------------------
def test_sub_appends_relpath_and_shares_ledger(project: Project):
    child = project.sub("child")

    # root stays the same, relpath grows
    assert child.root == project.root
    assert child.relpath == project.relpath / "child"

    # Both clones talk to the *same* Ledger instance (singleton per path)
    assert child.ledger is project.ledger


# ----------------------------------------------------------------------
# session creation & folder layout (SUBFOLDER mode)
# ----------------------------------------------------------------------
def test_session_allocates_record_and_makes_uid_folder(project: Project):
    run = project.session(
        "train",
        params={"lr": 0.1},
        existing_run_strategy=ExistingRun.NEW,
    )

    # Ledger now contains exactly one record for this identifier
    rec_dict = project.ledger.get_uid_record_dict("train")
    assert list(rec_dict) == [run.uid]          # same UID
    assert rec_dict[run.uid].status is Status.PENDING

    # Folder was created at  <root>/<relpath>/<uid>/
    assert run.folder.is_dir()
    assert run.folder == project.path / run.uid

    # Path-builder should prefix identifier (your chosen convention)
    metrics_path = run.path("metrics.json")
    assert metrics_path == run.folder / f"{run.identifier}_metrics.json"


# ----------------------------------------------------------------------
# path-building rules in PREFIX mode
# ----------------------------------------------------------------------
def test_prefix_storage_mode_paths(project: Project):
    grid_proj = project.sub("grid")

    run = grid_proj.session(
        "train",
        storage_mode=StorageMode.PREFIX,
        existing_run_strategy=ExistingRun.NEW,
    )

    # Folder is *not* inside uid – it’s exactly grid_proj.path
    assert run.folder == grid_proj.path
    assert run.folder.is_dir()

    # Filename must be "<uid>_train_model.pt"
    model_path = run.path("model.pt")
    expected = run.folder / f"{run.uid}_{run.identifier}_model.pt"
    assert model_path == expected


# ----------------------------------------------------------------------
# resume logic via ExistingRun.RESUME
# ----------------------------------------------------------------------
def test_resume_returns_existing_uid(project: Project):
    # First run – mark it DONE
    first = project.session(
        "train",
        params={"seed": 123},
        existing_run_strategy=ExistingRun.NEW,
    )
    first.done()
    uid_first = first.uid

    # Second call with same params & RESUME strategy
    resumed = project.session(
        "train",
        params={"seed": 123},
        existing_run_strategy=ExistingRun.RESUME,
    )

    # Should reuse the previous UID and *not* create a second record
    assert resumed.uid == uid_first
    rec_dict = project.ledger.get_uid_record_dict("train")
    assert len(rec_dict) == 1
