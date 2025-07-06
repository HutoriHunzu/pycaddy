# tests/test_session.py
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
def proj(tmp_path) -> Project:
    root = tmp_path / "results"
    p = Project(root=root)
    p.ensure_folder()
    return p


# ----------------------------------------------------------------------
# 1. Status helpers reflect in the ledger
# ----------------------------------------------------------------------
def test_status_transitions(proj: Project):
    run = proj.session("train")

    # initial state (Project allocates with Status.PENDING)
    assert run.status is Status.PENDING

    # RUNNING
    run.start()
    assert run.status is Status.RUNNING

    # DONE
    run.done()
    assert run.status is Status.DONE

    # ERROR override
    run.error()
    assert run.status is Status.ERROR


# ----------------------------------------------------------------------
# 2. SUBFOLDER layout:  <root>/<relpath>/<uid>/identifier_<name>
# ----------------------------------------------------------------------
def test_subfolder_path_builder(proj: Project, tmp_path):
    run = proj.session("exp")

    # Folder auto-created inside uid
    expected_folder = proj.path / run.uid
    assert run.folder == expected_folder
    assert run.folder.is_dir()

    # Identifier prefix present by default
    p = run.path("metrics.json")
    assert p == expected_folder / "exp_metrics.json"

    # include_identifier=False drops that piece
    p2 = run.path("metrics.json", include_identifier=False)
    assert p2 == expected_folder / "metrics.json"

    # Attach a file and verify it winds up in the ledger
    metrics = tmp_path / "m.json"
    metrics.write_text("{}")
    run.attach_files({"metrics": str(metrics)})

    record = proj.ledger.get_record("exp", run.uid)
    assert record.files["metrics"] == metrics


# ----------------------------------------------------------------------
# 3. PREFIX layout:  <root>/<relpath>/uid_identifier_<name>
# ----------------------------------------------------------------------
def test_prefix_path_builder(proj: Project):
    grid = proj.sub("grid")                                # relpath grows

    run = grid.session(
        "eval",
        storage_mode=StorageMode.PREFIX,
        existing_run_strategy=ExistingRun.NEW,
    )

    # Folder is exactly the project folder (no uid subdir)
    assert run.folder == grid.path

    # Filename includes uid + identifier
    fpath = run.path("preds.npy")
    expect = grid.path / f"{run.uid}_eval_preds.npy"
    assert fpath == expect


# ----------------------------------------------------------------------
# 4. Resume keeps same uid and skips re-allocation
# ----------------------------------------------------------------------
def test_resume_session(proj: Project):
    # First run, mark DONE
    s1 = proj.session("sim", params={"seed": 7})
    s1.done()
    first_uid = s1.uid

    # Same params with RESUME strategy
    s2 = proj.session(
        "sim", params={"seed": 7}, existing_run_strategy=ExistingRun.RESUME
    )
    assert s2.uid == first_uid            # reused
    assert len(proj.ledger.get_uid_record_dict("sim")) == 1
