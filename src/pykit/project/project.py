"""
project.py
~~~~~~~~~~
A *pydantic-aware* facade that lets you organise output folders and
log runs through a shared :class:`~workflow.ledger.Ledger`.

* ``root``     – absolute directory that owns ``metadata.json``
* ``relpath``  – sub-folder this instance points to (``Path("")`` at root)
* ``ledger``   – lazily-loaded, shared across every clone produced by
                 :py:meth:`Project.group`
"""

from __future__ import annotations

import dataclasses

from pydantic import BaseModel, Field, PrivateAttr
from pathlib import Path

# from .run_location import RunLocation
from .session import Session

from ..ledger import Ledger, Status, RunRecord
from ..dict_utils import hash_dict

from .structs import StorageMode, ExistingRun

from ..utils import PathLike, AbsolutePathLike


# ─────────────────────────────── class ────────────────────────────── #

class Project(BaseModel):
    # serializable fields -------------------------------------------------
    root: PathLike = Field(..., description="Project root directory")
    relpath: PathLike = Field(default_factory=Path, description="Sub-folder prefix")

    existing_run_strategy: ExistingRun = ExistingRun.RESUME
    storage_mode: StorageMode = StorageMode.SUBFOLDER

    # private, non-serialised state --------------------------------------
    _ledger: Ledger | None = PrivateAttr(default=None)
    _ledger_file_name: str = 'metadata.json'

    # -------------------------------------------------------------------- #
    # public helpers
    # -------------------------------------------------------------------- #
    @property
    def ledger_path(self):
        return self.root / self._ledger_file_name

    @property
    def ledger(self) -> Ledger:
        """Shared :class:`Ledger` instance (lazy-loaded)."""
        if self._ledger is None:
            self._ledger = Ledger(path=self.ledger_path)
        return self._ledger

    @property
    def path(self) -> Path:
        """Filesystem directory represented by *this* Project object."""
        return self.root / self.relpath

    @property
    def absolute_path(self) -> Path:
        return self.path.resolve()

    # make folder creation optional but available ------------------------
    def ensure_folder(self) -> None:
        """Create ``self.path`` (and parents) if missing."""
        self.path.mkdir(parents=True, exist_ok=True)

    def session(self,
                identifier: str,
                *,
                params: dict | None = None,
                existing_run_strategy: ExistingRun = None,
                storage_mode: StorageMode = None
                ) -> Session:  # may return None when SKIP

        # if resume try to find the current run record
        existing_run_strategy = existing_run_strategy or self.existing_run_strategy
        storage_mode = storage_mode or self.storage_mode

        # check params and compute hash
        param_hash = None
        if params:
            param_hash = hash_dict(params)

        # starting with None uid
        uid = None

        # if strategy is RESUME try to find it by hash
        if params and existing_run_strategy == ExistingRun.RESUME:
            hit = self.ledger.find_by_param_hash(identifier=identifier,
                                                 relpath=self.relpath,
                                                 param_hash=param_hash)
            if hit:
                uid, record = hit

        # in case uid is still None it means that either we couldn't find it or
        # the strategy is to create NEW record
        if not uid or existing_run_strategy == ExistingRun.NEW:
            uid = self.ledger.allocate(identifier=identifier,
                                       relpath=self.relpath,
                                       param_hash=param_hash)

        assert (uid is not None)

        return Session(
            ledger=self.ledger,
            identifier=identifier,
            uid=uid,
            relpath=self.relpath,
            absolute_path=self.absolute_path,
            param_hash=param_hash,
            storage_mode=storage_mode
        )

    # -------------------------------------------------------------------- #
    # grouping (clone with longer relpath)
    # -------------------------------------------------------------------- #
    def sub(self, name: str) -> Project:
        """
        Return a **new** Project scoped to ``<relpath>/<name>`` and sharing
        the same ledger.
        """
        child = Project(root=self.root,
                        relpath=self.relpath / name,
                        existing_run_strategy=self.existing_run_strategy,
                        storage_mode=self.storage_mode,
                        _ledger_file_name=self._ledger_file_name)

        child.ensure_folder()
        return child
