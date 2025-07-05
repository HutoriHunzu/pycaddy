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

from pydantic import BaseModel, Field, PrivateAttr
from pathlib import Path

# from .run_location import RunLocation
from .session import Session

from ..ledger import Ledger, Status, RunRecord
from ..dict_utils import hash_dict

from .structs import StorageMode, ExistingRun

from .utils import PathLike, AbsolutePathLike


# ─────────────────────────────── class ────────────────────────────── #
class Project(BaseModel):
    # serializable fields -------------------------------------------------
    root: PathLike = Field(..., description="Project root directory")
    relpath: PathLike = Field(default_factory=Path, description="Sub-folder prefix")
    # storage_mode: StorageMode

    if_exists: ExistingRun = ExistingRun.RESUME,
    storage: StorageMode = StorageMode.SUBFOLDER

    # private, non-serialised state --------------------------------------
    _ledger: Ledger | None = PrivateAttr(default=None)

    # -------------------------------------------------------------------- #
    # public helpers
    # -------------------------------------------------------------------- #
    @property
    def ledger(self) -> Ledger:
        """Shared :class:`Ledger` instance (lazy-loaded)."""
        if self._ledger is None:
            self._ledger = Ledger(path=self.root / "metadata.json")
        return self._ledger

    @property
    def data(self) -> dict[str, dict[str, RunRecord]]:
        data = self.ledger.data  # the raw data is string based
        return {k: {b: RunRecord(**t) for b, t in v.items()} for k, v in data.items()}

    def clean(self):
        self.ledger.clean_non_finished()

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
                if_exists: ExistingRun = None,
                storage: StorageMode = None
                ) -> Session | None:  # may return None when SKIP

        # if resume try to find the current run record
        if_exists = if_exists if if_exists is not None else self.if_exists
        storage = storage if storage is not None else self.storage

        # check params and compute hash
        param_hash = None
        if params:
            param_hash = hash_dict(params)

        # if strategy is RESUME try to find it by hash
        uid, record = None, None
        if params and if_exists.RESUME:
            hit = self.ledger.find_by_param_hash(identifier=identifier, param_hash=param_hash)
            if hit:
                uid, record = hit

        # in case uid is still None it means that either we couldn't find it or
        # the strategy is to create NEW record
        elif uid or if_exists.NEW:
            uid, record = self.ledger.allocate(identifier=identifier, relpath=self.relpath,
                                               param_hash=param_hash)

        else:
            raise ValueError(f'Given unknown strategy {if_exists}')

        assert (uid is not None)
        assert (record is not None)

        return Session(
            ledger=self.ledger,
            identifier=identifier,
            uid=uid,
            record=record,
            folder=self.absolute_path,
            param_hash=param_hash,
        )

    # -------------------------------------------------------------------- #
    # grouping (clone with longer relpath)
    # -------------------------------------------------------------------- #
    def sub(self, name: str) -> Project:
        """
        Return a **new** Project scoped to ``<relpath>/<name>`` and sharing
        the same ledger.
        """
        child = self.model_copy()
        child.relpath = self.relpath / name
        # child = Project(root=self.root, relpath=self.relpath / name)
        child.ensure_folder()
        return child

    # -------------------------------------------------------------------- #
    # three-step workflow (start → attach → finish)
    # -------------------------------------------------------------------- #
    def start(self, identifier: str, *, hint: str | None = None) -> str:
        """
        Allocate a *unique* ``base_path`` for *identifier* and mark status
        **RUNNING**.  Returns the allocated ``base_path``.
        """
        base = self.ledger.allocate(identifier, self.relpath.as_posix(), hint)
        self.ledger.log(identifier, base, status=Status.RUNNING, add_time_start=True)
        return base

    def attach(self, identifier: str, base: str, path_dict: dict[str, Path | str]) -> None:
        """Attach already-written artefacts to the ledger."""
        path_dict = dict(map(lambda x: (x[0], ensure_str_from_path(x[1])), path_dict.items()))
        self.ledger.attach_file(identifier, base, path_dict)

    def finish(
            self,
            identifier: str,
            base: str,
            *,
            status: Status = Status.DONE,
            path_dict: dict[str, Path | str] = None
    ) -> None:
        """Mark the run complete (or errored) and timestamp it."""
        if path_dict is not None:
            self.attach(identifier, base, path_dict)
        self.ledger.log(identifier, base, status=status, add_time_end=True)
