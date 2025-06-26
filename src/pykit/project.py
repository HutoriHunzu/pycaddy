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
from pathlib import Path
from typing import Annotated
from pydantic import BaseModel, Field, PrivateAttr, BeforeValidator

from .ledger import Ledger, Status


def ensure_path(v: Path | str) -> Path:
    if isinstance(v, str):
        return Path(v)
    return v


def ensure_str_from_path(v: Path | str) -> str:
    if isinstance(v, str):
        return v
    return v.as_posix()


PathLike = Annotated[Path | str, BeforeValidator(ensure_path)]


# ─────────────────────────────── class ────────────────────────────── #
class Project(BaseModel):
    # serializable fields -------------------------------------------------
    root: PathLike = Field(..., description="Project root directory")
    relpath: PathLike = Field(default_factory=Path, description="Sub-folder prefix")

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
    def root_data(self):
        return self.ledger.data

    @property
    def path(self) -> Path:
        """Filesystem directory represented by *this* Project object."""
        return self.root / self.relpath

    # make folder creation optional but available ------------------------
    def ensure_folder(self) -> None:
        """Create ``self.path`` (and parents) if missing."""
        self.path.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------------- #
    # grouping (clone with longer relpath)
    # -------------------------------------------------------------------- #
    def group(self, name: str) -> Project:
        """
        Return a **new** Project scoped to ``<relpath>/<name>`` and sharing
        the same ledger.
        """
        child = Project(root=self.root, relpath=self.relpath / name)
        child._ledger = self.ledger
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
