"""
ledger.py — v2
==============

Light-weight, JSON-backed *run ledger* for experiment tracking.
Each run is stored under the triple (identifier, relpath, uid).

Quick usage
-----------
>>> led = Ledger("results/metadata.json")
>>> uid = led.allocate("train", relpath=Path("grid/lr-1e3"))
>>> led.log("train", uid, status=Status.DONE)

What to keep in mind
--------------------
- Pass `relpath` as a *relative* `Path`; use `Path("")` for the project
  root.  The ledger does **no** path validation beyond converting it to
  a string key.
- One `multiprocessing.Lock` (set by `set_global_lock`) serialises every
  write; reads are lock-free.
- UID counters are independent for each (identifier, relpath) pair.
"""

from __future__ import annotations

import json
from contextlib import contextmanager, nullcontext
from datetime import datetime
from multiprocessing import Lock
from pathlib import Path
from typing import Generator, TypeAlias
from filelock import FileLock
from pydantic import TypeAdapter

from .naming_strategy import counter_naming_strategy
from .run_record import RunRecord
from .singleton import PerPathSingleton
from .status import Status

# ---------------------------------------------------------------------
#  Globals
# ---------------------------------------------------------------------

# LEDGER_LOCK: Lock | None = None   # installed once in the pool initializer
#
#
# def set_global_lock(lock: Lock | None) -> None:
#     """Register the writer lock used by all processes."""
#     global LEDGER_LOCK
#     LEDGER_LOCK = lock


# ---------------------------------------------------------------------
#  Type aliases & helpers
# ---------------------------------------------------------------------

UID_RECORD_DICT: TypeAlias = dict[str, RunRecord]          # uid   -> record
RELPATH_DICT:     TypeAlias = dict[str, UID_RECORD_DICT]   # rel   -> …
DATA_STRUCTURE:   TypeAlias = dict[str, RELPATH_DICT]      # ident -> …

DATA_ADAPTER = TypeAdapter(DATA_STRUCTURE)


def _relkey(relpath: Path) -> str:
    """
    Turn a `Path` into the JSON key.

    Path("") or Path(".") -> ""
    everything else       -> POSIX string
    """
    s = relpath.as_posix()
    return "" if s in ("", ".") else s


# ---------------------------------------------------------------------
#  Ledger
# ---------------------------------------------------------------------

class Ledger(metaclass=PerPathSingleton):
    """One instance per metadata.json *per process*."""

    # -----------------------------------------------------------------
    #  Life-cycle
    # -----------------------------------------------------------------
    def __init__(self, path: str | Path, maxsize: int = 1000) -> None:
        self.file: Path = Path(path).expanduser().resolve()
        self.file.parent.mkdir(parents=True, exist_ok=True)

        self.maxsize = maxsize
        self._data: DATA_STRUCTURE = {}        # lazy cache

        # NEW: always use a file-based lock
        self._file_lock_path = self.file.with_suffix(".lock")
        self._file_lock = FileLock(str(self._file_lock_path))

    # -----------------------------------------------------------------
    #  Public API
    # -----------------------------------------------------------------
    def allocate(
        self,
        identifier: str,
        *,
        status: Status = Status.PENDING,
        relpath: Path = Path(""),
        param_hash: str | None = None,
    ) -> str:
        """
        Reserve a fresh uid for (identifier, relpath) and create
        the first RunRecord.

        Returns
        -------
        uid : str      zero-padded counter ("000", "001", …)
        """
        rkey = _relkey(relpath)

        with self._edit_uid_record_dict(identifier, rkey) as uid_dict:
            uid = counter_naming_strategy(uid_dict.keys(), maxsize=self.maxsize)
            record = RunRecord(
                status=status,
                param_hash=param_hash
            )
            record.timestamp_status()
            uid_dict[uid] = record
            return uid

    # -----------------------------------------------------------------
    def log(
        self,
        identifier: str,
        uid: str,
        *,
        relpath: Path = Path(""),
        status: Status | None = None,
        path_dict: dict[str, Path] | None = None,
    ) -> None:
        """
        Update an existing run.

        - status     : new lifecycle status (optional)
        - path_dict  : artefact name -> file path (optional)
        """
        if not (status or path_dict):
            return

        rkey = _relkey(relpath)
        with self._edit_record(identifier, rkey, uid) as rec:
            if status:
                rec.status = status
                rec.timestamp_status()
            if path_dict:
                rec.files.update(path_dict)

    # -----------------------------------------------------------------
    def get_record(
        self,
        identifier: str,
        uid: str,
        *,
        relpath: Path = Path(""),
    ) -> RunRecord:
        """Return a single RunRecord; raises KeyError if not found."""
        rkey = _relkey(relpath)
        data = self._load()
        try:
            return data[identifier][rkey][uid]
        except KeyError as exc:
            raise KeyError(
                f"run not found: identifier='{identifier}', relpath='{rkey}', "
                f"uid='{uid}'"
            ) from exc

    def get_uid_record_dict(
        self,
        identifier: str,
        *,
        relpath: Path = Path(""),
    ):
        """
        Fetch many records.

        - relpath given -> dict[uid, RunRecord]
        - relpath None  -> dict[relpath, dict[uid, RunRecord]]
        """
        data = self._load()
        if identifier not in data:
            return {}

        rkey = _relkey(relpath)
        return data[identifier].get(rkey, {})

    def load(self):
        return self._load()

    def find_by_param_hash(
        self,
        identifier: str,
        param_hash: str,
        *,
        relpath: Path = Path(""),
    ) -> tuple[str, RunRecord] | None:
        """
        Linear scan for a run with matching param_hash.
        Returns (uid, RunRecord) or None.
        """

        # if relpath is None:
        #     uid_iter = (
        #         (u, r)
        #         for uid_dict in self.get_uid_record_dict(identifier).values()
        #         for u, r in uid_dict.items()
        #     )
        # else:
        #     rkey = _relkey(relpath)
        uid_iter = self.get_uid_record_dict(identifier, relpath=relpath).items()

        return next(((u, r) for u, r in uid_iter if r.param_hash == param_hash), None)

    # -----------------------------------------------------------------
    #  Internals: context helpers
    # -----------------------------------------------------------------
    @contextmanager
    def _edit_uid_record_dict(
        self,
        identifier: str,
        relkey: str,
    ) -> Generator[UID_RECORD_DICT, None, None]:
        """
        Yield the uid-dict for mutation and persist on exit
        (wrapped by the global lock).
        """
        with self._edit_data() as data:
            rel_dict = data.setdefault(identifier, {})
            yield rel_dict.setdefault(relkey, {})

    @contextmanager
    def _edit_record(
        self,
        identifier: str,
        relkey: str,
        uid: str,
    ) -> Generator[RunRecord, None, None]:
        """Yield a mutable RunRecord while guaranteeing persistence."""
        with self._edit_uid_record_dict(identifier, relkey) as uid_dict:
            if uid not in uid_dict:
                raise KeyError(
                    f"run not found: identifier='{identifier}', relpath='{relkey}', "
                    f"uid='{uid}'"
                )
            yield uid_dict[uid]

    # -----------------------------------------------------------------
    #  Disk I/O   (all writes go through _edit_data & optional lock)
    # -----------------------------------------------------------------
    def _load(self) -> DATA_STRUCTURE:
        """Return cached data or load from disk if needed (no lock)."""
        if not self.file.exists():
            return {}

        try:
            data = DATA_ADAPTER.validate_json(self.file.read_bytes())
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"failed to parse {self.file}: {exc}") from exc

        self._data = data
        return data

    @contextmanager
    def _edit_data(self) -> Generator[DATA_STRUCTURE, None, None]:
        """
        Context manager that:
        1. acquires the global lock (if any),
        2. loads JSON,
        3. yields it for mutation,
        4. writes it back.
        """
        lock = LEDGER_LOCK or nullcontext()
        with lock:
            data = self._load()
            yield data
            self._save(data)

    @contextmanager
    def _edit_data(self) -> Generator[DATA_STRUCTURE, None, None]:
        with self._file_lock:
            data = self._load()
            yield data
            self._save(data)

    def _save(self, data: DATA_STRUCTURE) -> None:
        """Pretty-print JSON to disk and update the in-memory cache."""
        serialized_data = DATA_ADAPTER.dump_json(data, indent=2)
        self.file.write_bytes(serialized_data)
        self._data = data
