"""
ledger.py
=========
JSON-backed run ledger that is **safe for multiprocessing** and keeps
its in-memory footprint small by loading the file fresh for every write
operation (“lazy reload on write”).

Concurrency model
-----------------
• A single `multiprocessing.Lock` is created by the parent script and
  passed to every worker via the pool’s *initializer*.
• All mutating methods (`allocate`, `attach_file`, `log_end`, `save`)
  acquire this lock so only one process reads-modify-writes the JSON at
  a time.
• The ledger content is *not* cached between calls; each mutating method
  reloads the JSON, applies its change, and writes the file back.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path

import json
from multiprocessing import Lock
from pydantic import BaseModel, Field, TypeAdapter
from pydantic import dataclasses
from contextlib import contextmanager, nullcontext
from typing import ContextManager
from typing import Generator, Literal, TypeAlias
# from collections.abc import Generator

from .singleton import PerPathSingleton
from .status import Status
from .naming_strategy import counter_naming_strategy

from .run_record import RunRecord

# ------------------------------------------------------------------ #
# global multiprocessing lock (set once by the pool initializer)
# ------------------------------------------------------------------ #
LEDGER_LOCK: Lock | None = None
UID_RECORD_DICT: TypeAlias = dict[str, RunRecord]
DATA_STRUCTURE = dict[str, UID_RECORD_DICT]  # type alias for the ledger data structure
DATA_STRUCTURE_MODEL = TypeAdapter(DATA_STRUCTURE)


def _locked(fn):
    """Wrap *fn* with the global LEDGER_LOCK if running under a pool."""

    def wrapper(self, *args, **kwargs):
        if LEDGER_LOCK is None:  # serial mode
            return fn(self, *args, **kwargs)
        with LEDGER_LOCK:
            return fn(self, *args, **kwargs)

    return wrapper


def set_global_lock(lock: Lock | None) -> None:
    """Install the (multiprocessing-) lock that serialises all writers."""
    global LEDGER_LOCK
    LEDGER_LOCK = lock


# ------------------------------------------------------------------ #
# ledger: one instance per file / per process
# ------------------------------------------------------------------ #
class Ledger(metaclass=PerPathSingleton):
    """
    Usage
    -----
    >>> led = Ledger(path="results/metadata.json")
    >>> base = led.allocate("calibration", prefix="", hint=None)
    """

    def __init__(self, path: str | Path, maxsize: int = 1000):
        self.file: Path = Path(path).expanduser().resolve()
        self.file.parent.mkdir(parents=True, exist_ok=True)
        self.maxsize = maxsize
        self._data: DATA_STRUCTURE = {}

    # --------------- public, lock-protected API ---------------- #

    # @_locked  # keep top-level lock here; internals are unlocked now
    def allocate(
            self,
            identifier: str,
            status: Status = Status.PENDING,
            relpath: Path | str = '.',
            param_hash: int | None = None,
    ) -> str:
        """
        Reserve a fresh *uid* for *identifier* and create an initial RunRecord.
        """
        # print("lock id in allocate:", id(LEDGER_LOCK))  # diagnostic
        with self._edit_uid_record_dict(identifier) as uid_records:
            uid = counter_naming_strategy(list(uid_records.keys()), maxsize=self.maxsize)

            record = RunRecord(status=status,
                               param_hash=param_hash,
                               relpath=relpath)
            record.timestamp_status()  # first stamp
            uid_records[uid] = record

            return uid

    def log(
            self,
            identifier: str,
            uid: str,
            *,
            status: Status | None = None,
            path_dict: dict[str, str] | None = None,
    ) -> None:
        """Add a status transition and/or attach files to an existing run."""
        if not (status or path_dict):
            return

        with self._edit_record(identifier, uid) as record:
            if status:
                record.status = status
                record.timestamp_status()
            if path_dict:
                record.files.update(path_dict)

    def get_record(self, identifier: str, uid: str) -> RunRecord:
        """
        Retrieve a run record by its identifier and uid.
        """
        data = self._load()
        if identifier not in data or uid not in data[identifier]:
            raise KeyError(f"No run found for identifier '{identifier}' and uid '{uid}'")
        return data[identifier][uid]

    def get_uid_record_dict(self, identifier: str) -> UID_RECORD_DICT:
        """
        Retrieve all run records for a given identifier.
        """
        data = self._load()
        if identifier not in data:
            raise KeyError(f"No runs found for identifier '{identifier}'")
        return data[identifier]

    def find_by_param_hash(self, identifier: str, param_hash: int | None = None) -> tuple[str, RunRecord] | None:
        """
        Find a run by its identifier and uid.
        requires coherent data READ
        """
        if param_hash is None:
            return None

        uid_records_dict = self.get_uid_record_dict(identifier)
        match = ((u, r) for u, r in uid_records_dict.items() if r.param_hash == param_hash)
        return next(match, None)

    # ------------------------------------------------------------------

    @contextmanager
    def _edit_uid_record_dict(self, identifier) -> Generator[UID_RECORD_DICT, None, None]:
        # data = self._load()
        with self._edit_data() as data:
            yield data.setdefault(identifier, {})
            # uid_record_dict = data.get(identifier, {})
            # data[identifier] = uid_record_dict
            # yield uid_record_dict

    @contextmanager
    def _edit_record(self, identifier: str, uid: str) -> Generator[RunRecord, None, None]:
        with self._edit_uid_record_dict(identifier) as records_dict:
            if uid not in records_dict:
                raise KeyError(f"No run found for identifier '{identifier}' and uid '{uid}'")
            yield records_dict[uid]

    # ------------------------------------------------------------------

    def _load(self) -> DATA_STRUCTURE:
        """Read the ledger JSON and updates in-memory dict; return an in-memory dict."""
        if not self.file.exists():
            return {}

        data = self.file.read_bytes()
        self._data = DATA_STRUCTURE_MODEL.validate_json(data)

        return self._data

    # @_locked
    @contextmanager
    def _edit_data(self):
        lock = LEDGER_LOCK or nullcontext()
        with lock:
            data = self._load()
            yield data
            self._save(data)

    def _save(self, data) -> None:
        """Write *runs* back to disk, pretty-printed."""
        # writes to disk
        serialized_data = DATA_STRUCTURE_MODEL.dump_json(data, indent=2)
        self.file.write_bytes(serialized_data)
        self._data = data  # update in-memory cache
