# tests/test_ledger_concurrency.py
from __future__ import annotations

import json
import multiprocessing as mp
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
import time
from pykit.ledger import Ledger, Status


# ----------------------------------------------------------------------
def _init_lock(lock):
    """
    Pool initializer: make the shared lock visible as pykit.ledger.LEDGER_LOCK
    in every child process (works for both 'spawn' and 'fork').
    """
    import pykit.ledger.ledger as lg  # local import to get the module inside the worker
    lg.LEDGER_LOCK = lock


def _worker(meta: str, iterations: int):
    """Allocate + mark DONE 'iterations' times against the same ledger file."""
    led = Ledger(meta)
    for _ in range(iterations):
        # print('sleeping for', wait)
        # time.sleep(wait)
        uid = led.allocate("stress")
        led.log("stress", uid, status=Status.DONE)


def test_concurrent_allocate_log_cross_platform():
    """
    Two processes writing concurrently should leave a valid, un-corrupted JSON
    and produce the expected number of DONE records.
    """
    with TemporaryDirectory() as tmpdir:
        meta_file = Path(tmpdir) / "meta.json"
        lock = mp.Lock()

        # Use a process pool so the initializer runs once per worker
        with mp.Pool(
            processes=2,
            initializer=_init_lock,
            initargs=(lock,),
        ) as pool:
            # Each worker performs 50 allocate+log operations
            pool.starmap(_worker, [(str(meta_file), 50)] * 2)
            # pool.join()

        # ---- main process: verify results ----------------------------------
        # Clear the lock in this proc; reads don't need it
        # LEDGER_LOCK = None  # noqa: F401  (silences linters)

        ledger = Ledger(meta_file)
        records = ledger.get_uid_record_dict("stress")

        # Expect 2 workers × 50 iterations = 100 records, all DONE
        assert len(records) == 100
        assert all(r.status is Status.DONE for r in records.values())

        # JSON file should still be parseable by plain json.load (corruption check)
        json.loads(meta_file.read_text())
