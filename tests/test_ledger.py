# tests/test_ledger.py
from pykit.ledger import Status


def test_allocate_creates_unique_uids(ledger):
    """Ten allocations under the same identifier should yield ten distinct uids."""
    uids = [ledger.allocate("A") for _ in range(10)]

    records = ledger.get_uid_record_dict("A")
    print(records)
    assert len(records) == 10
    # ensure the returned uids match what the ledger stored
    assert set(uids) == set(records.keys())


def test_log_updates_status_and_timestamps(ledger):
    """Calling log() with a new status should update .status and add a timestamp."""
    uid = ledger.allocate("A")                     # initial status = PENDING
    ledger.log("A", uid, status=Status.DONE)      # transition to DONE

    record = ledger.get_record("A", uid)
    assert record.status is Status.DONE

    # last entry in the history must correspond to this DONE transition
    ts, st = record.timestamp_status_lst[-1]
    assert st is Status.DONE
    assert (ts - ts.replace(tzinfo=None)).total_seconds() >= 0   # basic sanity


def test_log_can_attach_files(ledger, tmp_path):
    """log() should merge arbitrary file mappings into the record."""
    uid = ledger.allocate("A")

    metrics = tmp_path / "metrics.json"
    metrics.write_text("{}")

    ledger.log("A", uid, path_dict={"metrics": str(metrics)})

    record = ledger.get_record("A", uid)
    assert "metrics" in str(record.files)
    assert record.files["metrics"] == metrics

# ----------------------------------------------------------------------
def test_find_by_param_hash(ledger):
    uid_foo = ledger.allocate("A", param_hash=hash("foo"))
    ledger.allocate("A", param_hash=hash("bar"))
    hit_uid, hit_record = ledger.find_by_param_hash("A", hash("foo"))

    assert hit_uid == uid_foo
    assert hit_record.param_hash == hash("foo")

# ----------------------------------------------------------------------
def test_log_noop_fast_exit(ledger):
    """If log() is called with no status and no files, nothing should change."""
    uid = ledger.allocate("A")
    before = ledger.get_record("A", uid)

    ledger.log("A", uid)  # noop
    after = ledger.get_record("A", uid)

    assert before.model_dump() == after.model_dump()  # deep-equality check

