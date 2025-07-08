# Ledger — Run Registry

Small, JSON-backed book-keeper that tracks every experiment run in
`metadata.json`. One file per project, one singleton per process.

---

## Quick Start

```python
from tracker import Ledger, Status
from multiprocessing import Lock
from pathlib import Path

# one lock per worker pool
Ledger.set_global_lock(Lock())

led = Ledger(path=Path("results/metadata.json"))

uid = led.allocate("train", relpath=Path("mnist/cnn"))
led.log("train", uid, status=Status.RUNNING)

# … training …

led.log("train", uid, status=Status.DONE,
        path_dict={"model": Path("model.pt")})
```

---

## Key Concepts

| Name                 | Why you care                                                                             |
| -------------------- | ---------------------------------------------------------------------------------------- |
| **identifier**       | Groups runs (`"train"`, `"eval"`, …).                                                    |
| **relpath / relkey** | Sub-folder namespace; empty string means project root.                                   |
| **uid**              | Zero-padded counter unique inside *(identifier, relpath)*.                               |
| **param\_hash**      | Deterministic `int` hash of the params dict; enables auto-resume logic in higher layers. |
| **LEDGER\_LOCK**     | Single `multiprocessing.Lock` that serialises every write.                               |

---

## Examples

### 1 • Record auxiliary files

```python
led.log("train", uid,
        path_dict={"tensorboard": Path("tb/events.out"),
                   "notes":       Path("notes.yaml")})
```

### 2 • Find a previous run by parameters

```python
hit = led.find_by_param_hash("train", param_hash=123456)
if hit:
    old_uid, record = hit
    print("Already trained:", record.files["model"])
```

---

## Cheat-Sheet

```python
# Allocate a new run
uid = led.allocate(identifier, relpath=Path("sub"), param_hash=123)

# Update
led.log(identifier, uid, status=Status.RUNNING)
led.log(identifier, uid, path_dict={"weights": Path("w.pt")})

# Read
rec  = led.get_record(identifier, uid)
uids = led.get_uid_record_dict(identifier, relpath=Path("sub"))
data = led.load()                         # raw JSON snapshot
```



