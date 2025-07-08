# Project & Session — Friendly Facade

`Project` organises output folders and talks to the shared `Ledger`.
`Session` is a lightweight handle for *one* run: it builds file paths,
updates status, and remembers artefacts.

---

## Quick Start

```python
from tracker import Project

proj = Project(root="results").ensure_folder()

run = proj.session("train", params={"lr": 1e-3})
run.start()

save_model(run.path("model.pt"))
run.done()
```

---

## Key Concepts

| Term            | What it means in practice                              |
| --------------- | ------------------------------------------------------ |
| **Project**     | *Namespace* = root folder + relpath + shared Ledger.   |
| **Session**     | One run identified by `(identifier, uid)`.             |
| **StorageMode** | `SUBFOLDER` (default) or `PREFIX` for file layout.     |
| **ExistingRun** | `RESUME` (reuse finished run) or `NEW` (always fresh). |

---

## Examples

### 1 • Grid Search with Auto-Resume

```python
from tracker import StorageMode, ExistingRun

grid = Project("results/grid", storage_mode=StorageMode.PREFIX)

for cfg in sweep:
    s = grid.session("train", params=cfg,
                     existing_run_strategy=ExistingRun.RESUME)
    if s.is_done():
        continue                 # already finished
    train(cfg, checkpoint=s.path("ckpt.pt"))
    s.done()
```

### 2 • Nested Namespaces

```python
root = Project("results")
imagenet = root.sub("imagenet")           # results/imagenet/
rnd_aug   = imagenet.sub("randaug")       # results/imagenet/randaug/
```

### 3 • Logging Extra Files

```python
s.attach_files({"log": s.path("train.log", include_identifier=False)})
```

---

## Cheat-Sheet

```python
# Project helpers
p.ensure_folder()              # mkdir -p
p.sub("name")                  # deeper relpath
s = p.session("train", params=cfg,
              existing_run_strategy=None,
              storage_mode=None)

# Session helpers
s.start() / s.done() / s.error()
s.status, s.is_done()
s.folder                       # auto-created dir
s.path("file.ext")             # helper for filenames
s.attach_files({"ckpt": Path("ckpt.pt")})
s.files                        # dict[str, Path]
```

