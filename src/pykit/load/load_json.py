import json
from pathlib import Path
from typing import Type, TypeVar

T = TypeVar('T')


def load_json(path: Path | str, cls: T = dict) -> T:
    path = Path(path)

    with open(path, 'r') as f:
        data = f.read()

    data = json.loads(data)
    return cls(**data)
