from typing import Any
from pydantic import TypeAdapter

from .utils import apply_adapter


def flatten(d: dict, parent_key=(), adapter: TypeAdapter = None) -> dict[tuple, Any]:
    items = {}
    for k, v in d.items():
        new_key = parent_key + (k,)  # Always treat keys as tuples
        v = apply_adapter(v, adapter)
        if isinstance(v, dict):
            items.update(flatten(v, new_key, adapter=adapter))
        else:
            items[new_key] = v
    return items


def flatten_with_pretty_keys(d: dict, sep: str = '__', adapter: TypeAdapter = None) -> dict[str, Any]:
    flat = flatten(d, adapter=adapter)
    return {sep.join(k): v for k, v in flat.items()}