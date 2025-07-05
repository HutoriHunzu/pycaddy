from typing import Any
from collections import defaultdict
from pydantic import TypeAdapter, ValidationError


def tree():
    """Creates a recursive defaultdict."""
    return defaultdict(tree)


def dictify(d):
    """Recursively convert a defaultdict to a regular dict."""
    if isinstance(d, defaultdict):
        return {k: dictify(v) for k, v in d.items()}
    return d


def apply_adapter(value: Any, adapter: TypeAdapter = None):
    if not adapter:
        return value
    try:
        value = adapter.validate_python(value)
    except ValidationError:
        pass
    return value



