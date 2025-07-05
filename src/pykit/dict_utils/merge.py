from .flatten import flatten
from .unflatten import unflatten

def merge_by_update(*args: dict) -> dict:
    result = {}
    for flat_dict in args:
        result.update(flat_dict)
    return result


def merge_dicts(*args: dict) -> dict:
    return unflatten(merge_by_update(*map(flatten, args)))