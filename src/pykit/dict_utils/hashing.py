from .flatten import flatten


def _hashable_flat_dict_set(d: dict):
    return frozenset(sorted(flatten(d).items()))


def hash_dict(d: dict):
    hashable_object = _hashable_flat_dict_set(d)
    return hash(hashable_object)
