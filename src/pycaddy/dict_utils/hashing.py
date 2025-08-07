from .flatten import flatten
import hashlib
import json

# def _hashable_flat_dict_set(d: dict):
#     return frozenset(sorted(flatten(d).items()))
#
#
# def hash_dict(d: dict):
#     hashable_object = _hashable_flat_dict_set(d)
#     return hash(hashable_object)



def _hashable_flat_dict_set(d: dict):
    # Create a sorted list of flattened key-value pairs
    return sorted(flatten(d).items())


def hash_dict(d: dict) -> str:
    hashable_object = _hashable_flat_dict_set(d)
    # Convert to JSON string for consistent serialization
    json_string = json.dumps(hashable_object, separators=(',', ':'), ensure_ascii=True)
    # Use SHA256 for consistent hash
    return hashlib.sha256(json_string.encode('utf-8')).hexdigest()



