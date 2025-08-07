from .utils import tree, dictify


def unflatten(flat_dict):
    """
    Convert a flat dictionary with tuple keys into a nested dictionary
    using defaultdict for automatic sub-dictionary creation.

    Example:
        flat_dict = {(1, 2, 3): 4, (1, 2, 5): 6, (7,): 8}
        returns {1: {2: {3: 4, 5: 6}}, 7: 8}
    """
    nested = tree()  # This is our recursive defaultdict.

    for key, value in flat_dict.items():
        # Normalize the key: if it's not a tuple, wrap it in one.
        if not isinstance(key, tuple):
            key = (key,)

        current = nested
        # Traverse the tree for all key parts except the last one.
        for part in key[:-1]:
            current = current[part]
        # Set the value at the final key part.
        current[key[-1]] = value

    # Optionally, convert the defaultdict tree into a standard dict.
    return dictify(nested)
