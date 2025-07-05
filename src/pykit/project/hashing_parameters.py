from ..dict_utils import hash_dict


def hashable_flat_dict(params: dict | None = None):
    if params is None:
        return None

    return hash_dict(params)
