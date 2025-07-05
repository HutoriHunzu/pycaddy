from typing import NamedTuple, Any, Iterable
from pydantic import TypeAdapter
from ..dict_utils import split_dict_by_adapter, flatten, merge_by_update

DefaultAdapter = list | tuple
IterableAdapter = TypeAdapter(DefaultAdapter)


class SplitData(NamedTuple):
    iterable_keys: list[str]
    iterable_values: list[Iterable]
    constants: dict[str, Any]


def construct_split_data(parameters: dict, constants: dict = None,
                         adapter: TypeAdapter = None) -> SplitData:

    if constants is None:
        constants = {}

    # flattening the dictionary given user adapter
    flat_data = flatten(parameters, adapter=adapter)

    # dividing by iterable adapter
    iterable_data, iterable_constants = split_dict_by_adapter(flat_data, IterableAdapter)

    # combine constants with constants appearing in parameters
    constants = merge_by_update(constants, iterable_constants)

    # get all iterable with their corresponding keys
    iterable_keys = list(iterable_data.keys())
    iterable_values = list(iterable_data.values())

    return SplitData(iterable_keys, iterable_values, constants)
