from pydantic import TypeAdapter, ValidationError


def split_dict_by_adapter(d: dict, adapter: TypeAdapter) -> tuple[dict, dict]:
    matching: dict = {}
    non_matching: dict = {}
    for key, value in d.items():
        try:
            adapter.validate_python(value)
        except ValidationError:
            non_matching[key] = value
        else:
            matching[key] = value

    return matching, non_matching
