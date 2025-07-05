from typing import Callable
from math import log10, ceil

NAMING_STRATEGY = Callable[[list[str]], str]


def uid_formatting(number: int, padding: int):
    return f'{number:0{padding}d}'

def counter_naming_strategy(data: list[str], maxsize: int = 1000) -> str:
    padding = int(ceil(log10(maxsize)))
    data = set(data)
    i = 0
    name = uid_formatting(i, padding)
    while name in data:
        i += 1
        name = uid_formatting(i, padding)
    return name
