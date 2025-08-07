from typing import Mapping, Callable, Any
from itertools import product
from typing import Iterable
from enum import StrEnum, auto

StrategyFunc = Callable[[Iterable[Iterable[Any]]], Iterable[Iterable[Any]]]


class StrategyName(StrEnum):
    PRODUCT = auto()
    ZIP = auto()


def product_strategy(values: Iterable[Iterable]) -> Iterable[Iterable]:
    return product(*values)


def zip_strategy(values: Iterable[Iterable]) -> Iterable[Iterable]:
    return zip(*values)


# CONSTANT mapping of name → callable
STRATEGIES: Mapping[StrategyName, StrategyFunc] = {
    StrategyName.PRODUCT: product_strategy,
    StrategyName.ZIP: zip_strategy,
}
