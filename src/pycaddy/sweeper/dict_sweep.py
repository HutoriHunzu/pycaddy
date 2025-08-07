from pydantic import BaseModel, TypeAdapter
from typing_extensions import Iterable
from ..dict_utils import unflatten, merge_by_update
from .construct import construct_split_data
from .strategies import StrategyName, STRATEGIES
from .base import SweepAbstract


class DictSweep(BaseModel, SweepAbstract[dict]):
    """
    Parameter sweeper for generating combinations of experiment parameters.
    
    Takes a dictionary of parameters with lists of values and generates
    all combinations using different strategies (product, zip).
    """
    parameters: dict = {}
    constants: dict = {}
    strategy: StrategyName = StrategyName.PRODUCT

    def sweep(self, values: Iterable[Iterable]) -> Iterable[Iterable]:
        strategy_func = STRATEGIES[self.strategy]
        return strategy_func(values)

    def generate(self, adapter: TypeAdapter = None) -> Iterable[dict]:

        split_data = construct_split_data(parameters=self.parameters,
                                          constants=self.constants,
                                          adapter=adapter)

        keys, values, constants = split_data

        # in case there is nothing to sweep over just return the constants
        if len(keys) == 0 or len(values) == 0:
            yield unflatten(constants)
            return

        for combination in self.sweep(values):
            current = dict(zip(keys, combination))
            current = merge_by_update(constants, current)
            yield unflatten(current)
