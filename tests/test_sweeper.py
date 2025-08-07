import pytest
from pydantic import TypeAdapter, BaseModel
from typing_extensions import Literal
from pycaddy.sweeper import DictSweep, ChainSweep, StrategyName
from itertools import product


class RangeAdapter(BaseModel):
    type: Literal['range'] = 'range'
    start: int
    end: int
    step: int

    def __iter__(self):
        return iter(range(self.start, self.end, self.step))


class ConstantAdapter(BaseModel):
    type: Literal['constant'] = 'constant'

    def __iter__(self):
        return iter([1, 2])


func_map = {
    StrategyName.PRODUCT: product,
    StrategyName.ZIP: zip
}

DATA = {'a': [1, 2], 'b': [3, 4]}

EXPECTED = {
    StrategyName.PRODUCT: [{'a': 1, 'b': 3}, {'a': 1, 'b': 4},
                           {'a': 2, 'b': 3}, {'a': 2, 'b': 4}],
    StrategyName.ZIP: [{'a': 1, 'b': 3}, {'a': 2, 'b': 4}],
}


@pytest.mark.parametrize("strategy", [StrategyName.PRODUCT, StrategyName.ZIP])
def test_dict_sweeper_product(strategy: StrategyName):
    sweep_values = DictSweep(parameters=DATA, strategy=strategy).generate()
    for v, e in zip(sweep_values, EXPECTED[strategy]):
        assert (v == e)


def test_chain_sweeper():
    sweep_values = ChainSweep(
        sweepers=[
            DictSweep(parameters={'a': [1, 2], 'b': [3, 4]}, strategy=StrategyName.PRODUCT),
            DictSweep(parameters={'c': [5, 6], 'd': [7, 8]}, strategy=StrategyName.ZIP),
            DictSweep(parameters={'e': [9]}, strategy=StrategyName.PRODUCT)
        ]
    ).generate()

    expected = [
        {'a': 1, 'b': 3, 'c': 5, 'd': 7, 'e': 9},
        {'a': 1, 'b': 3, 'c': 6, 'd': 8, 'e': 9},
        {'a': 1, 'b': 4, 'c': 5, 'd': 7, 'e': 9},
        {'a': 1, 'b': 4, 'c': 6, 'd': 8, 'e': 9},
        {'a': 2, 'b': 3, 'c': 5, 'd': 7, 'e': 9},
        {'a': 2, 'b': 3, 'c': 6, 'd': 8, 'e': 9},
        {'a': 2, 'b': 4, 'c': 5, 'd': 7, 'e': 9},
        {'a': 2, 'b': 4, 'c': 6, 'd': 8, 'e': 9},
    ]

    for v, e in zip(sweep_values, expected):
        assert (v == e)


def test_dict_sweeper_with_adapter():
    data = {'a': {'type': 'range', 'start': 1, 'end': 10, 'step': 2},
            'b': {'type': 'constant'}}

    expected = [
        {'a': 1, 'b': 1},
        {'a': 1, 'b': 2},
        {'a': 3, 'b': 1},
        {'a': 3, 'b': 2},
        {'a': 5, 'b': 1},
        {'a': 5, 'b': 2},
        {'a': 7, 'b': 1},
        {'a': 7, 'b': 2},
        {'a': 9, 'b': 1},
        {'a': 9, 'b': 2},

    ]

    sweep_values = (DictSweep(parameters=data, strategy=StrategyName.PRODUCT).
                    generate(adapter=TypeAdapter(RangeAdapter | ConstantAdapter)))

    for v, e in zip(sweep_values, expected):
        assert (v == e)
