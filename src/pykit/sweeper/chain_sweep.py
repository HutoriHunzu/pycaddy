from .base import SweepAbstract
from itertools import product
from typing import Iterable
from ..dict_utils import merge_dicts


class ChainSweep(SweepAbstract[dict]):

    def __init__(self, sweepers: Iterable[SweepAbstract[dict]]):
        self.sweepers = sweepers

    def generate(self) -> Iterable[dict]:
        sweepers_generated = (s.generate() for s in self.sweepers)
        for elem in product(*sweepers_generated):
            yield merge_dicts(*elem)
