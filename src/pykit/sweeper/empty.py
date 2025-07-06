from typing import Iterable
from pydantic import BaseModel
from .base import SweepAbstract



class EmptySweep(BaseModel, SweepAbstract[dict]):

    def generate(self) -> Iterable[dict]:
        return [{}]

