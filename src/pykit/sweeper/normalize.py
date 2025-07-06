from typing import Annotated, TypeAlias
from pydantic import BeforeValidator
from .empty import EmptySweep
from .dict_sweep import DictSweep
from .chain_sweep import ChainSweep

SUPPORTED_SWEEP: TypeAlias = DictSweep | ChainSweep | EmptySweep

# Step 1: Create your normalization function
def normalize_sweep(value) -> SUPPORTED_SWEEP:

    if value is None:
        return EmptySweep()

    if isinstance(value, list):
        sweepers = [x if isinstance(x, DictSweep) else DictSweep(**x) for x in value]
        return ChainSweep(sweepers=sweepers)

    if isinstance(value, dict):
        return DictSweep(**value)

    return value


NormalizedSweep = Annotated[SUPPORTED_SWEEP, BeforeValidator(normalize_sweep)]

