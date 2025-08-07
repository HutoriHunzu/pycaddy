from pathlib import Path
from typing import Annotated
from pydantic import BeforeValidator

def ensure_path(v: Path | str) -> Path:
    if isinstance(v, str):
        return Path(v)
    return v


def ensure_str_from_path(v: Path | str) -> str:
    if isinstance(v, str):
        return v
    return v.as_posix()


def ensure_absolute_path(v: Path) -> Path:
    return v.resolve()


PathLike = Annotated[Path | str, BeforeValidator(ensure_path)]
AbsolutePathLike = Annotated[PathLike, BeforeValidator(ensure_absolute_path)]