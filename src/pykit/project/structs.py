from enum import StrEnum, auto


class StorageMode(StrEnum):
    SUBFOLDER = auto()
    PREFIX = auto()


class ExistingRun(StrEnum):
    RESUME = auto()
    NEW = auto()