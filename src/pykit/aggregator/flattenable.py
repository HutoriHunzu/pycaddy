from abc import ABC, abstractmethod

class Flattenable(metaclass=ABC):

    @abstractmethod
    def flatten(self) -> dict:
        pass