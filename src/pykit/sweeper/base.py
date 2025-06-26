from typing_extensions import Iterable, TypeVar, Generic
from abc import ABC, abstractmethod

"""

The core idea of the sweeper classes is to facilitate iteration of dict to some iterable values.
i.e. {'u': [1,2,3]} --> {'u': 1}, {'u', 2}, {'u', 3}

the construction of the iterable goes like the following:
1. flat dict
2. convert each value given adapter
3. split into iterables and constants
4. return the dynamic part as well as the constant part
5. goes into a strategy

"""

T = TypeVar('T', bound=Iterable)


class SweepAbstract(ABC, Generic[T]):

    @abstractmethod
    def generate(self) -> Iterable[T]:
        pass


    def len(self):
        return len(list(self.generate()))

