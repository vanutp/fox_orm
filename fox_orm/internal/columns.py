from abc import ABC, abstractmethod
from typing import Type

from fox_orm.internal.utils import NonInstantiable


class FieldType(NonInstantiable):
    sql_type: Type


class ColumnArgument(ABC):
    @abstractmethod
    def apply(self, args: list, kwargs: dict) -> None:
        ...


class ColumnFlag(ColumnArgument):
    key: str
    inverse: bool

    def __init__(self, key: str, inverse: bool = False):
        self.key = key
        self.inverse = inverse

    def __invert__(self):
        return ColumnFlag(self.key, not self.inverse)

    def apply(self, args: list, kwargs: dict):
        kwargs[self.key] = not self.inverse
