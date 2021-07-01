from abc import ABC, abstractmethod
from typing import Type, Any

from fox_orm.internal.utils import NonInstantiable


class FieldType(NonInstantiable):
    sql_type: Type


class ColumnArgument(ABC):
    key: str

    def apply(self, kwargs):
        kwargs[self.key] = self.get_value()

    @abstractmethod
    def get_value(self) -> Any:
        ...


class ColumnFlag(ColumnArgument):
    inverse: bool

    def get_value(self):
        return not self.inverse

    def __init__(self, key: str, inverse: bool = False):
        self.key = key
        self.inverse = inverse

    def __invert__(self):
        return ColumnFlag(self.key, not self.inverse)
