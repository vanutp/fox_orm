import json as json_mod
from abc import abstractmethod, ABC
from typing import Any

from pydantic import BaseModel
from sqlalchemy import ForeignKey


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


# noinspection PyPep8Naming
# pylint: disable=invalid-name
class default(ColumnArgument):
    key = 'server_default'

    def __init__(self, value: Any):
        self.value = value

    def should_set_server_default(self):
        return (
            isinstance(self.value, (int, str, bool, dict, list, BaseModel))
            or self.value is None
        )

    def apply(self, args: list, kwargs: dict):
        if self.should_set_server_default():
            if isinstance(self.value, BaseModel):
                kwargs['server_default'] = self.value.json()
            else:
                kwargs['server_default'] = json_mod.dumps(self.value)


# noinspection PyPep8Naming
# pylint: disable=invalid-name
class fkey(ColumnArgument):
    def __init__(self, target: str):
        self.target = target

    def apply(self, args: list, kwargs: dict):
        args.append(ForeignKey(self.target))


null = ColumnFlag('nullable')
pk = ColumnFlag('primary_key')
autoincrement = ColumnFlag('autoincrement')
unique = ColumnFlag('unique')
index = ColumnFlag('index')

__all__ = [
    'ColumnArgument',
    'ColumnFlag',
    'default',
    'fkey',
    'null',
    'pk',
    'autoincrement',
    'unique',
    'index',
]
