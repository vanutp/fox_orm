import json as json_mod
from abc import abstractmethod, ABC
from typing import Any


from sqlalchemy import ForeignKey
from sqlalchemy.sql.type_api import TypeEngine

from fox_orm.column import json, jsonb
from fox_orm.internal.pydantic_compat import BaseModel


class ColumnArgument(ABC):
    @abstractmethod
    def apply(self, args: list, kwargs: dict, type_: TypeEngine) -> None:
        ...


class ColumnFlag(ColumnArgument):
    key: str
    inverse: bool

    def __init__(self, key: str, inverse: bool = False):
        self.key = key
        self.inverse = inverse

    def __invert__(self):
        return ColumnFlag(self.key, not self.inverse)

    def apply(self, args: list, kwargs: dict, type_: TypeEngine) -> None:
        kwargs[self.key] = not self.inverse


# noinspection PyPep8Naming
# pylint: disable=invalid-name
class default(ColumnArgument):
    key = 'server_default'

    def __init__(self, value: Any):
        self.value = value

    def apply(self, args: list, kwargs: dict, type_: TypeEngine) -> None:
        if callable(self.value):
            return
        if isinstance(type_, (type(json), type(jsonb))):
            if isinstance(self.value, BaseModel):
                kwargs[self.key] = self.value.json()
            else:
                kwargs[self.key] = json_mod.dumps(self.value)
        else:
            kwargs[self.key] = str(self.value)


# noinspection PyPep8Naming
# pylint: disable=invalid-name
class fkey(ColumnArgument):
    def __init__(self, target: Any):
        self.target = target

    def apply(self, args: list, kwargs: dict, type_: TypeEngine) -> None:
        args.append(ForeignKey(self.target))


null = ColumnFlag('nullable')
pkey = ColumnFlag('primary_key')
autoincrement = ColumnFlag('autoincrement')
unique = ColumnFlag('unique')
index = ColumnFlag('index')

__all__ = [
    'ColumnArgument',
    'ColumnFlag',
    'default',
    'fkey',
    'null',
    'pkey',
    'autoincrement',
    'unique',
    'index',
]
