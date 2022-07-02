import re
import types
import typing
from typing import Type, Any

from sqlalchemy.sql.type_api import TypeEngine


def try_index(lst: list, item: Any) -> int | None:
    try:
        return lst.index(item)
    except ValueError:
        return None


def lenient_issubclass(
    cls: Any, class_or_tuple: Type[Any] | tuple[Type[Any], ...] | None
) -> bool:
    return isinstance(cls, type) and issubclass(cls, class_or_tuple)


def is_sqla_type(type_: Type[Any]) -> bool:
    return lenient_issubclass(type_, TypeEngine) or isinstance(type_, TypeEngine)


class _UnsupportedType:
    pass


UNSUPPORTED_TYPE = _UnsupportedType()


# returns (type, required)
def parse_type(type_: Type[Any]) -> tuple[Type[Any] | _UnsupportedType, bool]:
    origin = typing.get_origin(type_)
    if origin is None:
        return type_, True
    if (
        origin is types.UnionType
        or origin is typing.Union
        and len((args := typing.get_args(type_))) == 2
        and (none_idx := try_index(args, type(None))) is not None
    ):
        res, _ = parse_type(args[1 - none_idx])
        return res, False
    if any(origin is x for x in (list, set, dict)):
        return origin, True
    return UNSUPPORTED_TYPE, True


class OptionalAwaitable:
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __await__(self):
        return self.func(*self.args, **self.kwargs).__await__()


# https://stackoverflow.com/a/1176023
CAMEL_TO_SNAKE_PAT1 = re.compile(r'(.)([A-Z][a-z]+)')
CAMEL_TO_SNAKE_PAT2 = re.compile(r'([a-z0-9])([A-Z])')


def camel_to_snake(name):
    name = CAMEL_TO_SNAKE_PAT1.sub(r'\1_\2', name)
    return CAMEL_TO_SNAKE_PAT2.sub(r'\1_\2', name).lower()


__all__ = [
    'try_index',
    'lenient_issubclass',
    'is_sqla_type',
    'UNSUPPORTED_TYPE',
    'parse_type',
    'OptionalAwaitable',
    'class_or_instancemethod',
    'camel_to_snake',
]
