from copy import deepcopy, copy
from enum import Enum
from functools import wraps
from typing import TYPE_CHECKING, TypeVar, Generic, Type, Any, overload

from fox_orm import FoxOrm
from fox_orm.exceptions import QueryBuiltError, InvalidQueryTypeError
from sqlalchemy.sql import Select, Delete

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from fox_orm.model import OrmModel

MODEL = TypeVar('MODEL', bound='OrmModel')


class QueryType(Enum):
    select = 0
    delete = 1


def _builder(applies_to_built_query: bool = False):
    def decorator(f):
        @wraps(f)
        def wrapper(self: 'Query[MODEL]', *args, **kwargs) -> 'Query[MODEL]':
            if not applies_to_built_query:
                self._assert_query_not_built()
            self = copy(self)
            ret = f(self, *args, **kwargs)
            assert ret is None
            return self

        return wrapper

    return decorator


class Query(Generic[MODEL]):
    _model: Type[MODEL]
    _type: QueryType
    _where: list
    _order_by: list
    _limit: int | None
    _offset: int | None
    _built_query: Any
    _values: dict | None

    def __init__(self, _model: Type[MODEL], _type: QueryType):
        self._model = _model
        self._type = _type
        self._where = []
        self._order_by = []
        self._limit = None
        self._offset = None
        self._built_query = None
        self._values = None

    def __copy__(self):
        query = Query(self._model, self._type)
        query._where = copy(self._where)
        query._order_by = copy(self._order_by)
        query._limit = self._limit
        query._offset = self._offset
        query._built_query = self._built_query
        query._values = deepcopy(self._values)
        return query

    def _build_query(self) -> Select | Delete:
        if self._built_query is not None:
            return self._built_query
        if self._type == QueryType.select:
            built_query = self._model.__table__.select()
        elif self._type == QueryType.delete:
            built_query = self._model.__table__.delete()
        else:
            raise ValueError('Invalid query type')
        for where_component in self._where:
            built_query = built_query.where(where_component)
        for order_by_component in self._order_by:
            built_query = built_query.order_by(order_by_component)
        if self._limit is not None:
            built_query = built_query.limit(self._limit)
        if self._offset is not None:
            built_query = built_query.offset(self._offset)
        return built_query

    def _assert_query_not_built(self):
        if self._built_query is not None:
            raise QueryBuiltError

    @_builder()
    def _set_built_query(
        self: 'Query[MODEL]', query: Select | Delete | str
    ) -> 'Query[MODEL]':
        if not isinstance(query, str):
            if not isinstance(query, (Select, Delete)):
                raise TypeError('query must be a Select or Delete object or a string')
            if (isinstance(query, Select) and self._type != QueryType.select) or (
                isinstance(query, Delete) and self._type != QueryType.delete
            ):
                raise InvalidQueryTypeError(
                    type(query).__qualname__.lower(), self._type.name
                )
        self._built_query = query

    @overload
    def where(self: 'Query[MODEL]', *args, **kwargs) -> 'Query[MODEL]':
        ...

    @_builder()
    def where(self, *args, **kwargs):
        if not args and not kwargs:
            raise TypeError('No arguments provided')
        if args and kwargs:
            raise TypeError('Positional and keyword arguments cannot be used together')
        for clause in args:
            self._where.append(clause)
        for key, value in kwargs.items():
            self._where.append(self._model.__table__.c[key] == value)

    @overload
    def order_by(self: 'Query[MODEL]', *args, **kwargs) -> 'Query[MODEL]':
        ...

    @_builder()
    def order_by(self, *args, **kwargs):
        if not args and not kwargs:
            raise TypeError('No arguments provided')
        if args and kwargs:
            raise TypeError('Positional and keyword arguments cannot be used together')
        for clause in args:
            self._order_by.append(clause)
        for key, value in kwargs.items():
            column = self._model.__table__.c[key]
            if value not in (-1, 1):
                raise ValueError('Keyword argument value must be -1 or 1')
            self._order_by.append(column.asc() if value == 1 else column.desc())

    @overload
    def limit(self: 'Query[MODEL]', limit: int) -> 'Query[MODEL]':
        ...

    @_builder()
    def limit(self, limit: int):
        self._limit = limit

    @overload
    def offset(self: 'Quчery[MODEL]', offset: int) -> 'Query[MODEL]':
        ...

    @_builder()
    def offset(self, offset: int):
        self._offset = offset

    @overload
    def values(
        self: 'Query[MODEL]', values: dict[str, Any] | None = None, **kwargs: Any
    ) -> 'Query[MODEL]':
        ...

    @_builder(applies_to_built_query=True)
    def values(self, values: dict[str, Any] | None = None, **kwargs: Any):
        if values is None and len(kwargs) == 0:
            raise ValueError('No values provided')
        if self._values is None:
            self._values = {}
        if values is not None:
            self._values |= values
        self._values |= kwargs

    async def first(self) -> MODEL | None:
        if self._limit != 1 and self._built_query is None:
            q = self.limit(1)
        else:
            q = self
        built_query = q._build_query()
        row = await FoxOrm.db.fetch_one(built_query, q._values)
        if row is None:
            return None
        return self._model._from_row(row)

    async def all(self) -> list[MODEL]:
        built_query = self._build_query()
        rows = await FoxOrm.db.fetch_all(built_query, self._values)
        return [self._model._from_row(row) for row in rows]


__all__ = ['Query']
