from types import FunctionType, MethodType
from typing import TypeVar, Any, Type

from fox_orm import FoxOrm
from fox_orm.connection import Connection
from fox_orm.exceptions import (
    NoPrimaryKeyError,
    AbstractModelRelationError,
    UnannotatedFieldError,
    PrivateColumnError,
    AbstractModelInstantiationError,
    InvalidColumnError,
    UnboundInstanceError,
    NoSuchColumnError,
)
from fox_orm.internal.table import construct_column
from fox_orm.internal.utils import camel_to_snake
from fox_orm.query import Query, QueryType
from fox_orm.relations import Relation
from sqlalchemy import Column, Table
from sqlalchemy.sql import Delete, Select, ClauseElement


def is_method(obj):
    return isinstance(
        obj,
        (MethodType, FunctionType, property, classmethod, staticmethod),
    )


MODEL = TypeVar('MODEL', bound='OrmModel')


def split_namespace(class_name, orig_namespace):
    annotations = orig_namespace.pop('__annotations__', {})
    namespace = {}
    relations = {}
    field_names = set()
    for k, v in orig_namespace.items():
        if k.startswith('__'):
            if k in ('__qualname__', '__module__'):
                namespace[k] = v
                continue
            raise PrivateColumnError(k)
        if k.startswith(f'_{class_name}__'):
            raise PrivateColumnError(k)

        if isinstance(v, Relation):
            relations[k] = v
        elif not is_method(v):
            if k.startswith('_'):
                raise PrivateColumnError(k)
            field_names.add(k)

    annotations = {k: v for k, v in annotations.items() if k not in relations}
    if invalid_annotations := [x for x in annotations.keys() if x.startswith('_')]:
        raise PrivateColumnError(invalid_annotations[0])
    for column_name in field_names:
        if column_name not in annotations:
            raise UnannotatedFieldError(column_name)
    return annotations, namespace, relations, field_names


def generate_columns(bases, annotations, orig_namespace):
    res = {}
    for base in bases[::-1]:
        if issubclass(base, OrmModel) and base is not OrmModel:
            res.update({x.name: x.copy() for x in base.__columns__.values()})

    for column_name in annotations.keys():
        column_annotation = annotations[column_name]
        column_flags = orig_namespace.get(column_name, ())
        column = construct_column(column_name, column_annotation, column_flags)
        res[column.name] = column
    res.update(res)
    return res


class OrmModelMeta(type):
    def __new__(
        mcs,
        class_name,
        bases,
        orig_namespace,
        table_name: str | None = None,
        abstract: bool = False,
        connection: Connection | str = 'default',
    ):
        if not bases:
            return super().__new__(mcs, class_name, bases, orig_namespace)

        annotations, namespace, relations, field_names = split_namespace(
            class_name, orig_namespace
        )
        columns = generate_columns(bases, annotations, orig_namespace)
        pkeys = [v for k, v in columns.items() if v.primary_key]
        if not pkeys:
            raise NoPrimaryKeyError

        table_name = table_name or camel_to_snake(class_name)

        if abstract and relations:
            raise AbstractModelRelationError

        namespace = {}
        namespace['__abstract__'] = abstract
        namespace['__columns__'] = columns
        namespace['__pkeys__'] = pkeys
        namespace['__relations__'] = relations
        if abstract:
            namespace['__connection__'] = None
            namespace['__table__'] = None
        else:
            if isinstance(connection, str):
                connection = FoxOrm.connections[connection]
            namespace['__connection__'] = connection
            namespace['__table__'] = Table(
                table_name, connection.metadata, *columns.values()
            )

        cls = super().__new__(mcs, class_name, bases, namespace)

        return cls

    def __getattribute__(cls: Type[MODEL], item):
        if item.startswith('__'):
            return super().__getattribute__(item)

        if item == 'c':
            return cls.__table__.c
        if item in cls.__columns__:
            return cls.__table__.columns[item]
        if item in cls.__relations__:
            return cls.__relations__[item]

        return super().__getattribute__(item)


class OrmModel(metaclass=OrmModelMeta):
    # Class attributes
    __abstract__: bool
    __connection__: Connection | None
    __table__: Table | None
    __columns__: dict[str, Column]
    __pkeys__: list[Column]
    __relations__: dict[str, Relation]
    c: Any

    # Instance attributes
    __bound: bool
    __modified: set[str]
    __column_values: dict[str, Any]
    __relation_values: dict[str, Relation]

    def __init__(self, **kwargs):
        if self.__abstract__:
            raise AbstractModelInstantiationError
        self.__column_values = {}
        self.__bound = False
        self.__modified = set()
        for k, v in kwargs.items():
            if k not in self.__columns__:
                raise InvalidColumnError(k)
            self.__column_values[k] = v

    def __getattr__(self, item):
        if item in self.__columns__:
            return self.__column_values.get(item, None)
        if item in self.__relations__:
            return self.__relation_values.get(item, None)
        raise AttributeError

    def __setattr__(self, key, value):
        if key.startswith('_'):
            return super().__setattr__(key, value)

        if key in self.__columns__:
            self.__column_values[key] = value
            self.__modified.add(key)
        else:
            raise NoSuchColumnError(key)

    def flag_modified(self, *args):
        self.__modified.update(args)

    @property
    def __pkey_condition(self):
        self.__ensure_bound()
        conditions = []
        for x in self.__pkeys__:
            conditions.append(x == self.__column_values[x.name])
        res = conditions[0]
        for condition in conditions[1:]:
            res &= condition
        return res

    # TODO: type for row
    @classmethod
    def _from_row(cls, row):
        instance = cls()
        instance.__column_values = row
        instance.__bound = True
        return instance

    def __ensure_bound(self):
        if not self.__bound:
            raise UnboundInstanceError

    async def save(self: MODEL) -> MODEL:
        db = self.__connection__.db
        table = self.__table__
        if self.__bound:
            if not self.__modified:
                return self
            await db.execute(
                table.update().where(self.__pkey_condition),
                {k: v for k, v in self.__column_values.items() if k in self.__modified},
            )
            self.__modified.clear()
        else:
            pkeys = await db.fetch_one(
                table.insert().returning(*type(self).__pkeys__), self.__column_values
            )
            for k, v in pkeys.items():
                self.__column_values[k] = v
            self.__bound = True
        return self

    @classmethod
    def select(cls: Type[MODEL], query=None) -> Query[MODEL]:
        q = Query(cls, QueryType.select)
        if query is not None:
            if isinstance(query, (Select, Delete, str)):
                q = q._set_built_query(query)
            elif isinstance(query, ClauseElement):
                q = q.where(query)
            else:
                raise TypeError('Invalid parameter type')
        return q

    @classmethod
    async def get(cls: Type[MODEL], *args, **kwargs) -> MODEL:
        if kwargs:
            if args:
                raise TypeError(
                    'Positional and keyword arguments cannot be used together'
                )
            if list(kwargs.keys()) != [x.name for x in cls.__pkeys__]:
                raise ValueError('Values passed to .get must be primary keys')
            query = cls.select().where(**kwargs)
        else:
            if len(args) != len(cls.__pkeys__):
                raise ValueError(
                    'Number of values passed to .get must match the number of primary keys'
                )
            query = cls.select()
            for col, val in zip(cls.__pkeys__, args):
                query = query.where(col == val)
        return await query.first()

    @classmethod
    async def get_or_create(cls: Type[MODEL], **kwargs) -> MODEL:
        pkey_names = [x.name for x in cls.__pkeys__]
        pkeys = {k: v for k, v in kwargs.items() if k in pkey_names}
        instance = await cls.get(**pkeys)
        if instance is None:
            instance = await cls(**kwargs).save()
        return instance

    @classmethod
    async def delete_where(cls, *args, **kwargs):
        db = cls.__connection__.db
        table = cls.__table__
        query = table.delete()
        for arg in args:
            query = query.where(arg)
        for k, v in kwargs.items():
            query = query.where(table.c[k] == v)
        return await db.fetch_val(query)

    async def delete(self):
        self.__ensure_bound()
        db = self.__connection__.db
        table = self.__table__
        res = await db.fetch_val(table.delete().where(self.__pkey_condition))
        self.__bound = False
        return res


__all__ = ['OrmModel']
