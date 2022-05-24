from dataclasses import dataclass
from types import FunctionType, MethodType
from typing import TYPE_CHECKING, TypeVar, Any, Type

from sqlalchemy import Column, Table

from fox_orm import FoxOrm
from fox_orm.connection import Connection
from fox_orm.exceptions import OrmException
from fox_orm.internal.table import construct_column
from fox_orm.internal.utils import camel_to_snake
from fox_orm.relations import Relation


def is_method(obj):
    return isinstance(
        obj,
        (MethodType, FunctionType, property, classmethod, staticmethod),
    )


MODEL = TypeVar('MODEL', bound='OrmModel')


def split_namespace(orig_namespace):
    annotations = orig_namespace.pop('__annotations__', {})
    namespace = {}
    relations = {}
    field_names = set()
    for k, v in orig_namespace.items():
        if k.startswith('__'):
            if k in ('__qualname__', '__module__'):
                namespace[k] = v
                continue
            raise OrmException(f'Fields starting with __ are not allowed (got {k})')

        if isinstance(v, Relation):
            relations[k] = v
        elif not is_method(v):
            field_names.add(k)

    annotations = {k: v for k, v in annotations.items() if k not in relations}
    if invalid_annotations := [x for x in annotations.keys() if x.startswith('__')]:
        raise OrmException(
            f'Fields starting with __ are not allowed (got {invalid_annotations[0]})'
        )
    for column_name in field_names:
        if column_name not in annotations:
            raise OrmException(f'Unannotated field {column_name}')
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

        annotations, namespace, relations, field_names = split_namespace(orig_namespace)
        columns = generate_columns(bases, annotations, orig_namespace)

        table_name = table_name or camel_to_snake(class_name)

        if abstract and relations:
            raise OrmException('Abstract models cannot have relations')

        namespace = {}
        namespace['__abstract__'] = abstract
        namespace['__columns__'] = columns
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
        namespace['__slots__'] = tuple(columns.keys()) + tuple(relations.keys())

        cls = super().__new__(mcs, class_name, bases, namespace)

        return cls

    def __getattribute__(cls: Type[MODEL], item):
        if item.startswith('__'):
            return super().__getattribute__(item)

        if item in cls.__columns__:
            return cls.__table__.columns[item]
        if item in cls.__relations__:
            return cls.__relations__[item]

        return super().__getattribute__(item)


class OrmModel(metaclass=OrmModelMeta):
    if TYPE_CHECKING:
        __abstract__: bool
        __connection__: Connection | None
        __table__: Table | None
        __columns__: dict[str, Column]
        __relations__: dict[str, Relation]


__all__ = ['OrmModel', 'MODEL']
