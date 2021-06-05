import asyncio
from typing import Union, Mapping, TYPE_CHECKING, Dict, Any, TypeVar, List, Type, Optional

from pydantic import BaseModel
from pydantic.main import ModelMetaclass
from sqlalchemy import select, func, Table, exists, Column
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.elements import ColumnElement

from fox_orm import FoxOrm
from fox_orm.exceptions import OrmException
from fox_orm.internal import FieldType
from fox_orm.internal.table import construct_column
from fox_orm.relations import _GenericIterableRelation
from fox_orm.internal.utils import class_or_instancemethod, camel_to_snake

if TYPE_CHECKING:
    # pylint: disable=no-name-in-module,ungrouped-imports
    from pydantic.typing import MappingIntStrAny, AbstractSetIntStr, TupleGenerator, ReprArgs

EXCLUDE_KEYS = {'__modified__', '__bound__', '__exclude__'}

MODEL = TypeVar('MODEL', bound='OrmModel')

def is_valid_column(name):
    return not (name.startswith('_') or name == 'Config')

class OrmModelMeta(ModelMetaclass):
    if TYPE_CHECKING:
        __sqla_table__: Table

    @classmethod
    def _ensure_proper_init(mcs, namespace):
        if '__sqla_table__' in namespace:
            raise OrmException('You are using pre 0.3 model syntax. Check the docs for new instructions')
        if '__table__' in namespace and not isinstance(namespace['__table__'], str):
            raise OrmException('__table__ must be of type str')

    def __new__(mcs, name, bases, namespace, **kwargs):
        if bases[0] == BaseModel:
            return super().__new__(mcs, name, bases, namespace, **kwargs)

        mcs._ensure_proper_init(namespace)

        new_namespace = {}
        relation_namespace = {}
        for k, v in namespace.items():
            if k == '__table__':
                continue
            if isinstance(v, _GenericIterableRelation):
                relation_namespace[k] = v
            else:
                new_namespace[k] = v

        new_namespace['__annotations__'] = annotations = {}
        for k, v in namespace['__annotations__'].items():
            if k in relation_namespace:
                continue
            annotations[k] = v

        columns = []
        processed_columns = set()
        table_name = namespace.get('__table__', None) or camel_to_snake(name)
        for column_name in new_namespace:
            if not is_valid_column(column_name):
                continue
            if column_name not in annotations:
                raise OrmException(f'Unannotated field {column_name}')
            column, value = construct_column(column_name, annotations[column_name], new_namespace[column_name])
            columns.append(column)
            new_namespace[column_name] = value
            processed_columns.add(column_name)
        for column_name, annotation in annotations.items():
            if not is_valid_column(column_name):
                continue
            if column_name not in processed_columns:
                columns.append(construct_column(column_name, annotations[column_name], tuple())[0])
        table = Table(table_name, FoxOrm.metadata, *columns)

        new_namespace['__sqla_table__'] = table
        new_namespace['c'] = table.c
        new_namespace['__relations__'] = relation_namespace

        cls = super().__new__(mcs, name, bases, new_namespace, **kwargs)
        return cls


class OrmModel(BaseModel, metaclass=OrmModelMeta):
    class Config:
        validate_assignment = True

    if TYPE_CHECKING:
        # cls attrs
        __private_attributes__: Dict[str, Any]
        __sqla_table__: Table
        __relations__: dict

        # instance attrs
        __modified__: set
        __bound__: bool

    __class_vars__ = {'c'}

    __slots__ = ('__fields_set__', '__modified__', '__bound__')

    def __repr_args__(self) -> 'ReprArgs':
        exclude = EXCLUDE_KEYS | set(self.__relations__.keys())
        return [(k, v) for k, v in self.__dict__.items() if k not in exclude]

    def _init_private_attributes(self):
        self.__modified__ = set()
        self.__bound__ = False
        for k, v in self.__relations__.items():  # pylint: disable=no-member
            self.__dict__[k] = v._init_copy(self)  # pylint: disable=protected-access
        super()._init_private_attributes()

    # pylint: disable=unsubscriptable-object, too-many-arguments
    def _iter(self, to_dict: bool = False, by_alias: bool = False,
              include: Union['AbstractSetIntStr', 'MappingIntStrAny'] = None,
              exclude: Union['AbstractSetIntStr', 'MappingIntStrAny'] = None, exclude_unset: bool = False,
              exclude_defaults: bool = False, exclude_none: bool = False) -> 'TupleGenerator':
        exclude_private = EXCLUDE_KEYS
        if exclude is None:
            exclude = exclude_private
        elif isinstance(exclude, Mapping):  # pylint: disable=isinstance-second-argument-not-valid-type
            exclude = dict(exclude)
            exclude.update({k: ... for k in exclude_private})
        else:
            exclude |= exclude_private
        return super()._iter(to_dict, by_alias, include, exclude, exclude_unset, exclude_defaults, exclude_none)

    def __setattr__(self, name, value):
        if name == 'id':
            if self.__bound__:
                raise OrmException('Can not modify id')
            return super().__setattr__(name, value)
        if name in EXCLUDE_KEYS:
            return object.__setattr__(self, name, value)
        if name not in self.__private_attributes__ and name in self.__fields__:
            self.flag_modified(name)
        if name in self.__relations__:
            raise ValueError('Do not set relation field')
        return super().__setattr__(name, value)

    @classmethod
    def construct(cls, values):  # pylint: disable=arguments-differ
        m = cls.__new__(cls)
        fields_values = {name: field.get_default() for name, field in cls.__fields__.items() if
                         not field.required and field.name not in cls.__exclude__}
        fields_values.update(values)
        object.__setattr__(m, '__dict__', fields_values)
        object.__setattr__(m, '__fields_set__', set(values.keys()))
        m._init_private_attributes()  # pylint: disable=protected-access
        return m

    def flag_modified(self, attr):
        self.__modified__.add(attr)

    def ensure_id(self):
        if not self.__bound__:
            raise OrmException('Object is not bound to db, execute insert first')
        if getattr(self, 'id', None) is None:
            raise OrmException('id must be set')

    # pylint: disable=access-member-before-definition
    async def save(self):
        if self.__bound__:
            self.ensure_id()
            if not self.__modified__:
                return
            table = self.__class__.__sqla_table__
            fields = self.dict(include=self.__modified__)
            # pylint: disable=access-member-before-definition
            await FoxOrm.db.execute(table.update().where(table.c.id == self.id), fields)
            self.__modified__.clear()
        else:
            table = self.__class__.__sqla_table__
            data = self.dict(exclude={'id'}, include=self.__fields__.keys())
            if len(data) == 0:
                data['id'] = None
            if self.id is not None:
                data['id'] = self.id
            self.id = await FoxOrm.db.execute(table.insert(), data)  # pylint: disable=attribute-defined-outside-init
            self.__bound__ = True

    @classmethod
    def _generate_query(cls, where, order_by, limit, offset):
        if isinstance(where, str):
            return where
        if isinstance(where, ClauseElement) and not isinstance(where, ColumnElement):
            query = where
        else:
            query = cls.__sqla_table__.select()
            if where is not None:
                query = query.where(where)
        if order_by is not None:
            if not isinstance(order_by, list):
                order_by = [order_by]
            for i in order_by:
                query = query.order_by(i)
        if limit is not None:
            query = query.limit(limit)
        if offset is not None:
            query = query.offset(offset)
        return query

    @classmethod
    async def select(cls: Type[MODEL], where, values: dict = None, *,
                     order_by=None, skip_parsing=False) -> Optional[MODEL]:
        construct_func = cls.construct if skip_parsing else cls.parse_obj
        res = await FoxOrm.db.fetch_one(cls._generate_query(where, order_by, None, None), values)
        if not res:
            return None
        res = construct_func(res)
        res.__bound__ = True
        return res

    @classmethod
    async def select_all(cls: Type[MODEL], where, values: dict = None, *,
                         order_by=None, limit=None, offset=None, skip_parsing=False) -> List[MODEL]:
        construct_func = cls.construct if skip_parsing else cls.parse_obj
        q_res = await FoxOrm.db.fetch_all(cls._generate_query(where, order_by, limit, offset), values)
        res = []
        for x in q_res:
            res.append(construct_func(x))
            res[-1].__bound__ = True
        return res

    @classmethod
    async def exists(cls: Type[MODEL], where, values: dict = None) -> bool:
        query = cls._generate_query(where, None, None, None)
        query = exists(query).select()
        return await FoxOrm.db.fetch_val(query, values)

    @classmethod
    async def _delete_cls(cls, where, values: dict = None):
        query = cls.__sqla_table__.delete().where(where)
        await FoxOrm.db.execute(query, values)

    async def _delete_inst(self):
        self.ensure_id()
        table = self.__class__.__sqla_table__
        query = table.delete().where(table.c.id == self.id)
        self.__bound__ = False
        await FoxOrm.db.execute(query)

    # pylint: disable=bad-classmethod-argument,no-else-return
    @class_or_instancemethod
    async def delete(self_or_cls, *args, **kwargs) -> None:
        if isinstance(self_or_cls, type):
            return await self_or_cls._delete_cls(*args, **kwargs)
        else:
            return await self_or_cls._delete_inst(*args, **kwargs)

    @classmethod
    async def count(cls: Type[MODEL], where, values: dict = None) -> int:
        query = select([func.count()]).select_from(cls.__sqla_table__)
        if where is not None:
            query = query.where(where)
        return await FoxOrm.db.fetch_val(query, values)

    @classmethod
    async def get(cls: Type[MODEL], obj_id: int, skip_parsing=False) -> MODEL:
        return await cls.select(cls.__sqla_table__.c.id == obj_id, skip_parsing=skip_parsing)

    async def fetch_related(self, *fields: str) -> None:
        self.ensure_id()
        tasks = []
        for field in fields:
            relation: _GenericIterableRelation = getattr(self, field)
            if not isinstance(relation, _GenericIterableRelation):
                raise OrmException('fetch_related argument is not a relation')
            tasks.append(relation.fetch())
        await asyncio.gather(*tasks)


__all__ = ['OrmModel']
