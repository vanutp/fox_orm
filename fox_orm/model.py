from typing import Union, Mapping, TYPE_CHECKING, Dict, Any, TypeVar, List, Type

from pydantic import BaseModel
from pydantic.main import ModelMetaclass
from sqlalchemy import select, func, Table, exists

from fox_orm import FoxOrm
from fox_orm.exceptions import OrmException
from fox_orm.relations import _GenericIterableRelation
from fox_orm.utils import validate_model, class_or_instancemethod

if TYPE_CHECKING:
    # pylint: disable=no-name-in-module,ungrouped-imports
    from pydantic.typing import MappingIntStrAny, AbstractSetIntStr, TupleGenerator, ReprArgs

EXCLUDE_KEYS = {'__modified__', '__bound__', '__exclude__'}

MODEL = TypeVar('MODEL', bound='OrmModel')


class OrmModelMeta(ModelMetaclass):
    __sqla_table__: Table

    def _ensure_proper_init(cls):
        if not hasattr(cls, '__sqla_table__'):
            raise OrmException('__sqla_table__ must be set')
        if not isinstance(cls.__sqla_table__, Table):
            raise OrmException('__sqla_table__ type should be sqlalchemy.Table')

    def __new__(mcs, name, bases, namespace, **kwargs):
        if name == 'OrmModel':
            return super().__new__(mcs, name, bases, namespace, **kwargs)
        new_namespace = {}
        relation_namespace = {}
        exclude = set()
        for k, v in namespace.items():
            if isinstance(v, _GenericIterableRelation):
                relation_namespace[k] = v
                exclude.add(k)
            else:
                new_namespace[k] = v
        new_namespace['__annotations__'] = {}
        for k, v in namespace['__annotations__'].items():
            if k not in exclude:
                new_namespace['__annotations__'][k] = v
        cls = super().__new__(mcs, name, bases, new_namespace, **kwargs)
        cls._ensure_proper_init()
        cls.c = cls.__sqla_table__.c
        cls.__exclude__ = EXCLUDE_KEYS.copy() | exclude
        cls.__relations__ = relation_namespace
        return cls


class OrmModel(BaseModel, metaclass=OrmModelMeta):
    class Config:
        validate_assignment = True

    # cls attrs
    __private_attributes__: Dict[str, Any]
    __sqla_table__: Table
    __exclude__: set

    # instance attrs
    __modified__: set
    __bound__: bool

    def __repr_args__(self) -> 'ReprArgs':
        return [(k, v) for k, v in self.__dict__.items() if k not in self.__exclude__]

    def _init_private_attributes(self):
        self.__modified__ = set()
        self.__bound__ = False
        for k, v in self.__relations__.items():  # pylint: disable=no-member
            self.__dict__[k] = v._init_copy(self)  # pylint: disable=protected-access
        super()._init_private_attributes()

    def __init__(self, **data: Any) -> None:  # pylint: disable=super-init-not-called
        values, fields_set, validation_error = validate_model(self.__class__, data)
        if validation_error:
            raise validation_error  # pylint: disable=raising-bad-type
        object.__setattr__(self, '__dict__', values)
        object.__setattr__(self, '__fields_set__', fields_set)
        self._init_private_attributes()

    # pylint: disable=unsubscriptable-object, too-many-arguments
    def _iter(self, to_dict: bool = False, by_alias: bool = False,
              include: Union['AbstractSetIntStr', 'MappingIntStrAny'] = None,
              exclude: Union['AbstractSetIntStr', 'MappingIntStrAny'] = None, exclude_unset: bool = False,
              exclude_defaults: bool = False, exclude_none: bool = False) -> 'TupleGenerator':
        exclude_private = self.__exclude__ | EXCLUDE_KEYS
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
        if name not in self.__private_attributes__:
            self.flag_modified(name)
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

    async def save(self):
        if self.__bound__:
            self.ensure_id()
            if not self.__modified__:
                return
            table = self.__class__.__sqla_table__
            fields = {k: getattr(self, k) for k in self.__modified__}
            # pylint: disable=access-member-before-definition
            await FoxOrm.db.execute(table.update().where(table.c.id == self.id), fields)
            self.__modified__ = set()
        else:
            table = self.__class__.__sqla_table__
            data = self.dict(exclude={'id'})
            if len(data) == 0:
                data['id'] = None
            if self.id is not None:
                data['id'] = self.id
            self.id = await FoxOrm.db.execute(table.insert(), data)  # pylint: disable=attribute-defined-outside-init
            self.__bound__ = True

    @classmethod
    def _generate_query(cls, where, order_by, limit, offset):
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
    async def select(cls: Type[MODEL], where, *, order_by=None, skip_parsing=False) -> MODEL:
        construct_func = cls.construct if skip_parsing else cls.parse_obj
        res = await FoxOrm.db.fetch_one(cls._generate_query(where, order_by, None, None))
        if not res:
            return None
        res = construct_func(res)
        res.__bound__ = True
        return res

    @classmethod
    async def select_all(cls: Type[MODEL], where, *, order_by=None, limit=None, offset=None, skip_parsing=False) -> \
            List[MODEL]:
        construct_func = cls.construct if skip_parsing else cls.parse_obj
        q_res = await FoxOrm.db.fetch_all(cls._generate_query(where, order_by, limit, offset))
        res = []
        for x in q_res:
            res.append(construct_func(x))
            res[-1].__bound__ = True
        return res

    @classmethod
    async def exists(cls: Type[MODEL], where) -> bool:
        query = cls._generate_query(where, None, None, None)
        query = exists(query).select()
        return await FoxOrm.db.fetch_val(query)

    @classmethod
    async def _delete_cls(cls, where):
        query = cls.__sqla_table__.delete().where(where)
        await FoxOrm.db.execute(query)

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
    async def count(cls: Type[MODEL], where) -> int:
        query = select([func.count()]).select_from(cls.__sqla_table__)
        if where is not None:
            query = query.where(where)
        return await FoxOrm.db.fetch_val(query)

    @classmethod
    async def get(cls: Type[MODEL], obj_id: int, skip_parsing=False) -> MODEL:
        return await cls.select(cls.__sqla_table__.c.id == obj_id, skip_parsing=skip_parsing)

    async def _fetch_related(self, field: str):
        self.ensure_id()
        relation: _GenericIterableRelation = getattr(self, field)
        if not isinstance(relation, _GenericIterableRelation):
            raise OrmException('fetch_related argument is not a relation')
        await relation.fetch()

    async def fetch_related(self, *fields: str) -> None:
        for field in fields:
            await self._fetch_related(field)


__all__ = ['OrmModel']
