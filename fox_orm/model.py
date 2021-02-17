from typing import Union, Mapping, TYPE_CHECKING

from pydantic import BaseModel
from pydantic.main import ModelMetaclass
from sqlalchemy import select, func, Table, exists

from fox_orm import FoxOrm
from fox_orm.exceptions import OrmException
from fox_orm.relations import ManyToMany

if TYPE_CHECKING:
    # pylint: disable=no-name-in-module,ungrouped-imports
    from pydantic.typing import MappingIntStrAny, AbstractSetIntStr, TupleGenerator, ReprArgs

EXCLUDE_KEYS = {'__modified__', '__bound__', '__exclude__'}


class OrmModelMeta(ModelMetaclass):
    __sqla_table__: Table

    def _ensure_proper_init(cls):
        if not hasattr(cls, '__sqla_table__'):
            raise OrmException('__sqla_table__ must be set')
        if not isinstance(cls.__sqla_table__, Table):
            raise OrmException('__sqla_table__ type should be sqlalchemy.Table')

    def __init__(cls, name, bases, namespace):
        if name == 'OrmModel':
            return
        cls._ensure_proper_init()  # pylint: disable=no-value-for-parameter
        cls.c = cls.__sqla_table__.c

        cls.__exclude__ = EXCLUDE_KEYS.copy()

        super().__init__(name, bases, namespace)
        for i in cls.__fields__:
            if isinstance(cls.__fields__[i].default, ManyToMany):
                cls.__exclude__.add(i)
        cls.__exclude__.add('id')


class OrmModel(BaseModel, metaclass=OrmModelMeta):
    class Config:
        validate_assignment = True

    # cls attrs
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
        for i in self.__fields__:
            if isinstance(self.__fields__[i].default, ManyToMany):
                self.__dict__[i] = self.__fields__[i].default._init_copy(self)  # pylint: disable=protected-access
        super()._init_private_attributes()

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
    def parse_obj(cls, *args, **kwargs):
        raise OrmException('Do not use parse_obj with OrmModel')

    @classmethod
    def construct(cls, values):
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
            data = self.dict()
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
    async def select(cls, where, *, order_by=None):
        res = await FoxOrm.db.fetch_one(cls._generate_query(where, order_by, None, None))
        if not res:
            return None
        res = cls.construct(res)
        res.__bound__ = True
        return res

    @classmethod
    async def select_all(cls, where, *, order_by=None, limit=None, offset=None):
        q_res = await FoxOrm.db.fetch_all(cls._generate_query(where, order_by, limit, offset))
        res = []
        for x in q_res:
            res.append(cls.construct(dict(x)))
            res[-1].__bound__ = True
        return res

    @classmethod
    async def exists(cls, where):
        query = cls._generate_query(where, None, None, None)
        query = exists(query).select()
        return await FoxOrm.db.fetch_val(query)

    @classmethod
    async def delete(cls, where):
        query = cls.__sqla_table__.delete().where(where)
        return await FoxOrm.db.execute(query)

    @classmethod
    async def count(cls, where):
        query = select([func.count()]).select_from(cls.__sqla_table__)
        if where is not None:
            query = query.where(where)
        return await FoxOrm.db.fetch_val(query)

    @classmethod
    async def get(cls, obj_id: int):
        return await cls.select(cls.__sqla_table__.c.id == obj_id)

    async def _fetch_related(self, field: str):
        self.ensure_id()
        relation: ManyToMany = getattr(self, field)
        if not isinstance(relation, ManyToMany):
            raise OrmException('fetch_related argument is not a relation')
        await relation.fetch()

    async def fetch_related(self, *fields: str):
        for field in fields:
            await self._fetch_related(field)


__all__ = ['OrmModel']
