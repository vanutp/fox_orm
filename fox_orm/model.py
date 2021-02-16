from typing import Any, Union, Mapping, TYPE_CHECKING, Dict

from pydantic import BaseModel, PrivateAttr
from pydantic.main import ModelMetaclass
from sqlalchemy import select, func, Table

from fox_orm import FoxOrm
from fox_orm.exceptions import OrmException
from fox_orm.relations import ManyToMany

if TYPE_CHECKING:
    # pylint: disable=no-name-in-module,ungrouped-imports
    from pydantic.typing import MappingIntStrAny, AbstractSetIntStr, TupleGenerator

PRIVATE_ATTRS = {
    '__modified__': set(),
    '__bound__': False,
    '__exclude__': set()
}
EXCLUDE_KEYS = set(PRIVATE_ATTRS.keys())


class OrmModelMeta(ModelMetaclass):
    __sqla_table__: Table

    def _ensure_proper_init(cls):
        if getattr(cls, '__sqla_table__', None) is None:
            raise OrmException('__sqla_table__ must be set')
        if not isinstance(cls.__sqla_table__, Table):
            raise OrmException('__sqla_table__ type should be sqlalchemy.Table')

    def __init__(cls, name, bases, namespace):
        if name == 'OrmModel':
            return
        cls._ensure_proper_init()  # pylint: disable=no-value-for-parameter
        cls.c = cls.__sqla_table__.c
        super().__init__(name, bases, namespace)


class OrmModel(BaseModel, metaclass=OrmModelMeta):
    class Config:
        validate_assignment = True

    __private_attributes__: Dict[str, Any]
    __sqla_table__: Table
    __modified__: set
    __bound__: bool
    __exclude__: set

    def __init__(self, **data: Any) -> None:
        for k, v in PRIVATE_ATTRS.items():
            self.__private_attributes__[k] = PrivateAttr(default=v)
        super().__init__(**data)
        for i in self.__fields__:
            if isinstance(self.__fields__[i].default, ManyToMany):
                self.__exclude__.add(i)
                self.__dict__[i] = self.__fields__[i].default._init_copy(self)
        self.__exclude__.add('id')

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

    def flag_modified(self, attr):
        self.__modified__.add(attr)

    def __setattr__(self, name, value):
        if name == 'id' and self.__bound__:
            raise OrmException('Can not modify id')
        if name not in self.__private_attributes__ and name not in self.__exclude__:
            self.flag_modified(name)
        return super().__setattr__(name, value)

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
    def _generate_query(cls, where, order_by):
        query = cls.__sqla_table__.select()
        if where is not None:
            query = query.where(where)
        if order_by is not None:
            if not isinstance(order_by, list):
                order_by = [order_by]
            for i in order_by:
                query = query.order_by(i)
        return query

    @classmethod
    async def select(cls, where, *, order_by=None):
        res = await FoxOrm.db.fetch_one(cls._generate_query(where, order_by))
        if not res:
            return None
        res = cls.parse_obj(res)
        res.__bound__ = True
        return res

    @classmethod
    async def select_all(cls, where, *, order_by=None):
        q_res = await FoxOrm.db.fetch_all(cls._generate_query(where, order_by))
        res = []
        for x in q_res:
            res.append(cls.parse_obj(x))
            res[-1].__bound__ = True
        return res

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
