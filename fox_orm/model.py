import asyncio
from typing import Union, Mapping, TYPE_CHECKING, Dict, Any, TypeVar, List, Type, Optional

from pydantic import BaseModel
from pydantic.main import ModelMetaclass, UNTOUCHED_TYPES
from sqlalchemy import select, func, Table, exists, MetaData, Column
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.elements import ColumnElement

from fox_orm import FoxOrm
from fox_orm.exceptions import OrmException
from fox_orm.internal.const import EXCLUDE_KEYS
from fox_orm.internal.table import construct_column
from fox_orm.internal.utils import class_or_instancemethod, camel_to_snake, validate_model
from fox_orm.relations import _GenericIterableRelation

if TYPE_CHECKING:
    # pylint: disable=no-name-in-module,ungrouped-imports
    from pydantic.typing import MappingIntStrAny, AbstractSetIntStr, TupleGenerator, ReprArgs

MODEL = TypeVar('MODEL', bound='OrmModel')


def is_valid_column_name(name: str) -> bool:
    return not (name.startswith('_') or name == 'Config')


def is_valid_column_value(v: Any) -> bool:
    # pylint: disable=import-outside-toplevel
    from fox_orm.internal.columns import FieldType

    is_untouched = isinstance(v, UNTOUCHED_TYPES) or v.__class__.__name__ == 'cython_function_or_method'
    is_field_type = isinstance(v, type) and issubclass(v, FieldType)
    return not is_untouched or is_field_type


class OrmModelMeta(ModelMetaclass):
    if TYPE_CHECKING:
        __columns__: List[Column]
        __table__: Table
        __tablename__: str
        __metadata__: MetaData
        __abstract__: bool
        __pkey_name__: str

    @property
    def pkey_column(cls):
        return getattr(cls.__table__.c, cls.__pkey_name__)

    @classmethod
    def _check_type(mcs, namespace: dict, key: str, expected_type: type):
        if key in namespace and not isinstance(namespace[key], expected_type):
            raise OrmException(f'{key} must be of type {expected_type.__qualname__}')

    @classmethod
    def _ensure_proper_init(mcs, namespace):
        if '__sqla_table__' in namespace:
            raise OrmException('You are using pre 0.3 model syntax. Check the docs for new instructions')
        if '__table__' in namespace:
            raise OrmException('__table__ should not be set')
        mcs._check_type(namespace, '__tablename__', str)
        mcs._check_type(namespace, '__metadata__', MetaData)
        mcs._check_type(namespace, '__abstract__', bool)

    def __new__(mcs, name, bases, namespace, **kwargs):
        if bases[0] == BaseModel:
            return super().__new__(mcs, name, bases, namespace, **kwargs)

        inherited_columns = {}
        for base in bases[::-1]:
            if issubclass(base, OrmModel) and base != OrmModel:
                inherited_columns.update({x.name: x.copy() for x in base.__columns__})

        mcs._ensure_proper_init(namespace)

        table_name = namespace.get('__tablename__', None) or camel_to_snake(name)
        metadata = namespace.get('__metadata__', None) or FoxOrm.metadata
        abstract = namespace.get('__abstract__', None) or False

        new_namespace = {}
        relation_namespace = {}
        for k, v in namespace.items():
            if k == '__tablename__':
                continue
            if isinstance(v, _GenericIterableRelation):
                relation_namespace[k] = v
            else:
                new_namespace[k] = v

        new_namespace['__annotations__'] = annotations = {}
        for k, v in namespace.get('__annotations__', {}).items():
            if k in relation_namespace:
                continue
            annotations[k] = v

        columns = {}
        for column_name, namespace_value in new_namespace.items():
            if not is_valid_column_name(column_name) or not is_valid_column_value(namespace_value):
                continue
            if column_name not in annotations:
                raise OrmException(f'Unannotated field {column_name}')
            column, value = construct_column(column_name, annotations[column_name], namespace_value)
            columns[column.name] = column
            new_namespace[column_name] = value
        for column_name, annotation in annotations.items():
            if not is_valid_column_name(column_name):
                continue
            if column_name not in columns:
                column, _ = construct_column(column_name, annotation, tuple())
                columns[column.name] = column
        all_columns = list(inherited_columns.values())
        all_columns.extend(columns.values())

        new_namespace['__abstract__'] = abstract
        new_namespace['__columns__'] = all_columns
        if abstract:
            new_namespace['__pkey_name__'] = None
            new_namespace['__table__'] = None
            new_namespace['c'] = None
        else:
            if sum([x.primary_key for x in all_columns]) != 1:
                raise OrmException('Model should have exactly one primary key')
            new_namespace['__pkey_name__'] = [x.name for x in all_columns if x.primary_key][0]
            new_namespace['__table__'] = table = Table(table_name, metadata, *all_columns)
            new_namespace['c'] = table.c
        new_namespace['__relations__'] = relation_namespace

        cls = super().__new__(mcs, name, bases, new_namespace, **kwargs)
        if not abstract:
            for rel in relation_namespace.values():
                FoxOrm._lazyinit_relation(metadata, rel, cls)
        return cls


class OrmModel(BaseModel, metaclass=OrmModelMeta):
    class Config:
        validate_assignment = True

    if TYPE_CHECKING:
        # cls attrs
        __private_attributes__: Dict[str, Any]
        __columns__: List[Column]
        __table__: Table
        __relations__: dict
        __tablename__: str
        __metadata__: MetaData
        __abstract__: bool
        __pkey_name__: str

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

    # noinspection PyMissingConstructor
    def __init__(self, **data: Any) -> None:  # pylint: disable=super-init-not-called
        if self.__abstract__:
            raise OrmException('Can\'t instantiate abstract model')
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
        exclude_private = EXCLUDE_KEYS
        if exclude is None:
            exclude = exclude_private
        elif isinstance(exclude, Mapping):  # pylint: disable=isinstance-second-argument-not-valid-type
            exclude = dict(exclude)
            exclude.update({k: ... for k in exclude_private})
        else:
            exclude |= exclude_private
        return super()._iter(to_dict, by_alias, include, exclude, exclude_unset, exclude_defaults, exclude_none)

    @property
    def pkey_column(self):
        return getattr(self.__class__.__table__.c, self.__pkey_name__)

    @property
    def pkey_value(self):
        return getattr(self, self.__pkey_name__)

    @pkey_value.setter
    def pkey_value(self, value):
        setattr(self, self.__pkey_name__, value)

    def __setattr__(self, name, value):
        if name == self.__pkey_name__:
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

    # noinspection PyMethodOverriding
    # pylint: disable=arguments-differ
    @classmethod
    def construct(cls, values):
        m = cls.__new__(cls)
        if m.__abstract__:
            raise OrmException('Can\'t instantiate abstract model')
        fields_values = {name: field.get_default() for name, field in cls.__fields__.items() if
                         not field.required and field.name not in EXCLUDE_KEYS}
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
        assert getattr(self, self.__pkey_name__, None) is not None

    # pylint: disable=access-member-before-definition
    async def save(self) -> MODEL:
        table = self.__table__
        pkey_name = self.__pkey_name__
        if self.__bound__:
            self.ensure_id()
            if not self.__modified__:
                return self
            fields = self.dict(include=self.__modified__)
            # pylint: disable=access-member-before-definition
            await FoxOrm.db.execute(
                table.update().where(self.pkey_column == self.pkey_value),
                fields
            )
            self.__modified__.clear()
        else:
            data = self.dict(exclude={pkey_name}, include=self.__fields__.keys())
            if len(data) == 0:
                data[pkey_name] = None
            if self.pkey_value is not None:
                data[pkey_name] = self.pkey_value
            # pylint: disable=attribute-defined-outside-init
            self.pkey_value = await FoxOrm.db.fetch_val(table.insert().returning(self.pkey_column), data)
            self.__bound__ = True
        return self

    @classmethod
    def _generate_query(cls, where, order_by, limit, offset):
        if isinstance(where, str):
            return where
        if isinstance(where, ClauseElement) and not isinstance(where, ColumnElement):
            query = where
        else:
            query = cls.__table__.select()
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
    async def select_all(cls: Type[MODEL], where=None, values: dict = None, *,
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
        query = cls.__table__.delete().where(where)
        await FoxOrm.db.execute(query, values)

    async def _delete_inst(self):
        self.ensure_id()
        table = self.__table__
        query = table.delete().where(self.pkey_column == self.pkey_value)
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
        query = select([func.count()]).select_from(cls.__table__)
        if where is not None:
            query = query.where(where)
        return await FoxOrm.db.fetch_val(query, values)

    @classmethod
    async def get(cls: Type[MODEL], obj_id: int, skip_parsing=False) -> Optional[MODEL]:
        # false positive
        # pylint: disable=comparison-with-callable
        return await cls.select(cls.pkey_column == obj_id, skip_parsing=skip_parsing)

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
