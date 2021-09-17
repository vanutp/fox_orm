import asyncio
from abc import abstractmethod, ABC
from typing import Union, Type, TypeVar, Iterator, Optional, List, Generic, TYPE_CHECKING

from sqlalchemy import and_, select, Table, exists, MetaData, func

from fox_orm import FoxOrm
from fox_orm.exceptions import NotFetchedException, OrmException
from fox_orm.internal.utils import full_import, OptionalAwaitable

if TYPE_CHECKING:
    from fox_orm.model import OrmModel

MODEL = TypeVar('MODEL', bound='OrmModel')
RELATION = TypeVar('RELATION', bound='_GenericIterableRelation')


class HashList(List[MODEL]):
    map: dict

    def __init__(self, items: Optional[List[MODEL]] = None):  # pylint: disable=unsubscriptable-object
        self.map = {}
        if items is not None:
            super().__init__(items)
            for i, x in enumerate(items):
                self.map[x.pkey_value] = i
        else:
            super().__init__()

    def add(self, other: MODEL):
        if other.pkey_value in self.map:
            return
        self.append(other)
        self.map[other.pkey_value] = len(self) - 1

    def delete(self, other: MODEL):
        if other.pkey_value not in self.map:
            return
        del self[self.map[other.pkey_value]]
        del self.map[other.pkey_value]

    def __contains__(self, item: Optional[Union[MODEL, int]]):  # pylint: disable=unsubscriptable-object
        if item is None:
            return False
        if isinstance(item, int):
            return item in self.map
        return item.pkey_value in self.map

    def __and__(self, other: 'HashList'):
        result = []
        for x in other:
            if x.pkey_value in self.map:
                result.append(x)
        return result

    def __or__(self, other: 'HashList'):
        result = []
        for x in other:
            result.append(x)
        for x in self:
            if x.pkey_value not in other.map:
                result.append(x)
        return result


class _GenericIterableRelation(ABC):
    # FoxOrm.init_relations() called, _from set, _to resolved to class
    _initialized: bool
    _from: 'Type[OrmModel]'
    _to: Union[Type[MODEL], str]  # pylint: disable=unsubscriptable-object
    # Relation bound to model instance
    _copied: bool
    _model: 'OrmModel'
    # Relation objects fetched
    _fetched: bool
    _objects: HashList

    __modified__: dict

    def __init__(self):
        self._objects = HashList()
        self.__modified__ = {}
        self._fetched = False
        self._initialized = False
        self._copied = False

    @abstractmethod
    def _init(self: RELATION, metadata: MetaData, _from: 'Type[OrmModel]'):
        ...

    @abstractmethod
    def _init_copy(self: RELATION, model: 'OrmModel') -> RELATION:
        ...

    @abstractmethod
    async def fetch_ids(self) -> List[int]:
        ...

    @abstractmethod
    async def save(self) -> None:
        ...

    @abstractmethod
    async def count(self) -> int:
        ...

    async def fetch(self) -> None:
        self._check_model_state()
        ids = await self.fetch_ids()
        self._objects = HashList(await asyncio.gather(*[self.objects_type.get(x) for x in ids]))
        self._fetched = True

    def _raise_if_not_initialized(self):
        if not self._initialized:
            raise OrmException('Relation not initialized, call FoxOrm.init_relations() first')

    def _check_model_state(self):
        assert self._copied
        self._model.ensure_id()

    def _raise_if_not_fetched(self):
        self._raise_if_not_initialized()
        if not self._fetched:
            raise NotFetchedException('No values were fetched for this relation, first use .fetch_related()')

    @property
    def objects_type(self) -> Type[MODEL]:
        self._raise_if_not_initialized()
        return self._to

    def add(self, other: MODEL):
        self._raise_if_not_initialized()
        other.ensure_id()
        if not isinstance(other, self.objects_type):
            raise OrmException('other is not instance of target model')
        self._objects.add(other)
        self.__modified__[other.pkey_value] = True
        return OptionalAwaitable(self.save)

    def delete(self, other: MODEL):
        self._raise_if_not_initialized()
        other.ensure_id()
        if not isinstance(other, self.objects_type):
            raise OrmException('other is not instance of target model')
        self._objects.delete(other)
        self.__modified__[other.pkey_value] = False
        return OptionalAwaitable(self.save)

    def __contains__(self, item: Union[MODEL, int]) -> bool:  # pylint: disable=unsubscriptable-object
        self._raise_if_not_fetched()
        return item in self._objects

    def __iter__(self) -> Iterator[MODEL]:
        self._raise_if_not_fetched()
        return self._objects.__iter__()

    def __getitem__(self, item) -> MODEL:
        self._raise_if_not_fetched()
        return self._objects[item]

    def __len__(self) -> int:
        self._raise_if_not_fetched()
        return len(self._objects)

    def __bool__(self) -> int:
        if not self._initialized:
            return True
        self._raise_if_not_fetched()
        return bool(self._objects)

    def __and__(self, other: '_GenericIterableRelation') -> List[MODEL]:
        self._raise_if_not_fetched()
        if not isinstance(other, _GenericIterableRelation):
            raise OrmException('given parameter is not relation')
        if other.objects_type != self.objects_type:
            raise OrmException('given relation\'s objects type is incompatible with this relation\'s type')
        return self._objects & other._objects

    def __or__(self, other: '_GenericIterableRelation') -> List[MODEL]:
        self._raise_if_not_fetched()
        if not isinstance(other, _GenericIterableRelation):
            raise OrmException('given parameter is not relation')
        if other.objects_type != self.objects_type:
            raise OrmException('given relation\'s objects type is incompatible with this relation\'s type')
        return self._objects | other._objects


class ManyToMany(Generic[MODEL], _GenericIterableRelation):
    _via: Table
    _via_name: str
    _this_id: str
    _other_id: str

    async def _get_entry(self, other_id):
        return await FoxOrm.db.fetch_val(select([exists().where(and_(
            getattr(self._via.c, self._this_id) == self._model.pkey_value,
            getattr(self._via.c, self._other_id) == other_id
        ))]))

    # pylint: disable=unsubscriptable-object
    def __init__(self, to: Union[Type[MODEL], str], via: str):
        self._to = to
        self._via_name = via
        super().__init__()

    def _init(self, metadata: MetaData, _from: 'Type[OrmModel]'):
        self._from = _from
        if isinstance(self._to, str):
            self._to = full_import(self._to)
        self._via, self._this_id, self._other_id = \
            FoxOrm.get_assoc_table(metadata, self._from, self._to, self._via_name)
        self._initialized = True

    # pylint: disable=protected-access
    def _init_copy(self: 'ManyToMany', model: 'OrmModel') -> 'ManyToMany':
        self._raise_if_not_initialized()
        res = ManyToMany(to=self._to, via=self._via.name)
        res._model = model
        res._copied = True
        res._initialized = True
        res._via = self._via
        res._from = self._from
        res._this_id = self._this_id
        res._other_id = self._other_id
        return res

    async def fetch_ids(self) -> List[int]:
        self._check_model_state()
        return [x[self._other_id] for x in await FoxOrm.db.fetch_all(self._via.select().where(
            getattr(self._via.c, self._this_id) == self._model.pkey_value
        ))]

    async def count(self) -> int:
        self._check_model_state()
        return await FoxOrm.db.fetch_val(
            select([func.count()]).select_from(self._via).where(
                getattr(self._via.c, self._this_id) == self._model.pkey_value
            )
        )

    async def save(self) -> None:
        self._check_model_state()
        queries = []
        for k, v in self.__modified__.items():
            entry_exists = await self._get_entry(k)
            if v and not entry_exists:
                queries.append(FoxOrm.db.execute(self._via.insert(), {
                    self._this_id: self._model.pkey_value,
                    self._other_id: k
                }))
            elif not v and entry_exists:
                queries.append(FoxOrm.db.execute(self._via.delete().where(and_(
                    getattr(self._via.c, self._this_id) == self._model.pkey_value,
                    getattr(self._via.c, self._other_id) == k
                ))))
        await asyncio.gather(*queries)
        self.__modified__ = {}


class OneToMany(Generic[MODEL], _GenericIterableRelation):
    key: str

    async def _get_entry(self, other_id):
        return await FoxOrm.db.fetch_val(select([exists().where(and_(
            getattr(self._to.c, self.key) == self._model.pkey_value,
            self._to.pkey_column == other_id
        ))]))

    # pylint: disable=unsubscriptable-object
    def __init__(self, to: Union[Type[MODEL], str], key: str):
        self._to = to
        self.key = key
        super().__init__()

    def _init(self, metadata: MetaData, _from: 'Type[OrmModel]'):
        self._from = _from
        if isinstance(self._to, str):
            self._to = full_import(self._to)
        self._initialized = True

    # pylint: disable=protected-access
    def _init_copy(self: 'OneToMany', model: MODEL) -> 'OneToMany':
        self._raise_if_not_initialized()
        res = OneToMany(to=self._to, key=self.key)
        res._model = model
        res._initialized = True
        res._copied = True
        return res

    async def fetch_ids(self) -> List[int]:
        self._check_model_state()
        return [x[self._to.__pkey_name__] for x in await FoxOrm.db.fetch_all(select([self._to.pkey_column]).where(
            getattr(self._to.c, self.key) == self._model.pkey_value
        ))]

    async def count(self) -> int:
        self._check_model_state()
        return await FoxOrm.db.fetch_val(
            select([func.count()]).select_from(self._to.__table__).where(
                getattr(self._to.c, self.key) == self._model.pkey_value
            )
        )

    async def save(self) -> None:
        self._check_model_state()
        queries = []
        for k, v in self.__modified__.items():
            entry_exists = await self._get_entry(k)
            if v and not entry_exists:
                queries.append(FoxOrm.db.execute(self._to.__table__.update().where(
                    self._to.pkey_column == k
                ), {
                    self.key: self._model.pkey_value
                }))
            elif not v and entry_exists:
                queries.append(FoxOrm.db.execute(self._to.__table__.update().where(
                    self._to.pkey_column == k
                ), {
                    self.key: None
                }))
        await asyncio.gather(*queries)
        self.__modified__ = {}


__all__ = ['ManyToMany', 'OneToMany']
