import asyncio
from abc import abstractmethod, ABC
from typing import Union, Type, TypeVar, Iterator, Optional, List, Generic, TYPE_CHECKING

from sqlalchemy import and_, select, Table, exists, MetaData

from fox_orm import FoxOrm
from fox_orm.exceptions import WTFException, NotFetchedException, OrmException
from fox_orm.internal.utils import full_import, OptionalAwaitable

if TYPE_CHECKING:
    from fox_orm.model import OrmModel

MODEL = TypeVar('MODEL', bound='OrmModel')
RELATION = TypeVar('RELATION', bound='_GenericIterableRelation')


class HashList(list):
    map: dict

    def __init__(self, items: Optional[List[MODEL]] = None):  # pylint: disable=unsubscriptable-object
        self.map = dict()
        if items is not None:
            super().__init__(items)
            for i, x in enumerate(items):
                self.map[x.id] = i
        else:
            super().__init__()

    def add(self, other: MODEL):
        if other.id in self.map:
            return
        self.append(other)
        self.map[other.id] = len(self) - 1

    def delete(self, other: MODEL):
        if other.id not in self.map:
            return
        del self[self.map[other.id]]
        del self.map[other.id]

    def __contains__(self, item: Optional[Union[MODEL, int]]):  # pylint: disable=unsubscriptable-object
        if item is None:
            return False
        if isinstance(item, int):
            return item in self.map
        return item.id in self.map

    def __and__(self, other: 'HashList'):
        result = []
        for x in other:
            if x.id in self.map:
                result.append(x)
        return result

    def __or__(self, other: 'HashList'):
        result = []
        for x in other:
            result.append(x)
        for x in self:
            if x.id not in other.map:
                result.append(x)
        return result


class _GenericIterableRelation(ABC):
    _fetched: bool
    _initialized: bool
    _objects: HashList
    __modified__: dict

    _model: 'OrmModel'

    def __init__(self):
        self._objects = HashList()
        self.__modified__ = dict()
        self._fetched = False
        self._initialized = False

    @abstractmethod
    def _init(self: RELATION, metadata: MetaData, _from: 'Type[OrmModel]'):
        ...

    @abstractmethod
    def _init_copy(self: RELATION, model: 'OrmModel') -> RELATION:
        ...

    @abstractmethod
    async def fetch_ids(self) -> List[int]:
        ...

    @property
    @abstractmethod
    def objects_type(self) -> Type[MODEL]:
        ...

    @abstractmethod
    async def save(self) -> None:
        ...

    async def fetch(self) -> None:
        self._raise_if_not_initialized()
        self._model.ensure_id()
        ids = await self.fetch_ids()
        self._objects = HashList(await asyncio.gather(*[self.objects_type.get(x) for x in ids]))
        self._fetched = True

    def _raise_if_not_initialized(self):
        if not self._initialized:
            raise WTFException('Relation not initialized')

    def _raise_if_not_fetched(self):
        self._raise_if_not_initialized()
        if not self._fetched:
            raise NotFetchedException('No values were fetched for this relation, first use .fetch_related()')

    def add(self, other: MODEL):
        self._raise_if_not_initialized()
        other.ensure_id()
        if not isinstance(other, self.objects_type):
            raise OrmException('other is not instance of target model')
        self._objects.add(other)
        self.__modified__[other.id] = True
        return OptionalAwaitable(self.save)

    def delete(self, other: MODEL):
        self._raise_if_not_initialized()
        other.ensure_id()
        if not isinstance(other, self.objects_type):
            raise OrmException('other is not instance of target model')
        self._objects.delete(other)
        self.__modified__[other.id] = False
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
    _from: 'Type[OrmModel]'
    _to: Union[Type[MODEL], str]  # pylint: disable=unsubscriptable-object
    _via: Table
    _via_name: str
    _this_id: str
    _other_id: str

    async def _get_entry(self, other_id):
        return await FoxOrm.db.fetch_val(select([exists().where(and_(
            getattr(self._via.c, self._this_id) == self._model.id,
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

    # pylint: disable=protected-access
    def _init_copy(self: 'ManyToMany', model: 'OrmModel') -> 'ManyToMany':
        res = ManyToMany(to=self._to, via=self._via)
        res._model = model
        res._initialized = True
        res._via = self._via
        res._from = self._from
        res._this_id = self._this_id
        res._other_id = self._other_id
        return res

    @property
    def objects_type(self) -> Type[MODEL]:
        self._raise_if_not_initialized()
        return self._to

    async def fetch_ids(self) -> List[int]:
        self._raise_if_not_initialized()
        return [x[self._other_id] for x in await FoxOrm.db.fetch_all(self._via.select().where(
            getattr(self._via.c, self._this_id) == self._model.id
        ))]

    async def save(self) -> None:
        self._model.ensure_id()
        self._raise_if_not_initialized()
        queries = []
        for k, v in self.__modified__.items():
            entry_exists = await self._get_entry(k)
            if v and not entry_exists:
                queries.append(FoxOrm.db.execute(self._via.insert(), {
                    self._this_id: self._model.id,
                    self._other_id: k
                }))
            elif not v and entry_exists:
                queries.append(FoxOrm.db.execute(self._via.delete().where(and_(
                    getattr(self._via.c, self._this_id) == self._model.id,
                    getattr(self._via.c, self._other_id) == k
                ))))
        await asyncio.gather(*queries)
        self.__modified__ = dict()


class OneToMany(Generic[MODEL], _GenericIterableRelation):
    to: Union[Type[MODEL], str]  # pylint: disable=unsubscriptable-object
    key: str
    _from: 'Type[OrmModel]'

    async def _get_entry(self, other_id):
        return await FoxOrm.db.fetch_val(select([exists().where(and_(
            getattr(self.to.c, self.key) == self._model.id,
            self.to.c.id == other_id
        ))]))

    # pylint: disable=unsubscriptable-object
    def __init__(self, to: Union[Type[MODEL], str], key: str):
        self.to = to
        self.key = key
        super().__init__()

    def _init(self, metadata: MetaData, _from: 'Type[OrmModel]'):
        self._from = _from
        if isinstance(self.to, str):
            self.to = full_import(self.to)

    def _init_copy(self: 'OneToMany', model: MODEL) -> 'OneToMany':
        res = OneToMany(to=self.to, key=self.key)
        res._model = model  # pylint: disable=protected-access
        res._initialized = True  # pylint: disable=protected-access
        return res

    @property
    def objects_type(self) -> Type[MODEL]:
        self._raise_if_not_initialized()
        return self.to

    async def fetch_ids(self) -> List[int]:
        return [x['id'] for x in await FoxOrm.db.fetch_all(select([self.to.c.id]).where(
            getattr(self.to.c, self.key) == self._model.id
        ))]

    async def save(self) -> None:
        self._model.ensure_id()
        queries = []
        for k, v in self.__modified__.items():
            entry_exists = await self._get_entry(k)
            if v and not entry_exists:
                queries.append(FoxOrm.db.execute(self.to.__table__.update().where(
                    self.to.c.id == k
                ), {
                    self.key: self._model.id
                }))
            elif not v and entry_exists:
                queries.append(FoxOrm.db.execute(self.to.__table__.update().where(
                    self.to.c.id == k
                ), {
                    self.key: None
                }))
        await asyncio.gather(*queries)
        self.__modified__ = dict()


__all__ = ['ManyToMany', 'OneToMany']
