import asyncio
from abc import abstractmethod, ABC
from typing import Union, Type, TypeVar, Iterator, Optional, List, Generic

from sqlalchemy import and_, select, Table, exists

from fox_orm import FoxOrm
from fox_orm.exceptions import WTFException, NotFetchedException, OrmException
from fox_orm.utils import full_import, OptionalAwaitable

MODEL = TypeVar('MODEL', bound='OrmModel')
RELATION = TypeVar('RELATION', bound='_GenericIterableRelation')


class IdsList(list):
    ids: set

    def __init__(self, items: Optional[List[MODEL]] = None):  # pylint: disable=unsubscriptable-object
        self.ids = set()
        if items is not None:
            super().__init__(items)
            for x in items:
                self.ids.add(x.id)
        else:
            super().__init__()

    def add(self, other: MODEL):
        if other.id in self.ids:
            return
        self.append(other)
        self.ids.add(other.id)

    def delete(self, other: MODEL):
        if other.id not in self.ids:
            return
        for i, x in enumerate(self):
            if x.id == other.id:
                to_delete = i
                break
        else:
            raise WTFException('Object not in list')
        del self[to_delete]
        self.ids.remove(other.id)

    def __contains__(self, item: Union[MODEL, int]):  # pylint: disable=unsubscriptable-object
        if isinstance(item, int):
            return item in self.ids
        return item.id in self.ids

    def __and__(self, other: 'IdsList'):
        result = []
        for x in other:
            if x.id in self.ids:
                result.append(x)
        return result

    def __or__(self, other: 'IdsList'):
        result = []
        ids = set()
        for x in other:
            result.append(x)
            ids.add(x.id)
        for x in self:
            if x.id not in ids:
                result.append(x)
        return result


class _GenericIterableRelation(ABC):
    _fetched: bool
    _initialized: bool
    _objects: IdsList
    __modified__: dict

    model: MODEL

    def __init__(self):
        self._objects = IdsList()
        self.__modified__ = dict()
        self._fetched = False
        self._initialized = False

    @abstractmethod
    def _init_copy(self: RELATION, model: MODEL) -> RELATION:
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
        self.model.ensure_id()
        ids = await self.fetch_ids()
        self._objects = IdsList(await asyncio.gather(*[self.objects_type.get(x) for x in ids]))
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

    @classmethod  # pylint: disable=duplicate-code
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        raise TypeError('Do not set relation field')


class ManyToMany(Generic[MODEL], _GenericIterableRelation):
    to: Union[Type[MODEL], str]  # pylint: disable=unsubscriptable-object
    via: Table
    this_id: str
    other_id: str

    async def _get_entry(self, other_id):
        return await FoxOrm.db.fetch_val(select([exists().where(and_(
            getattr(self.via.c, self.this_id) == self.model.id,
            getattr(self.via.c, self.other_id) == other_id
        ))]))

    # pylint: disable=unsubscriptable-object
    def __init__(self, to: Union[Type[MODEL], str], via: Table, this_id: str, other_id: str):
        self.to = to
        self.via = via
        self.this_id = this_id
        self.other_id = other_id
        super().__init__()

    def _init_copy(self: 'ManyToMany', model: MODEL) -> 'ManyToMany':
        if isinstance(self.to, str):
            self.to = full_import(self.to)
        res = ManyToMany(to=self.to, via=self.via, this_id=self.this_id, other_id=self.other_id)
        res.model = model
        res._initialized = True  # pylint: disable=protected-access
        return res

    @property
    def objects_type(self) -> Type[MODEL]:
        self._raise_if_not_initialized()
        return self.to

    async def fetch_ids(self) -> List[int]:
        return [x[self.other_id] for x in await FoxOrm.db.fetch_all(self.via.select().where(
            getattr(self.via.c, self.this_id) == self.model.id
        ))]

    async def save(self) -> None:
        self.model.ensure_id()
        queries = []
        for k, v in self.__modified__.items():
            entry_exists = await self._get_entry(k)
            if v and not entry_exists:
                queries.append(FoxOrm.db.execute(self.via.insert(), {
                    self.this_id: self.model.id,
                    self.other_id: k
                }))
            elif not v and entry_exists:
                queries.append(FoxOrm.db.execute(self.via.delete().where(and_(
                    getattr(self.via.c, self.this_id) == self.model.id,
                    getattr(self.via.c, self.other_id) == k
                ))))
        await asyncio.gather(*queries)
        self.__modified__ = dict()


class OneToMany(Generic[MODEL], _GenericIterableRelation):
    to: Union[Type[MODEL], str]  # pylint: disable=unsubscriptable-object
    key: str

    async def _get_entry(self, other_id):
        return await FoxOrm.db.fetch_val(select([exists().where(and_(
            getattr(self.to.c, self.key) == self.model.id,
            self.to.c.id == other_id
        ))]))

    # pylint: disable=unsubscriptable-object
    def __init__(self, to: Union[Type[MODEL], str], key: str):
        self.to = to
        self.key = key
        super().__init__()

    def _init_copy(self: 'OneToMany', model: MODEL) -> 'OneToMany':
        if isinstance(self.to, str):
            self.to = full_import(self.to)
        res = OneToMany(to=self.to, key=self.key)
        res.model = model
        res._initialized = True  # pylint: disable=protected-access
        return res

    @property
    def objects_type(self) -> Type[MODEL]:
        self._raise_if_not_initialized()
        return self.to

    async def fetch_ids(self) -> List[int]:
        return [x['id'] for x in await FoxOrm.db.fetch_all(select([self.to.c.id]).where(
            getattr(self.to.c, self.key) == self.model.id
        ))]

    async def save(self) -> None:
        self.model.ensure_id()
        queries = []
        for k, v in self.__modified__.items():
            entry_exists = await self._get_entry(k)
            if v and not entry_exists:
                queries.append(FoxOrm.db.execute(self.to.__sqla_table__.update().where(
                    self.to.c.id == k
                ), {
                    self.key: self.model.id
                }))
            elif not v and entry_exists:
                queries.append(FoxOrm.db.execute(self.to.__sqla_table__.update().where(
                    self.to.c.id == k
                ), {
                    self.key: None
                }))
        await asyncio.gather(*queries)
        self.__modified__ = dict()
