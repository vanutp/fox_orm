import asyncio
from typing import Union, Type, TypeVar, Generic, Iterator, Optional, List

from sqlalchemy import and_, select, Table, exists

from fox_orm import FoxOrm
from fox_orm.exceptions import WTFException, NotFetchedException, OrmException
from fox_orm.utils import full_import, OptionalAwaitable

MODEL = TypeVar('MODEL', bound='OrmModel')


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


class ManyToMany(Generic[MODEL]):
    _fetched: bool
    _initialized: bool
    _objects: IdsList

    model: MODEL
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
        self._objects = IdsList()
        self.__modified__ = dict()
        self.to = to
        self.via = via
        self.this_id = this_id
        self.other_id = other_id
        self._fetched = False
        self._initialized = False

    def _init_copy(self, model: MODEL):
        res = ManyToMany(to=self.to, via=self.via, this_id=self.this_id, other_id=self.other_id)
        if isinstance(self.to, str):
            self.to = full_import(self.to)
        res.model = model
        res._initialized = True  # pylint: disable=protected-access
        return res

    async def fetch(self):
        self._raise_if_not_initialized()
        self.model.ensure_id()
        ids = await FoxOrm.db.fetch_all(self.via.select().where(
            getattr(self.via.c, self.this_id) == self.model.id
        ))
        self._objects = IdsList(await asyncio.gather(*[self.to.get(x[self.other_id]) for x in ids]))
        self._fetched = True

    def _raise_if_not_initialized(self):
        if not self._initialized:
            raise WTFException('Relation not initialized')

    def _raise_if_not_fetched(self):
        self._raise_if_not_initialized()
        if not self._fetched:
            raise NotFetchedException('No values were fetched for this relation, first use .fetch_related()')

    async def save(self):
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

    def add(self, other: MODEL):
        self._raise_if_not_initialized()
        other.ensure_id()
        if not isinstance(other, self.to):
            raise OrmException('other is not instance of target model')
        self._objects.add(other)
        self.__modified__[other.id] = True
        return OptionalAwaitable(self.save)

    def delete(self, other: MODEL):
        self._raise_if_not_initialized()
        other.ensure_id()
        if not isinstance(other, self.to):
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

    @classmethod  # pylint: disable=duplicate-code
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        raise TypeError('Do not set relation field')


__all__ = ['ManyToMany']
