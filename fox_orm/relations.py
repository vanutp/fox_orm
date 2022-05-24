from abc import ABC
from typing import TYPE_CHECKING, Generic, TypeVar, Iterable

if TYPE_CHECKING:
    from fox_orm.model import OrmModel


MODEL = TypeVar('MODEL', bound='OrmModel')


class Relation(ABC, Generic[MODEL]):
    def __init__(self, *args, **kwargs):
        ...


class OneToOne(Relation, Generic[MODEL]):
    ...


class OneToMany(Relation, Generic[MODEL]):
    ...


class IterableRelation(Relation, ABC, Generic[MODEL]):
    ...


class ManyToOne(IterableRelation, Generic[MODEL]):
    ...


class ManyToMany(IterableRelation, Generic[MODEL]):
    ...


__all__ = [
    'Relation',
    'OneToOne',
    'OneToMany',
    'IterableRelation',
    'ManyToOne',
    'ManyToMany',
]
