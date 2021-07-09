from collections import defaultdict
from typing import TYPE_CHECKING

from databases import Database
from sqlalchemy import MetaData, Table, Column, Integer, ForeignKey

from fox_orm.exceptions import AlreadyInitializedException

if TYPE_CHECKING:
    from fox_orm.relations import _GenericIterableRelation
    from typing import Union, Dict, Type, List, Tuple
    from fox_orm.model import OrmModel


class _FoxOrmMeta(type):
    _db = None
    _assoc_tables: 'Dict[MetaData, Dict[str, Table]]'
    _lazyinit_relations: 'Dict[MetaData, List[Tuple[_GenericIterableRelation, Type[OrmModel]]]]'

    def __init__(cls, *args):
        super().__init__(*args)
        cls.metadata = MetaData()
        cls._assoc_tables = defaultdict(dict)
        cls._lazyinit_relations = defaultdict(list)

    def init(cls, db_uri, **options):
        if cls._db is not None:
            raise AlreadyInitializedException()
        cls._db = Database(db_uri, **options)

    @property
    def db(cls):
        return cls._db

    async def connect(cls):
        await cls.db.connect()  # pylint: disable=no-member

    async def disconnect(cls):
        await cls.db.disconnect()  # pylint: disable=no-member

    def get_assoc_table(
            cls,
            metadata: MetaData,
            a: 'Union[Type[OrmModel], str]',
            b: 'Union[Type[OrmModel], str]',
            name: str
    ):
        a_name = a if isinstance(a, str) else a.__table__.name
        b_name = b if isinstance(b, str) else b.__table__.name
        tables = cls._assoc_tables[metadata]
        if name in tables:
            table = tables[name]
        else:
            table = Table(name, metadata,
                          Column(f'{a_name}_id', Integer, ForeignKey(f'{a_name}.id'), primary_key=True),
                          Column(f'{b_name}_id', Integer, ForeignKey(f'{b_name}.id'), primary_key=True)
                          )
            tables[name] = table
        return table, f'{a_name}_id', f'{b_name}_id'

    def _lazyinit_relation(cls, metadata: MetaData, relation: '_GenericIterableRelation', model_cls: 'Type[OrmModel]'):
        cls._lazyinit_relations[metadata].append((relation, model_cls))

    def init_relations(cls, metadata: MetaData = None):
        if not metadata:
            metadata = cls.metadata
        for rel, model_cls in cls._lazyinit_relations[metadata]:
            # noinspection PyProtectedMember
            # pylint: disable=protected-access
            rel._init(metadata, model_cls)
        cls._lazyinit_relations[metadata].clear()


class FoxOrm(metaclass=_FoxOrmMeta):
    db: Database
    metadata: MetaData


__all__ = ['FoxOrm']
