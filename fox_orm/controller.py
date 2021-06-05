from databases import Database
from sqlalchemy import MetaData

from fox_orm.exceptions import AlreadyInitializedException


class _FoxOrmMeta(type):
    _db = None

    def __init__(cls, *args):
        super().__init__(*args)
        cls.metadata = MetaData()

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


class FoxOrm(metaclass=_FoxOrmMeta):
    db: Database
    metadata: MetaData


__all__ = ['FoxOrm']
