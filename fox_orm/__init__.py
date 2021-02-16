from databases import Database

from fox_orm.exceptions import AlreadyInitializedException


class _FoxOrmMeta(type):
    _db = None

    def init(cls, db_uri, **options):
        if cls._db is not None:
            raise AlreadyInitializedException()
        cls._db = Database(db_uri, **options)

    @property
    def db(cls):
        return cls._db


class FoxOrm(metaclass=_FoxOrmMeta):
    db: Database


__all__ = ['FoxOrm']
