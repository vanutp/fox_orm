from sqlalchemy import MetaData

from fox_orm.connection import Connection


class FoxOrmMeta(type):
    connections: dict[str, Connection]

    def __init__(cls, *args, **kwargs):
        cls.connections = {'default': Connection()}
        super().__init__(*args, **kwargs)

    @property
    def connection(cls) -> Connection:
        return cls.connections.get('default')

    @property
    def metadata(cls) -> MetaData:
        return cls.connection.metadata

    def init(cls, db_url: str, **db_options):
        if cls.connection.db:
            raise ValueError('Default connection already initialized')
        cls.connection.init(db_url, **db_options)

    async def connect(cls):
        await cls.connection.connect()

    async def disconnect(cls):
        await cls.connection.disconnect()

    def init_relations(cls):
        ...


class FoxOrm(metaclass=FoxOrmMeta):
    pass


__all__ = ['FoxOrm']
