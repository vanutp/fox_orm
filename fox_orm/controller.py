from databases import Database
from sqlalchemy import MetaData

from fox_orm.connection import Connection


class _FoxOrm:
    connections: dict[str, Connection]

    def __init__(self):
        self.connections = {'default': Connection()}

    @property
    def connection(self) -> Connection:
        return self.connections.get('default')

    @property
    def db(self) -> Database:
        return self.connection.db

    @property
    def metadata(self) -> MetaData:
        return self.connection.metadata

    def init(self, db_url: str, **db_options):
        self.connection.init(db_url, **db_options)

    async def connect(self):
        await self.connection.connect()

    async def disconnect(self):
        await self.connection.disconnect()

    def init_relations(self):
        ...


FoxOrm = _FoxOrm()


__all__ = ['FoxOrm']
